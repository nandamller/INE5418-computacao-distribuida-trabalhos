import asyncio
import os
import sys
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
import uvicorn
import httpx

from utils.Enum import Status
from utils.args import PrepareArgs, PrepareOKArgs
from utils.exceptions import InvalidStatusError, ViewMismatchError, LogInconsistencyError


# --- Viewstamped Replication Node State ---
class VRNode:
    def __init__(self, replica_id: int, all_replicas: List[str]):
        self.host = host
        self.port = port
        self.replica_id = replica_id
        self.all_replicas = all_replicas  # List of URLs (e.g., ["http://127.0.0.1:8000", ...])
        self.N = len(all_replicas)
        self.f = (self.N - 1) // 2
        self.quorum_size = self.f + 1

        # VR State variables
        self.op_num = 0
        self.commit_num = 0
        self.view_num = 0
        self.primary_id = None
        self.status: Status = Status.NORMAL

        self.op_log: List[Dict[str, Any]] = [] # The replicated log: list of {"op": str, "view": int, "client_id": int, "request_num": int}
        self.client_table: Dict[int, Dict[str, Any]] = {} # Tracks client_id -> {"request_num": int, "result": Any, "status": str}

        self.prepare_ok_counts: Dict[int, set] = {} # op_num -> set of replica_nums that sent PREPARE_OK
        self.view_change_votes: Dict[int, List[Dict]] = {} # view_num -> list of received DO_VIEW_CHANGE messages

    # @property
    # def primary_id(self) -> int:
    #     return self.primary

    @property
    def is_primary(self) -> bool:
        return self.replica_id == self.primary_id

    # @primary_id.setter
    # def primary_id(self, new_primary_id: int):
    #     self.primary_id = new_primary_id

    def received_request(self, args: RequestArgs):
        """Executed ONLY by the Primary. Receives client request and starts replication."""
        if not self.is_primary():
            # TODO: implementar um retorno adequado pro cliente informando o endereço do primário
            raise InvalidStatusError(current_status="BACKUP", expected_status="PRIMARY")
        if self.status != NodeStatus.NORMAL:
            raise InvalidStatusError(current_status=self.status.value)

        # Drop or handle duplicate requests from the same client
        client_id = args.client_id
        if client_id in self.client_table:
            if args.request_num <= self.client_table[client_id]["request_num"]:
                print(f"[Node {self.replica_num}] Duplicate client request dropped.")
                return

        # Advance operation number and append to local log
        self.op_num += 1
        log_entry = {
            "op": args.op,
            "view": self.view_num,
            "client_id": args.client_id, 
            "request_num": args.request_num
        }
        self.op_log.append(log_entry)
        
        # Update client table status
        self.client_table[client_id] = {
            "request_num": args.request_num,
            "status": "PREPARING",
            "result": None
        }

        print(f"[Primary Node {self.replica_num}] Processing op_num {self.op_num}: '{args.op}'")
        
        # Trigger communication: In your network loop, you would now send a 
        # PREPARE message containing `PrepareArgs` to all backups.
        prepare_msg = PrepareArgs(
            view=self.view_num,
            op_num=self.op_num,
            op=args.op,
            commit_num=self.commit_num,
            client_id=args.client_id,
            request_num=args.request_num
        )
        return prepare_msg

    def prepare(self, args: PrepareArgs):
        """Executed by Backups. Processes a replication command from the primary."""
        if self.status != Status.NORMAL:
            raise InvalidStatusError(current_status=self.status)
        if args.view != self.view_num:
            raise ViewMismatchError(msg_view=args.view, node_view=self.view_num)

        if args.op_num == self.op_num + 1:
            # Increment op_num and append to backup log
            self.op_num += 1
            self.op_log.append({
                "op": args.op, 
                "view": args.view, 
                "client_id": args.client_id, 
                "request_num": args.request_num
            })

            # Update client table
            self.client_table[args.client_id] = {
                "request_num": args.request_num,
                "status": "PREPARING",
                "result": None
            }

            # Backups can safely execute up to the primary's last known committed transaction
            self.commit(args.commit_num)

            print(f"[Backup Node {self.replica_num}] Prepared op_num {self.op_num}. Sending PREPARE_OK.")
            
            # Trigger communication: Network layer sends this back to the primary
            return PrepareOkArgs(view=self.view_num, op_num=self.op_num, replica_num=self.replica_num)
        else:
            raise LogInconsistencyError(expected_op=self.op_num + 1, received_op=args.op_num)

    prepare_ok(self, args: PrepareOKArgs):
        """Executed ONLY by the Primary. Tracks quorum for an operation."""
        if not self.is_primary():
            return
        if args.view != self.view_num:
            return # Ignore stale view responses
        
        op_num = args.op_num
        if op_num <= self.commit_num:
            return # Already committed

        if op_num not in self.prepare_ok_counts:
            self.prepare_ok_counts[op_num] = set()

        # Add the replica that acknowledged the operation
        self.prepare_ok_counts[op_num].add(args.replica_num)

        # Quorum Check: Including the primary itself, we need f agreements (so f backups)
        if len(self.prepare_ok_counts[op_num]) >= self.quorum_size:
            print(f"[Primary Node {self.replica_num}] Quorum achieved for op_num {op_num}!")
            
            # Commit entries sequentially up to this one
            self.commit(op_num)
            
            # Primary executes the state machine and replies to client
            self.reply(op_num)

    def commit(self, primary_commit_num: int):
        """Advances the local commit pointer and applies logs to the state machine."""
        # Ensure we commit sequentially and never beyond our current op_num
        target_commit = min(primary_commit_num, self.op_num)
        
        while self.commit_num < target_commit:
            self.commit_num += 1
            entry = self.op_log[self.commit_num - 1] # 0-indexed log array
            
            # State Machine Application Execution Simulation
            result = f"Executed: {entry['op']}" 
            
            # Finalize client table status
            self.client_table[entry["client_id"]] = {
                "request_num": entry["request_num"],
                "status": "COMMITTED",
                "result": result
            }
            print(f"[Node {self.replica_num}] Committed entry op_num {self.commit_num}.")

    def reply(self, op_num: int):
        """Executed ONLY by the Primary. Sends response back to the client."""
        if not self.is_primary():
            return

        entry = self.op_log[op_num - 1]
        client_info = self.client_table.get(entry["client_id"])
        
        if client_info and client_info["status"] == "COMMITTED":
            print(f"[Primary Node {self.replica_num}] REPLYing to Client {entry['client_id']}: {client_info['result']}")
            # TODO: Inside your actual network logic, you would transmit the result to the client here.

    def start_view_change(self):
        """Executed when a backup suspects the primary has crashed (Timeout)."""
        self.status = NodeStatus.VIEW_CHANGE
        self.view_num += 1
        print(f"[Node {self.replica_num}] Primary timed out! Starting View Change to View {self.view_num}...")
        
        # Broadcast START_VIEW_CHANGE(v, replica_num) to all replicas
        # In actual network, broadcast: {"type": "START_VIEW_CHANGE", "view": self.view_num, "replica": self.replica_num}
        return {"type": "START_VIEW_CHANGE", "view": self.view_num, "replica": self.replica_num}

    def do_view_change(self, incoming_view: int, sender_replica: int, sender_state: Dict):
        """Executed by the deterministic NEXT Primary once it receives enough START_VIEW_CHANGEs."""
        # Only the next primary acts as the coordinator for the view change aggregation
        next_primary_id = incoming_view % self.num_replicas
        if self.replica_num != next_primary_id:
            return # I am not the designated coordinator for this view change
            
        if incoming_view not in self.view_change_votes:
            self.view_change_votes[incoming_view] = []
            
        # Store state payload sent by changing nodes
        self.view_change_votes[incoming_view].append({
            "sender": sender_replica,
            "state": sender_state
        })
        
        # Wait for a quorum of f DO_VIEW_CHANGE payloads
        if len(self.view_change_votes[incoming_view]) >= self.f:
            print(f"[Node {self.replica_num}] Received quorum of DO_VIEW_CHANGE. Taking over as Primary!")
            self.view_num = incoming_view
            
            # Select the most up-to-date log payload from quorum
            self.new_state(self.view_change_votes[incoming_view])
            
            self.status = NodeStatus.NORMAL
            self.start_view()

    def start_view(self):
        """Executed by the NEW Primary to notify all backups to move to the new view."""
        print(f"[New Primary Node {self.replica_num}] Broadcasting START_VIEW for view {self.view_num} to all backups.")
        # Broadcast START_VIEW message with the renewed log to all nodes.

    def get_state(self) -> Dict[str, Any]:
        """Packaging internal states to send during view changes or recoveries."""
        return {
            "view_num": self.view_num,
            "op_num": self.op_num,
            "commit_num": self.commit_num,
            "op_log": self.op_log,
            "client_table": self.client_table
        }

    def new_state(self, quorum_states: List[Dict]):
        """Executed by the new primary to consolidate logs based on the highest op_num received."""
        highest_op_entry = max(quorum_states, key=lambda x: x["state"]["op_num"])
        farthest_state = highest_op_entry["state"]
        
        # Overwrite internal log with the safest log path discovered
        self.op_log = farthest_state["op_log"]
        self.op_num = farthest_state["op_num"]
        self.commit_num = farthest_state["commit_num"]
        self.client_table = farthest_state["client_table"]
        print(f"[Node {self.replica_num}] Local state updated/synchronized to op_num {self.op_num}.")


