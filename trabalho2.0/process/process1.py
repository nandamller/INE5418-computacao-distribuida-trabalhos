import os

from pydantic import BaseModel

import socket
from Process import BaseProcess
from consensus.viewstamped_replication import VRNode


HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT","6061"))

DEFAULT_TOPOLOGY = '[["127.0.0.1", 6061], ["127.0.0.1", 6062], ["127.0.0.1", 6063]]'
CLUSTER_TOPOLOGY = os.getenv("CLUSTER_TOPOLOGY", DEFAULT_TOPOLOGY)

class NetworkMessage(BaseModel):
    sender_port: int
    msg_type: str
    payload: dict

class Process1(BaseProcess):
    def __init__(self, host: str, port: int, topology: list):
        BaseProcess.__init__(self, host=host, port=port)

        # TO FIX: o que é esperado para valor int de "replica_id" - corrigir linha abaixo
        replica_id = 0

        VRNode.__init__(self, current_address=(host, port), replica_id=replica_id, all_replicas=topology)
        
        self.is_running = True

    def execute(self):
        print(f"[Process 1] Started. Listening on {self.host}:{self.port}...")
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Allows re-binding to the port immediately after closing
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen()
            
            connection, address = s.accept()
            with connection:
                print(f"[Process 1] Connection established with {address}")
                
                while True:
                    data = connection.recv(1024)
                    if not data:
                        break
                    
                    received_msg = data.decode('utf-8')
                    print(f"[Process 1] Received: '{received_msg}'")
                    
                    # Echo back response
                    response = f"Process 1 confirmed: {received_msg}"
                    connection.sendall(response.encode('utf-8'))
                    
        print("[Process 1] Finished.")

if __name__ == '__main__':
    p1 = Process1(host=HOST, port=PORT, topology=CLUSTER_TOPOLOGY)
    p1.execute()