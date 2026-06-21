import json
import os

from Process import BaseProcess
from consensus.viewstamped_replication import VRNode

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "6062"))
PRIMARY_ID = int(os.getenv("PRIMARY_ID", "6061"))


DEFAULT_TOPOLOGY = '{"6061":["process1",7061],"6062":["process2",7062],"6063":["process3",7063]}'
CLUSTER_TOPOLOGY = os.getenv("CLUSTER_TOPOLOGY", DEFAULT_TOPOLOGY)


class Process2(BaseProcess, VRNode):
    """Réplica VR. Ver Process1 para a explicação da herança múltipla."""

    def __init__(self, host: str, port: int, topology: list, primary_id: int):
        BaseProcess.__init__(self, host=host, port=port)

        replica_id = port
        VRNode.__init__(self, current_address=(host, port), replica_id=replica_id, all_replicas=topology, primary_id=primary_id)
        self._start_timeout_watcher()

if __name__ == '__main__':
    topology = json.loads(CLUSTER_TOPOLOGY)
    p2 = Process2(host=HOST, port=PORT, topology=topology, primary_id=PRIMARY_ID)
    p2.start()