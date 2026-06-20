import json
import os

from Process import BaseProcess
from consensus.viewstamped_replication import VRNode

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "6063"))
PRIMARY_ID = int(os.getenv("PRIMARY_ID", "6061"))

DEFAULT_TOPOLOGY = '[["127.0.0.1", 7061], ["127.0.0.1", 7062], ["127.0.0.1", 7063]]'
CLUSTER_TOPOLOGY = os.getenv("CLUSTER_TOPOLOGY", DEFAULT_TOPOLOGY)


class Process3(BaseProcess, VRNode):
    """Réplica VR. Ver Process1 para a explicação da herança múltipla."""

    def __init__(self, host: str, port: int, topology: list):
        BaseProcess.__init__(self, host=host, port=port)

        replica_id = port
        VRNode.__init__(self, current_address=(host, port), replica_id=replica_id, all_replicas=topology, primary_id=PRIMARY_ID)


if __name__ == '__main__':
    topology = json.loads(CLUSTER_TOPOLOGY)
    p3 = Process3(host=HOST, port=PORT, topology=topology)
    p3.start()