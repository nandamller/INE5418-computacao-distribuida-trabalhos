#!/usr/bin/env bash
# Sobe todos os nós definidos em cluster.json em background, cada um gravando
# seu log em logs/<id>.log. Os PIDs ficam em .pids (usados por stop_cluster.sh).
set -e
cd "$(dirname "$0")"
mkdir -p logs
: > .pids

NODES=$(python3 -c "import json;print(' '.join(n['id'] for n in json.load(open('cluster.json'))['nodes']))")

for n in $NODES; do
  python3 raft_node.py "$n" > "logs/$n.log" 2>&1 &
  echo $! >> .pids
  echo "iniciado $n (pid $!) -> logs/$n.log"
done

echo
echo "Cluster no ar. Acompanhe os logs com:  tail -f logs/*.log"
echo "Para parar tudo:                       ./stop_cluster.sh"
