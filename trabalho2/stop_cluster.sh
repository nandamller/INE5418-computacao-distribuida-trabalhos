#!/usr/bin/env bash
# Para todos os nós iniciados por run_cluster.sh.
cd "$(dirname "$0")"
if [ -f .pids ]; then
  while read -r pid; do
    if kill "$pid" 2>/dev/null; then
      echo "parado pid $pid"
    fi
  done < .pids
  rm -f .pids
else
  echo "nenhum .pids encontrado (cluster não está rodando?)"
fi
