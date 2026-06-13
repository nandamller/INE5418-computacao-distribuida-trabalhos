"""
Camada de comunicação (Berkeley Sockets).

Toda a troca de mensagens entre processos — tanto as RPCs internas do Raft
(RequestVote / AppendEntries) quanto as requisições dos clientes — usa o mesmo
formato simples: uma mensagem é um objeto JSON terminado por '\n' (uma linha).

O enquadramento por linha ("newline-delimited JSON") é suficiente porque cada
conexão TCP transporta exatamente uma requisição e uma resposta.
"""

import json
import socket


def send_line(sock, obj):
    """Serializa `obj` como JSON e envia uma linha pelo socket."""
    data = (json.dumps(obj) + "\n").encode("utf-8")
    sock.sendall(data)


def recv_line(sockfile):
    """Lê uma linha (uma mensagem JSON) de um socket aberto em modo arquivo.

    Retorna o dicionário decodificado, ou None se a conexão foi fechada.
    """
    line = sockfile.readline()
    if not line:
        return None
    return json.loads(line.decode("utf-8"))


def rpc(host, port, obj, timeout=0.4):
    """Executa uma RPC síncrona: abre conexão, envia uma mensagem, lê a resposta.

    Retorna o dicionário de resposta, ou None se o destino estiver inacessível
    (processo caído, timeout, etc.). Esse None é justamente o que permite ao Raft
    tolerar falhas parciais — uma RPC sem resposta simplesmente não conta.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.settimeout(timeout)
            send_line(s, obj)
            f = s.makefile("rb")
            return recv_line(f)
    except (OSError, ValueError):
        return None
