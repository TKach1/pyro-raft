# pyro-raft

Implementação do algoritmo RAFT usando Pyro5 (Python Remote Objects) para comunicação entre nós distribuídos.

## Arquitetura

- **Candidatos (candidates):** Nós que participam de eleições RAFT. Um vira líder, os demais ficam como followers.
- **Clientes (clients):** Enviam dados (string) ao líder. Líder persiste em JSON e replica para followers.
- **Nameserver:** Pyro5 nameserver para discovery de nós.

## Setup Local

```bash
# Python 3.14 via pyenv
pyenv install 3.14.0a6
pyenv local 3.14.0a6

# venv
python -m venv .venv
source .venv/bin/activate

# deps
pip install -r requirements.txt
```

## Docker

```bash
# Subir cluster (3 candidates, 2 clients por default)
docker compose up --build

# Escalar
docker compose up --build --scale candidate=5 --scale client=10
```

## Estrutura

```
src/
├── __init__.py
├── config.py        # Configurações e constantes
├── candidate.py     # Nó candidato/follower/leader (RAFT)
├── client.py        # Nó cliente
└── nameserver.py    # Launcher do Pyro nameserver
```

---

## TODO

### RAFT Core
- [ ] Implementar estado do nó (follower, candidate, leader)
- [ ] Implementar election timeout aleatório
- [ ] Implementar RequestVote RPC
- [ ] Implementar eleição (transição follower → candidate → leader)
- [ ] Implementar heartbeat do líder (AppendEntries vazio)
- [ ] Implementar detecção de falha do líder (timeout sem heartbeat)
- [ ] Implementar termos (term) e lógica de voto

### Replicação
- [ ] Implementar log de entradas no líder
- [ ] Implementar AppendEntries RPC com dados
- [ ] Implementar confirmação de replicação (majority commit)
- [ ] Implementar persistência em JSON (`{client_id, data, timestamp}`)
- [ ] Implementar sincronização de log para novos followers

### Cliente
- [ ] Implementar discovery do líder via nameserver
- [ ] Implementar envio de dados ao líder
- [ ] Implementar redirect quando acertar follower ao invés do líder
- [ ] Implementar retry em caso de falha/eleição em progresso

### Infra
- [ ] Testar eleição com 3, 5, 7 nós
- [ ] Testar tolerância a falha (matar líder, verificar re-eleição)
- [ ] Testar replicação consistente entre todos os nós
- [ ] Testar com múltiplos clientes simultâneos
- [ ] Adicionar logging estruturado
- [ ] Adicionar health check nos containers
