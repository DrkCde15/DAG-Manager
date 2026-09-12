# DAG Manager

Dashboard para monitorar e gerenciar DAGs de múltiplas instâncias Airflow.

## Visão Geral

```
┌─────────────────────────────────────────────────┐
│              DAG Manager Dashboard              │
│  ┌─────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ Streamlit│←→│ FastAPI   │←→│ SQLite/Postgres│ │
│  │  :8501   │  │  :8000    │  │               │  │
│  └─────────┘  └────┬─────┘  └───────────────┘  │
│                    │                            │
│         ┌──────────┼──────────┐                 │
│         ▼          ▼          ▼                 │
│    ┌─────────┐ ┌─────────┐ ┌─────────┐         │
│    │Airflow 1│ │Airflow 2│ │Airflow N│         │
│    │ :8080   │ │ :8081   │ │ :808X   │         │
│    └─────────┘ └─────────┘ └─────────┘         │
└─────────────────────────────────────────────────┘
```

## Stack

| Camada | Tecnologia |
|--------|------------|
| Backend | Python 3.12+ / FastAPI / SQLAlchemy (async) |
| Frontend | Streamlit |
| Banco | SQLite (dev) / PostgreSQL (prod) |
| Deploy | Docker Compose / Podman Compose |

## Funcionalidades

- **Multi-instância** — Monitora N instâncias Airflow ao mesmo tempo
- **Sync automático** — Busca DAGs a cada 5 minutos
- **Dashboard consolidado** — Métricas de todas as instâncias em um só lugar
- **Busca e filtros** — Encontre qualquer DAG rapidamente
- **Health check** — Detecta instâncias com falha
- **Auth opcional** — Funciona com ou sem autenticação

## Pré-requisitos

- Python 3.12+
- Docker/Podman (opcional)

## Instalação

```bash
git clone <repo-url>
cd dag-manager
```

### Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

### Frontend

```bash
cd frontend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Uso

### 1. Iniciar o backend

```bash
cd backend
source .venv/bin/activate
uvicorn app.main:app --reload --port 8000
```

API docs: `http://localhost:8000/docs`

### 2. Iniciar o frontend

```bash
cd frontend
source .venv/bin/activate
streamlit run app.py
```

Dashboard: `http://localhost:8501`

### 3. Adicionar instâncias Airflow

Pelo dashboard (aba **Instances**):

- **Add Instance** — Preencha nome, porta, user e password
- **Quick Add** — Adicione várias de uma vez (`nome:porta`)

Ou via API:

```bash
# Sem autenticação
curl -X POST http://localhost:8000/instances/ \
  -H "Content-Type: application/json" \
  -d '{"name": "meu-projeto", "url": "http://localhost:8080"}'

# Com autenticação
curl -X POST http://localhost:8000/instances/ \
  -H "Content-Type: application/json" \
  -d '{"name": "meu-projeto", "url": "http://localhost:8080", "username": "admin", "password": "admin"}'
```

### 4. Sincronizar DAGs

- **Automático** — A cada 5 minutos
- **Manual** — Clique em "Sync" no dashboard

## API Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/` | Info da API |
| `GET` | `/instances/` | Listar instâncias |
| `POST` | `/instances/` | Criar instância |
| `PUT` | `/instances/{id}` | Atualizar instância |
| `DELETE` | `/instances/{id}` | Deletar instância |
| `POST` | `/instances/{id}/sync` | Sync manual |
| `POST` | `/instances/sync-all` | Sync todas |
| `GET` | `/dags/` | Listar DAGs |
| `GET` | `/dags/{id}/runs` | Runs de uma DAG |
| `GET` | `/dashboard/summary` | Métricas consolidadas |
| `GET` | `/dashboard/health` | Health das instâncias |

## Estrutura do Projeto

```
dag-manager/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI + scheduler
│   │   ├── config.py            # Settings
│   │   ├── database.py          # Conexão DB (async)
│   │   ├── models/
│   │   │   └── models.py        # AirflowInstance, DAG, DAGRun
│   │   ├── schemas/
│   │   │   └── schemas.py       # Pydantic models
│   │   ├── routers/
│   │   │   ├── instances.py     # CRUD instâncias
│   │   │   ├── dags.py          # Listar DAGs
│   │   │   └── dashboard.py     # Métricas
│   │   └── services/
│   │       ├── airflow_client.py   # Client API Airflow
│   │       └── sync_service.py     # Sync periódico
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── app.py                   # Dashboard Streamlit
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── .env.example
└── README.md
```

## Docker Compose

```bash
docker compose up --build
```

| Serviço | Porta |
|---------|-------|
| Backend | 8000 |
| Frontend | 8501 |

## Configuração

Variáveis de ambiente (`.env`):

```env
DATABASE_URL=sqlite+aiosqlite:///./dagmanager.db
SYNC_INTERVAL_MINUTES=5
```

Para PostgreSQL:

```env
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/dagmanager
```

## Compatibilidade

Testado com:
- Airflow 2.10.x (session auth)
- Airflow 2.9.x e anteriores (basic auth)

## Licença

MIT
