# DAG Manager

Dashboard para monitorar e gerenciar DAGs de múltiplas instâncias Airflow (locais ou na internet).

## Visão Geral

```
┌─────────────────────────────────────────────────┐
│              DAG Manager Dashboard              │
│  ┌─────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ Streamlit│←→│ FastAPI   │←→│ SQLite        │ │
│  │  :8501   │  │  :8000    │  │               │  │
│  └─────────┘  └────┬─────┘  └───────────────┘  │
│                    │                            │
│         ┌──────────┼──────────┐                 │
│         ▼          ▼          ▼                 │
│    ┌─────────┐ ┌─────────┐ ┌─────────┐         │
│    │Local    │ │Internet │ │Internet │         │
│    │:8080    │ │https:// │ │https:// │         │
│    │         │ │empresa  │ │staging  │         │
│    └─────────┘ └─────────┘ └─────────┘         │
└─────────────────────────────────────────────────┘
```

## Stack

| Camada | Tecnologia |
|--------|------------|
| Backend | Python 3.12+ / FastAPI / SQLAlchemy (async) |
| Frontend | Streamlit |
| Banco | SQLite (default) / PostgreSQL (prod) |
| Deploy | Docker Compose / Podman Compose |

## Funcionalidades

- **Multi-instância** — Monitora N instâncias Airflow ao mesmo tempo
- **Local + Internet** — Funciona com URLs locais e remotas (HTTPS)
- **Sync automático** — Busca DAGs e runs a cada 5 minutos
- **Session auth** — Suporta autenticação baseada em sessão (Airflow 2.10+)
- **Trigger DAG** — Execute DAGs pelo dashboard, com `conf` JSON opcional (parâmetros da run)
- **Ver Logs** — Visualize logs das execuções
- **Dashboard consolidado** — Métricas de todas as instâncias em um só lugar
- **Busca e filtros** — Encontre qualquer DAG rapidamente
- **Health check** — Detecta instâncias com falha
- **Bulk add** — Adicione várias instâncias de uma vez

## Pré-requisitos

- Python 3.12+
- Docker/Podman (opcional)

## Instalação

```bash
git clone <repo-url>
cd dag-manager

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt  # apenas para testes
```

Um único `requirements.txt` na raiz cobre backend e frontend.

## Uso

### 1. Iniciar o backend

```bash
source .venv/bin/activate
uvicorn app.main:app --reload --port 8000 --app-dir backend
```

API docs: `http://localhost:8000/docs`

### 2. Iniciar o frontend

```bash
source .venv/bin/activate
streamlit run frontend/app.py
```

Dashboard: `http://localhost:8501`

### Testes

```bash
source .venv/bin/activate
pytest
```

### 3. Adicionar instâncias Airflow

Pelo dashboard (aba **Instances**):

**Add Instance:**

| Campo | Exemplo |
|-------|---------|
| Name | data-warehouse |
| URL | `http://localhost:8080` ou `https://airflow.empresa.com` |
| Username | admin |
| Password | admin |

**Quick Add (bulk):**

```
data-warehouse|http://localhost:8080
airflow-producao|https://airflow.empresa.com
airflow-staging|https://staging.airflow.empresa.com
```

Ou via API:

```bash
# Local (sem auth)
curl -X POST http://localhost:8000/instances/ \
  -H "Content-Type: application/json" \
  -d '{"name": "meu-projeto", "url": "http://localhost:8080"}'

# Internet (com auth)
curl -X POST http://localhost:8000/instances/ \
  -H "Content-Type: application/json" \
  -d '{"name": "airflow-prod", "url": "https://airflow.empresa.com", "username": "admin", "password": "admin"}'
```

### 4. Sincronizar DAGs

- **Automático** — A cada 5 minutos (DAGs + últimas runs de cada DAG)
- **Manual** — Clique em "Sync" no dashboard

### 5. Trigger com conf (query params da DAG)

No dashboard (aba **DAGs**), em *Trigger DAG*, cole um JSON opcional:

```json
{
  "date": "2026-09-23",
  "full_refresh": true
}
```

Na DAG isso chega em `context["conf"]`. Via API:

```bash
curl -X POST http://localhost:8000/dags/1/trigger \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"conf": {"date": "2026-09-23"}}'
```

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
| `POST` | `/dags/{id}/trigger` | Trigger DAG (`{"conf": {...}}` opcional) |
| `GET` | `/dags/{id}/runs/{run_id}/logs` | Logs da execução |
| `GET` | `/dags/{id}/runs/{run_id}/tasks` | Tasks da execução |
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
│   ├── tests/                  # pytest (sync, runs, API)
│   └── Dockerfile
├── frontend/
│   ├── app.py                   # Dashboard Streamlit
│   └── Dockerfile
├── docker-compose.yml
├── requirements.txt             # único (backend + frontend)
├── requirements-dev.txt         # pytest
├── pytest.ini
├── .dockerignore
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
RUNS_SYNC_LIMIT=20

# Exige Authorization: Bearer <token> (ou X-API-Key) em todas as rotas exceto / e /docs
API_TOKEN=

# Criptografa a senha das instâncias Airflow no banco (Fernet via SHA-256)
SECRET_KEY=
```

Para PostgreSQL:

```env
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/dagmanager
```

### Segurança

| Variável | Efeito |
|----------|--------|
| `API_TOKEN` | Se definida, a API responde **401** sem `Authorization: Bearer <token>` ou `X-API-Key`. Frontend envia o header automaticamente. |
| `SECRET_KEY` | Se definida, senhas de instância são gravadas criptografadas no SQLite/Postgres. Sem ela, warning no startup e texto puro. |

**Antes de expor a porta 8000 fora de localhost, defina `API_TOKEN` e `SECRET_KEY`** (veja `.env.example`).

```bash
# gerar valores
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Exemplo com curl autenticado:

```bash
curl -H "Authorization: Bearer $API_TOKEN" http://localhost:8000/dashboard/summary
```

## URLs Suportadas

| Tipo | Exemplo |
|------|---------|
| Local | `http://localhost:8080` |
| IP local | `http://192.168.1.10:8080` |
| Internet HTTPS | `https://airflow.empresa.com` |
| Internet com porta | `https://airflow.empresa.com:8443` |

## Compatibilidade

Testado com:
- Airflow 2.10.x (session auth via CSRF)
- Airflow 2.9.x e anteriores (basic auth)

## Licença

MIT
