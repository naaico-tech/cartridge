# Cartridge

A modular data engineering toolkit. Use individual components or the whole stack.

### A. cartridge-init: Schema Inference & dbt Gen
- **Inputs**: database URI / schema name(s), or multi-database configuration
- **Uses**: SQLAlchemy + LLM (OpenAI, OSS like Ollama or LM Studio)
- **Outputs**: AI-generated dbt models + docs
- **Features**: Single/multi-schema scanning, multi-database scanning, organized output
- **Optional**: Add data quality tests using dbt-expectations
- **Tech**: Python, SQLAlchemy, OpenAI/LLM, dbt-core

See CLI-focused docs: `cartridge-init/README.md`.

### B. cartridge-orchestrator: Airflow DAG Generator
- **Inputs**: YAML config with timing, dependencies
- **Outputs**: Modular DAGs using DAG factories
- **Features**:
  - Time-based or event-driven
  - Auto-registration into Airflow
  - Optional Slack/email ops alerting
- **Tech**: Python, Airflow 2.x, Jinja templating

### C. cartridge-analytics: Data Summary Layer
- **Inputs**: dbt models, optionally sample queries
- **Outputs**:
  - Markdown reports, charts, dashboards (e.g. via Streamlit or Jupyter)
  - Auto-insight generation (patterns, anomalies)
- **Tech**: dbt-core, Pandas/Polars, Streamlit, AI (LangChain or OpenAI)

### D. cartridge-deployer: K8s & Infra Bootstrap
- **Helm Charts for**:
  - dbt Core on Kubernetes
  - Airflow
  - MinIO (S3-like)
  - Postgres/Clickhouse/Snowflake connectors
- **Optional**:
  - Cloud support (GKE/EKS/Azure AKS)
  - GitOps integration (e.g., ArgoCD)
- **Tech**: Helm, Terraform (optional), K8s, Docker

## Quick Start

- For schema inference and dbt generation, start with `cartridge-init`:
  - README: `cartridge-init/README.md`
  - CLI: `cartridge --help`

## Repository Layout

- `cartridge-init/`: CLI + API for schema scan and AI-generated dbt projects
- Future modules will live alongside this folder as they are added

## License

MIT