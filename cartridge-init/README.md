# Cartridge Init — CLI-first

AI-powered schema inference and dbt project generation.

- **Inputs**: database URI and schema name
- **Outputs**: dbt project (models, docs, optional tests)
- **Engines**: OpenAI, Anthropic, Gemini, or mock; SQLAlchemy for scanning
- **Optional**: add `dbt-expectations` tests

## Install

```bash
cd cartridge-init
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -e .
cartridge --help
```

## Environment

Set one provider key (or use `--ai-model mock`).

```bash
export OPENAI_API_KEY=...
# or
export ANTHROPIC_API_KEY=...
# or
export GEMINI_API_KEY=...
```

## One-minute E2E

```bash
# Optional: start local infra and sample data
docker compose up -d postgres redis
./scripts/load-ecommerce-data.sh

# 1) Scan
cartridge scan postgresql://cartridge:cartridge@localhost:5432/cartridge \
  --schema ecommerce \
  --output scan.json

# 2) Generate
cartridge generate scan.json \
  --ai-model gpt-4 \
  --output ./my_dbt_project \
  --project-name ecommerce_analytics

# 3) Run dbt
cd my_dbt_project
dbt deps && dbt run
```

## CLI

```bash
cartridge --help
cartridge scan <CONNECTION_STRING> --schema <SCHEMA> [--output scan.json] [--format json|yaml]
cartridge generate <SCAN_FILE> --ai-model <MODEL> [--ai-provider openai|anthropic|gemini|mock] \
  --output <DIR> --project-name <NAME> [--business-context "..."]
cartridge serve [--host 0.0.0.0 --port 8000 --reload]
cartridge init-database
cartridge reset-database
cartridge config
```

### Examples

- Postgres scan
```bash
cartridge scan postgresql://user:pass@host:5432/db --schema public --output scan.json
```

- OpenAI generation
```bash
export OPENAI_API_KEY=sk-...
cartridge generate scan.json --ai-model gpt-4 --output ./dbt_proj --project-name analytics
```

- Gemini generation
```bash
export GEMINI_API_KEY=...
cartridge generate scan.json --ai-model gemini-2.5-flash --output ./dbt_proj
```

- Mock provider (no key)
```bash
cartridge generate scan.json --ai-model mock --output ./dbt_proj
```

## Inputs/Outputs

- **Scan inputs**: `CONNECTION_STRING`, `--schema`
- **Scan outputs**: `scan.json`/`scan.yaml` with tables, columns, constraints, indexes, sample data
- **Generate inputs**: scan file + `--ai-model` (+ optional `--ai-provider` and `--business-context`)
- **Generate outputs**: dbt project folder with models, `schema.yml`, `packages.yml`, docs

Tip: add `dbt-expectations` in `packages.yml` to enable richer tests.

## Minimal API (optional)

Prefer the CLI. If you need an API for automation:

```bash
cartridge serve --reload
# Docs: http://localhost:8000/docs
```

## Troubleshooting

- Connection issues: ensure database is reachable; `pg_isready -h host -p 5432`
- Missing API key: set `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` / `GEMINI_API_KEY` or use `--ai-model mock`
- Verbose logs: `cartridge -v scan ...`

## License

MIT