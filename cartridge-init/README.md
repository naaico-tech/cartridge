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

## Quick Start

### Option 1: Interactive Onboarding (Recommended)

```bash
# 1) Onboard to collect business context
cartridge onboard --output business_context.csv

# 2) Scan your database
cartridge scan postgresql://cartridge:cartridge@localhost:5432/cartridge \
  --schema ecommerce \
  --output scan.json

# 3) Generate with business context
cartridge generate scan.json \
  --ai-model gpt-4 \
  --business-context-file business_context.csv \
  --output ./my_dbt_project \
  --project-name ecommerce_analytics

# 4) Run dbt
cd my_dbt_project
dbt deps && dbt run
```

### Option 2: One-minute E2E

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

## CLI Commands

```bash
cartridge --help
cartridge onboard [--output business_context.csv]
cartridge scan <CONNECTION_STRING> --schema <SCHEMA> [--output scan.json] [--format json|yaml]
cartridge generate <SCAN_FILE> --ai-model <MODEL> [--ai-provider openai|anthropic|gemini|mock] \
  --output <DIR> --project-name <NAME> [--business-context "..." | --business-context-file <CSV_FILE>]
cartridge serve [--host 0.0.0.0 --port 8000 --reload]
cartridge init-database
cartridge reset-database
cartridge config
```

### Business Context Options

- **`--business-context`**: Provide business context as a string
- **`--business-context-file`**: Provide business context from a CSV file (alternative to `--business-context`)
- **Cannot use both options together** - choose one or the other

The `onboard` command creates a CSV file that can be used with `--business-context-file`.

### Examples

- **Interactive onboarding**
```bash
cartridge onboard --output my_business.csv
```

- **Postgres scan**
```bash
cartridge scan postgresql://user:pass@host:5432/db --schema public --output scan.json
```

- **OpenAI generation with business context file**
```bash
export OPENAI_API_KEY=sk-...
cartridge generate scan.json --ai-model gpt-4 --business-context-file my_business.csv --output ./dbt_proj
```

- **Gemini generation with inline business context**
```bash
export GEMINI_API_KEY=...
cartridge generate scan.json --ai-model gemini-2.5-flash --business-context "E-commerce platform focusing on revenue and conversion metrics" --output ./dbt_proj
```

- **Mock provider (no key)**
```bash
cartridge generate scan.json --ai-model mock --output ./dbt_proj
```

### Business Context CSV Format

Use the provided `sample_business_context.csv` as a template, or create your own CSV with these columns:
- `business_name`, `industry`, `business_description`
- `primary_metrics`, `secondary_metrics`, `business_model`
- `target_audience`, `refresh_frequency_minutes`
- `reporting_needs`, `data_sources`, `use_cases`
- `stakeholders`, `current_challenges`, `success_criteria`

## Inputs/Outputs

- **Onboard inputs**: Interactive prompts
- **Onboard outputs**: CSV file with business context
- **Scan inputs**: `CONNECTION_STRING`, `--schema`
- **Scan outputs**: `scan.json`/`scan.yaml` with tables, columns, constraints, indexes, sample data
- **Generate inputs**: scan file + `--ai-model` (+ optional `--ai-provider` and `--business-context` or `--business-context-file`)
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