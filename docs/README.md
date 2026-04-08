# Documentation

Detailed technical reference for each system component.

## Contents

| Doc | Scope |
|-----|-------|
| [Architecture](architecture.md) | System overview, data flow, services, key design decisions |
| [Datasource](datasource.md) | CDC pipeline: PostgreSQL → Debezium SMT → Kafka → Spark → Delta |
| [Notebook](notebook.md) | Streaming notebook cells, HMS registration flow |
| [dbt](dbt.md) | dbt project: materialization, hooks, schema routing, testing |
| [Datamodel](datamodel.md) | Star schema: all column names, table joins, business logic |
| [Airflow](airflow.md) | DAG structure, Cosmos config, manual run commands |
| [Trino](trino.md) | Query engine: catalog config, HMS integration, troubleshooting |

## How These Docs Are Organized

These docs go **deep** on individual components. The root `README.md` provides the overview (quick start, tech stack, service endpoints). These docs provide the details a developer needs to understand and modify each piece.
