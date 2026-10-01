# Estado real do projeto

Este arquivo existe porque o `README.md` descreve a arquitetura pretendida e este
descreve o que está de fato verificado. Divergência entre os dois é bug de
documentação, não feature pendente.

Atualizado em 2026-09-30.

## TRA-45 — Fase 1: pipeline Redpanda → Flink → Iceberg

| Item | Estado | Evidência |
|---|---|---|
| `docker compose up` sobe Redpanda, Flink e Trino | **verificado** | os três containers `running`; `/health`, `:8081/overview` e `:8080/v1/info` respondem |
| Job Flink com upsert por `event_id` | **verificado** | `lakehouse-ingest` em `RUNNING`, 5 tasks, 0 failed |
| Tabela Iceberg v2 particionada por dia | **verificado** | metadata gravado em `/warehouse/default_database/events/metadata/`; 6 ficheiros parquet |
| Gerador com dado imperfeito (nulos, atraso, ids duplicados) | **verificado** | 313 eventos publicados; `brokerrports 313 messages` |
| Leitura pelo Trino | **NÃO funciona** | ver abaixo |

### Leitura pelo Trino — bloqueado

O Trino 439 recusa `iceberg.catalog.type = hadoop` e `= hive`: o enum
`CatalogType` só aceita `REST`, `GLUE`, `JDBC` e `NESSIE`, e a mensagem de erro
("Invalid value for type CatalogType") não diz quais são válidos. Extraí os
valores do `trino-iceberg-439.jar` para confirmar.

Adotei `JDBC` sobre SQLite por ser o único dos quatro que não exige um segundo
serviço. O catálogo carrega ("Added catalog iceberg using connector iceberg"),
mas consulta falha:

```
Error listing schemas for catalog iceberg:
Failed to execute query: SELECT DISTINCT table_namespace FROM iceberg_tables
WHERE catalog_name = ?
```

O ficheiro SQLite é criado (0 bytes, dono `trino`), mas a tabela `iceberg_tables`
nunca é criada — a biblioteca JDBC do Iceberg inicializa o esquema em
`catalog_type` e parece não o fazer para SQLite nesta versão do Trino.

Candidatos a testar, por ordem de custo:

1. Pré-criar `iceberg_tables` com o esquema exato do Iceberg JDBC catalog, em vez
   de deixar a biblioteca hacerlo.Um `.sql` versionado resolve.
2. Usar o `REST` catalog com `apache/iceberg-rest-catalog` — a imagem não
   não está acessível a partir deste ambiente, tal como o MinIO.
3. Usar `NESSIE` com `ghcr.io/projectnessie/nessie`.

Não escolhi nenhum por conta própria: cada um muda o `docker compose up` que o
projecto promete ser um comando, e essa decisão é do dono do repo.

## TRA-46 — Fase 2: observabilidade

| Item | Estado | Evidência |
|---|---|---|
| Freshness | **implementado, não exercitado contra dados** | `observability.alerts.freshness_alert` |
| Null rate | **implementado, não exercitado contra dados** | `observability.alerts.null_rate_alert` |
| Duplicatas | **implementado, não exercitado contra dados** | `observability.alerts.duplicate_alert` |
| Schema drift | **implementado, não exercitado contra dados** | `observability.alerts.schema_drift_alert` |
| `collect_metrics` — as queries reais contra o Trino | **escrito, nunca executado** | depende do bloqueio acima |
| `scripts/break_pipeline.sh` | **não escrito** | depende de `collect_metrics` funcionar |
| Dashboard (TRA-47) | **não iniciado** | — |

### Por que isto ainda importa

As regras de alerta e a função `evaluate` são funções puras sobre dataclasses, e
têm 21 testes a passar sem Docker, sem Trino e sem Kafka. Isto é deliberado: a
decisão de "isto está saudável ou não" é a parte que tem de ser testável sem
infraestrutura, porque é a parte que costuma não ser testada.

O que **não** está verificado é equally importante: nenhuma consulta foi
corrida contra a tabela real. `collect_metrics` pode estar sintaticamente
correto e semanticamente errado — por exemplo, a partilha de `NULL` que em JS dá
`NaN` e em SQL dá `NULL` já me enganou noutro projeto. Não afirmo que o
scorecard funciona; afirmo que a sua lógica de decisão está testada.

## Desvios do plano original

| Planeado | Feito | Porquê |
|---|---|---|
| MinIO como object store | volume local partilhado | `minio/minio` removido do Docker Hub; todas as tags e espelhos dão `pull access denied` |
| Job PyFlink | Flink SQL via `sql-client.sh` | não existe imagem PyFlink para o Flink 1.20.1 |
| `PARTITIONED BY (days(event_time))` | coluna `DATE` materializada | o parser deste build rejeita função dentro de `PARTITIONED BY` |

## Para continuar

1. Desbloquear o catálogo do Trino (ver acima) — bloqueia TRA-45 e TRA-46.
2. `scripts/demo.sh` a correr o caminho completo e imprimindo contagem por partição.
3. `scripts/break_pipeline.sh` a provar que o scorecard fica vermelho.
4. Screenshot do scorecard no README.
