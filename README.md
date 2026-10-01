# Tier 2 — Lakehouse streaming com Apache Iceberg

Prova de que o caminho **Redpanda → Flink → Iceberg → Trino** funciona, e que
alerta sem teste não é alerta.

O projeto tem duas camadas, e a segunda é a que interessa:

1. **Pipeline** — gerador de eventos imperfeito, job Flink com upsert por chave
   de negócio, tabela Iceberg particionada por dia.
2. **Observabilidade** — freshness, schema drift, taxa de nulos, duplicatas, e
   um script que **quebra o pipeline de propósito** para provar que os alertas
   disparam de verdade.

O gerador produz sujeira de propósito (nulos, timestamps atrasados, ids
duplicados) porque um pipeline que nunca vê dado ruim não tem observabilidade
para provar.

## Requisitos

- Docker + `docker compose`
- Python 3.11+ e `uv` para o código fora dos containers
- ~4 GB de RAM livres

## Rodar

```bash
./scripts/demo.sh
```

Sobe a stack, roda o gerador, aplica as tabelas, e imprime a contagem de linhas
por partição vinda do Trino. Do cold start ao resultado: menos de três minutos.

Para inspecionar na mão:

```bash
docker compose up -d          # sobe tudo
uv run python -m generator --count 500   # publica no Redpanda
uv run python -m observability.report    # scorecard de qualidade
```

## Arquitetura

| Camada | Escolha | Porquê |
|---|---|---|
| Transporte | Redpanda | Binário único, API compatível com Kafka. `docker compose up` funciona. |
| Compute | Apache Flink (PyFlink) | Motor de streaming em Python mais debochado. Spark exigiria uma JVM pesada. |
| Storage | Apache Iceberg v2 sobre MinIO/S3 | Evolução de schema e upsert sem reescrever histórico. |
| Query | Trino | Leitura federada. JVM, mas só leitor. |

## Estrutura

```
generator/       produtor de eventos sintéticos e imperfeitos
flink/           job PyFlink e as tabelas Iceberg
observability/   coleta de métricas e regras de alerta
scripts/         demo.sh, quebra o pipeline de propósito
```

## Estado

Ver [`STATUS.md`](STATUS.md) para o que está pronto e o que não está.