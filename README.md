# sql-editor-backend

API Spring Boot para execução dinâmica de SQL e futura integração com AI Assistant.

## Arquitetura adotada

Estrutura em camadas, com separação clara de responsabilidades:

- `presentation`: controllers HTTP (sem DTO dedicado no momento)
- `application`: services e modelos da aplicação
- `domain`: contratos (ports) e modelos centrais
- `infrastructure`: detalhes técnicos (JDBC, config, etc.)
- `shared`: resposta padronizada e tratamento global de exceções

Isso permite baixo acoplamento e facilita trocar implementação de infraestrutura sem afetar regras de negócio.

## Estrutura de pacotes

```text
com.victor.sql_api
├── assistant
│   ├── application
│   │   ├── model
│   │   └── service
│   └── presentation
│       └── controller
├── config
├── shared
│   ├── api
│   ├── exception
│   └── handler
└── sql
    ├── application
    │   ├── model
    │   └── service
    ├── domain
    │   ├── model
    │   └── port
    ├── infrastructure
    │   ├── config
    │   └── gateway
    └── presentation
        └── controller
```

## Metadata Retrieval (novo)

Foi adicionado o módulo `assistant.metadata` para recuperar contexto relevante de metadados sem chamar LLM.

Fluxo interno:

- `MetadataRetrievalService` concentra toda a lógica do retrieval:
  - análise da pergunta
  - leitura do catálogo `eqt_metadata`
  - matching/ranking de tabelas e colunas
  - resolução de relacionamentos
  - montagem de `MetadataContext`

Ou seja: uma classe principal para chamar e entender o fluxo.

Uso (camada de aplicação):

```java
MetadataContext context = metadataRetrievalService.retrieve(
    new RetrievalRequest("qual o faturamento por região no mês passado?", null)
);
```

## Endpoints

### 1) Executar SQL

- `POST /sql/run`
- Request:

```json
{
  "sql": "SELECT * FROM customers LIMIT 10",
  "page": 0,
  "size": 50
}
```

- Response (sucesso):

```json
{
  "success": true,
  "data": {
    "executionId": "uuid",
    "executedAt": "2026-04-17T18:00:00Z",
    "durationMs": 18,
    "statementType": "QUERY",
    "columns": ["id", "name"],
    "rows": [{ "id": 1, "name": "Ana" }],
    "rowCount": 1,
    "affectedRows": null,
    "truncated": false,
    "message": "Consulta executada com sucesso.",
    "pagination": {
      "page": 0,
      "size": 50,
      "hasNext": false,
      "totalRows": null
    }
  },
  "error": null,
  "meta": {
    "timestamp": "2026-04-17T18:00:00Z",
    "traceId": "uuid"
  }
}
```

### 2) AI Assistant (estrutura pronta)

- `POST /assistant/suggest`
- Request:

```json
{
  "prompt": "Quero os 10 clientes com maior faturamento",
  "currentSql": "SELECT ..."
}
```

- Atualmente retorna erro controlado `501 NOT_IMPLEMENTED` com payload padronizado.

## Padronização de resposta e erros

- Todas as respostas usam `ApiResponse<T>`
- Erros de negócio/controlados usam exceções de aplicação (`ApiException`)
- `GlobalExceptionHandler` centraliza o tratamento e mantém formato consistente

## Configurações relevantes

No `application.properties`:

- `app.sql-execution.max-rows`: limite de linhas retornadas por query
- `app.sql-execution.query-timeout-seconds`: timeout de execução SQL
- `app.sql-execution.default-page-size`: tamanho padrão para paginação lógica

## Evoluções já previstas no desenho

- Guardrails de SQL (allow/deny list, parser, políticas por usuário)
- Auditoria (executionId, usuário, SQL hash, tempo, status)
- Paginação real para queries arbitrárias (estratégia configurável)
- Plug de LLM via serviço dedicado no módulo `assistant`
