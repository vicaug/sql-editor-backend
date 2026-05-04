# sql-editor-backend

API Spring Boot para execução de SQL e sugestão de SQL com assistente de IA (NL2SQL).

## Visão geral

O backend expõe:

- execução de SQL (`/sql/run`)
- sugestão de SQL via IA com contexto de metadata (`/assistant/text-to-sql-query`)
- validação de SQL com contexto NL2SQL (`/assistant/nl2sql/validate`)

O fluxo do assistente hoje é:

1. Entende a pergunta (engine `auto`, `opennlp` ou `heuristic`)
2. Monta contexto de metadata (provider `askdata_like`)
3. Chama a LLM local (LM Studio, API OpenAI-compatible)
4. Opcionalmente valida com `SqlGuardService`

## Arquitetura

Estrutura em camadas:

- `presentation`: controllers HTTP
- `application`: serviços de orquestração e modelos de aplicação
- `domain`: modelos e contratos centrais
- `infrastructure`: integrações técnicas (JDBC, LLM client, etc.)
- `shared`: contrato padrão de API e tratamento global de exceções

## Endpoints

## Mapa de Fluxo (Endpoints)

### `POST /sql/run`

```text
Frontend/Client
   -> SqlExecutionController
      -> ExecuteSqlService
         -> JdbcSqlExecutionGateway
            -> PostgreSQL
         -> ExecuteSqlResult
      -> ApiResponse<ExecuteSqlResult>
```

### `POST /assistant/text-to-sql-query`

```text
Frontend/Client
   -> AiAssistantController
      -> AiAssistantRouterService (provider: lmstudio | openai)
         -> BaseAiSuggestionService
            -> MetadataContextProviderRouter (metadataProvider: askdata_like)
               -> AskDataLikeContextProvider
                  -> QueryUnderstandingService (auto | opennlp | heuristic)
                  -> AskDataLikeMetadataRetrievalService
                  -> PromptContextBuilder
            -> AiTextGenerator (LmStudioTextGenerator | OpenAiTextGenerator)
            -> (opcional) SqlGuardService
            -> AiAssistantResult
      -> ApiResponse<AiAssistantResult>
```

### `POST /assistant/nl2sql/validate`

```text
Frontend/Client
   -> AiAssistantController
      -> MetadataContextProviderRouter
         -> AskDataLikeContextProvider
      -> SqlGuardService.validate(...)
      -> SqlValidationResponse
      -> ApiResponse<SqlValidationResponse>
```

### 1) Executar SQL

- `POST /sql/run`

Request:

```json
{
  "sql": "SELECT * FROM customers LIMIT 10",
  "page": 0,
  "size": 50
}
```

### 2) Assistente de IA (sugestão SQL)

- `POST /assistant/text-to-sql-query` (principal)

Request:

```json
{
  "prompt": "Quero os 10 clientes com maior faturamento",
  "currentSql": "",
  "provider": "lmstudio",
  "metadataProvider": "askdata_like",
  "queryUnderstandingEngine": "auto",
  "enableSqlGuard": true
}
```

### 3) Validação NL2SQL

- `POST /assistant/nl2sql/validate`

Request:

```json
{
  "question": "traga pedidos pagos dos últimos 30 dias",
  "sql": "select * from orders where status = 'PAID'"
}
```

## Contrato de resposta

Todas as rotas retornam `ApiResponse<T>`:

```json
{
  "success": true,
  "data": {},
  "error": null,
  "meta": {
    "timestamp": "2026-05-04T12:00:00Z",
    "traceId": "uuid"
  }
}
```

Para erro:

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "AI_LMSTUDIO_TIMEOUT",
    "message": "Timeout ao chamar LM Studio...",
    "path": "/assistant/text-to-sql-query"
  },
  "meta": {
    "timestamp": "2026-05-04T12:00:00Z",
    "traceId": "uuid"
  }
}
```

## Configuração (application.properties)

Principais propriedades:

- `server.port=8080`
- `app.sql-execution.max-rows`
- `app.sql-execution.query-timeout-seconds`
- `app.sql-execution.default-page-size`

LLM local (LM Studio):

- `app.ai.provider=lmstudio`
- `app.ai.lmstudio.base-url=http://localhost:1234`
- `app.ai.lmstudio.model=mistralai/ministral-3-3b`
- `app.ai.lmstudio.temperature=0.0`
- `app.ai.lmstudio.timeout-seconds=25`

NLP:

- `app.nlp.pos-model-path=...`

## LM Studio

Este projeto usa a API OpenAI-compatible do LM Studio:

- `GET /v1/models`
- `POST /v1/chat/completions`

Garanta que:

1. O servidor do LM Studio está ativo em `localhost:1234`
2. O modelo configurado em `app.ai.lmstudio.model` está carregado no LM Studio

## Build e execução

Compile:

```bash
./mvnw -q -DskipTests compile
```

Run:

```bash
./mvnw spring-boot:run
```
