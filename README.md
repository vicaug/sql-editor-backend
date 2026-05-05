# sql-editor-backend

API Spring Boot para execução de SQL e fluxo de Text-to-SQL com IA local/remota.

## Visão Rápida

Endpoints principais:

- `POST /sql/run`
- `POST /assistant/text-to-sql-query`
- `POST /assistant/nl2sql/validate`

## Arquitetura por Serviço

O módulo `assistant` está organizado por responsabilidade de negócio:

- `assistant/metadata`
  - Modelos de metadata de contexto (`metadata/model/context`)
  - Gateway de acesso ao catálogo (`metadata/infrastructure/MetadataCatalogGateway`)

- `assistant/metadata_retrieval`
  - Entendimento da pergunta (`OpenNLP` + heurística)
  - Retrieval e ranking de metadata
  - Montagem de contexto textual para a LLM
  - Router/provider de contexto

- `assistant/llm`
  - Router de provider de LLM (`lmstudio`, `openai`)
  - Providers de geração (`AiTextGenerator`)
  - Prompt de sistema (`llm/prompt/TextToSqlPromptTemplate`)

- `assistant/nl2sql`
  - Orquestrador do fluxo completo (`nl2sql/application/Nl2SqlOrchestrator`)
  - Modelos de pipeline (`nl2sql/model`)
  - DTOs de validação (`nl2sql/presentation/model`)

- `assistant/sql_guard`
  - Validação de SQL gerado (`sql_guard/application/SqlGuardService`)

## Mapa de Fluxo (Text-to-SQL)

```text
POST /assistant/text-to-sql-query
  -> AiAssistantController
    -> LlmProviderRouter (escolhe provider: lmstudio/openai)
      -> BaseLlmSqlSuggester (adapter por provider)
        -> Nl2SqlOrchestrator (orquestra o pipeline)
          1) MetadataContextProviderRouter
             -> AskDataLikeContextProvider
                -> QueryUnderstandingRouter (opennlp/heuristic)
                -> AskDataLikeMetadataRetrievalService
                   -> MetadataCatalogGateway (eqt_metadata)
                -> PromptContextBuilder
          2) AiTextGenerator.generate(systemPrompt, userPrompt)
          3) SqlGuardService.validate(...) [opcional]
          4) AiAssistantResult
```

## Mapa de Fluxo (SQL Guard Validate)

```text
POST /assistant/nl2sql/validate
  -> AiAssistantController
    -> MetadataContextProviderRouter
    -> SqlGuardService.validate(...)
    -> SqlValidationResponse
```

## Contrato de Resposta

Todas as rotas usam `ApiResponse<T>`:

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

Exemplo de erro:

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

## Endpoints

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

### 2) Text-to-SQL

- `POST /assistant/text-to-sql-query`

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

## Configuração

Principais propriedades em `application.properties`:

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

Integração via API OpenAI-compatible:

- `GET /v1/models`
- `POST /v1/chat/completions`

Checklist:

1. Servidor LM Studio ativo em `localhost:1234`
2. Modelo configurado em `app.ai.lmstudio.model` carregado

## Build e Execução

Compile:

```bash
./mvnw -q -DskipTests compile
```

Run:

```bash
./mvnw spring-boot:run
```
