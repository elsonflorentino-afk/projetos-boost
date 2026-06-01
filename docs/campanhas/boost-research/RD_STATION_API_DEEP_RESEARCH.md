# RD Station APIs -- Deep Research Completo

**Data:** 2026-06-01
**Autor:** @analyst (Alex)
**Fontes:** developers.rdstation.com, ajuda.rdstation.com

---

## INDICE

1. [API RD Marketing (api.rd.services)](#1-api-rd-marketing)
2. [API RD CRM v1 (crm.rdstation.com/api/v1)](#2-api-rd-crm-v1)
3. [API RD CRM v2 (api.rd.services/crm/v2)](#3-api-rd-crm-v2)
4. [API RD Conversas (api.tallos.com.br)](#4-api-rd-conversas)
5. [Integracoes entre Modulos](#5-integracoes-entre-modulos)
6. [Referencia Rapida de Rate Limits](#6-referencia-rapida-de-rate-limits)
7. [Referencia de Erros](#7-referencia-de-erros)

---

# 1. API RD MARKETING

**Base URL:** `https://api.rd.services`

## 1.1 Autenticacao OAuth2

### Fluxo Completo

```
1. Criar App no App Store → recebe client_id + client_secret
2. Gerar authorization code → URL callback recebe ?code=XXX
3. Trocar code por tokens → access_token + refresh_token
4. Renovar access_token a cada 24h usando refresh_token
```

### Passo 2: Obter Authorization Code

O usuario autoriza o app e recebe o `code` na URL de callback configurada no App Store.

- O `code` expira no primeiro uso OU apos 60 minutos
- O `code` so pode ser usado UMA vez
- A URL de callback pode levar ate 1 hora para propagar apos alteracao

### Passo 3: Obter Tokens

```
POST https://api.rd.services/auth/token?token_by=code
Content-Type: application/json

{
  "client_id": "seu-client-id",
  "client_secret": "seu-client-secret",
  "code": "authorization-code-recebido"
}
```

**Response 200:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIs...",
  "expires_in": 86400,
  "refresh_token": "9YORmXHgOI32k-Y22tZWm-rsf--oFPr8JDCQIQhBEUY"
}
```

| Campo | Tipo | Descricao |
|-------|------|-----------|
| `access_token` | string (JWT) | Token Bearer para header Authorization |
| `expires_in` | integer | 86400 segundos = 24 horas |
| `refresh_token` | string | Token para renovacao (NAO expira) |

### Passo 4: Renovar Access Token

```
POST https://api.rd.services/auth/token
Content-Type: application/json

{
  "client_id": "seu-client-id",
  "client_secret": "seu-client-secret",
  "refresh_token": "refresh-token-atual"
}
```

**Response 200:**
```json
{
  "access_token": "novo-jwt-token...",
  "expires_in": 86400,
  "refresh_token": "novo-refresh-token"
}
```

**Response 401:**
```json
{
  "error_type": "INVALID_REFRESH_TOKEN",
  "error_message": "The provided refresh token is invalid or was revoked."
}
```

**IMPORTANTE:** O refresh_token retornado na renovacao e NOVO -- armazene-o para a proxima renovacao. O anterior e invalidado.

### Revogar Token

```
POST https://api.rd.services/auth/revoke
Content-Type: application/json

{
  "client_id": "seu-client-id",
  "client_secret": "seu-client-secret",
  "token": "access-token-ou-refresh-token"
}
```

### Uso do Access Token nas Requisicoes

```
GET https://api.rd.services/platform/contacts/email:lead@email.com
Authorization: Bearer {access_token}
Content-Type: application/json
```

**RESTRICAO:** Requisicoes com access_token DEVEM ser feitas do backend. Nunca enviar de frontend/browser (CORS bloqueado por seguranca).

---

## 1.2 Autenticacao API Key

Metodo simplificado, exclusivo para o endpoint de conversoes.

- Obtida no painel do RD Station Marketing
- Passada como query parameter `?api_key=XXX`
- NAO requer OAuth2

---

## 1.3 Endpoint: Conversions (via API Key)

### POST /platform/conversions

```
POST https://api.rd.services/platform/conversions?api_key=SUA_API_KEY
Content-Type: application/json
```

**Request Body:**
```json
{
  "event_type": "CONVERSION",
  "event_family": "CDP",
  "payload": {
    "conversion_identifier": "nome-da-conversao",
    "email": "lead@email.com",
    "name": "Nome do Lead",
    "job_title": "Cargo",
    "state": "SP",
    "city": "Sao Paulo",
    "country": "Brasil",
    "personal_phone": "11999999999",
    "mobile_phone": "11988888888",
    "twitter": "@handle",
    "facebook": "facebook.com/profile",
    "linkedin": "linkedin.com/in/profile",
    "website": "https://site.com",
    "company_name": "Empresa",
    "company_site": "https://empresa.com",
    "company_address": "Endereco",
    "client_tracking_id": "valor-cookie-_rdtrk",
    "traffic_source": "google",
    "traffic_medium": "paid-social",
    "traffic_campaign": "campanha-x",
    "traffic_value": "termo-utm",
    "available_for_mailing": true,
    "tags": ["tag1", "tag2"],
    "cf_campo_personalizado": "valor",
    "cf_patrimonio": "100k_200k",
    "legal_bases": [
      {
        "category": "communications",
        "type": "consent",
        "status": "granted"
      },
      {
        "category": "data_processing",
        "type": "consent",
        "status": "granted"
      }
    ]
  }
}
```

#### Campos do Payload

| Campo | Tipo | Obrigatorio | Descricao |
|-------|------|-------------|-----------|
| `conversion_identifier` | string | SIM | Nome/identificador da conversao |
| `email` | string | SIM | E-mail do contato |
| `name` | string | nao | Nome completo |
| `job_title` | string | nao | Cargo |
| `state` | string | nao | Estado/UF |
| `city` | string | nao | Cidade |
| `country` | string | nao | Pais |
| `personal_phone` | string | nao | Telefone fixo |
| `mobile_phone` | string | nao | Celular |
| `twitter` | string | nao | Perfil Twitter |
| `facebook` | string | nao | Perfil Facebook |
| `linkedin` | string | nao | Perfil LinkedIn |
| `website` | string | nao | Website |
| `company_name` | string | nao | Nome da empresa |
| `company_site` | string | nao | Site da empresa |
| `company_address` | string | nao | Endereco da empresa |
| `client_tracking_id` | string | nao | Cookie _rdtrk (tracking) |
| `traffic_source` | string | nao | utm_source ou cookie de origem |
| `traffic_medium` | string | nao | utm_medium |
| `traffic_campaign` | string | nao | utm_campaign |
| `traffic_value` | string | nao | utm_term |
| `available_for_mailing` | boolean | nao | Opt-in para e-mail |
| `tags` | array[string] | nao | Tags do contato |
| `cf_*` | string | nao | Campos customizados (prefixo cf_) |
| `legal_bases` | array[object] | nao | Base legal LGPD |

#### Legal Bases Object

| Campo | Tipo | Valores Validos |
|-------|------|-----------------|
| `category` | string | `communications`, `data_processing` |
| `type` | string | `consent`, `pre_existent_contract`, `legitimate_interest`, `judicial_process`, `vital_interest`, `public_interest` |
| `status` | string | `granted`, `declined` |

**Response 200:**
```json
{
  "event_uuid": "5408c5a3-4711-4f2e-8d0b-13407a3e30f3"
}
```

**Response 400:**
```json
{
  "errors": [
    {
      "error_type": "INVALID_OPTION",
      "error_message": "Must be one of the valid options.",
      "validation_rules": { "valid_options": ["CONVERSION"] },
      "path": "$.event_type"
    }
  ]
}
```

---

## 1.4 Endpoint: Events (via OAuth2)

### POST /platform/events

```
POST https://api.rd.services/platform/events
Authorization: Bearer {access_token}
Content-Type: application/json
```

**Tipos de Evento Suportados (event_type):**

| event_type | Descricao |
|------------|-----------|
| `CONVERSION` | Conversao padrao |
| `OPPORTUNITY` | Marcar como oportunidade |
| `SALE` | Oportunidade ganha |
| `OPPORTUNITY_LOST` | Oportunidade perdida |
| `CHAT_STARTED` | Inicio de chat |
| `CHAT_FINISHED` | Fim de chat |
| `CALL_FINISHED` | Fim de ligacao |
| `MEDIA_PLAYBACK_STARTED` | Inicio de reproducao de midia |
| `MEDIA_PLAYBACK_STOPPED` | Parada de reproducao de midia |
| `ECOMMERCE_CHECKOUT_STARTED` | Checkout iniciado |
| `ECOMMERCE_CART_ABANDONED` | Carrinho abandonado |
| `ECOMMERCE_ORDER_PLACED` | Pedido realizado |
| `ECOMMERCE_ORDER_PAID` | Pedido pago |
| `ECOMMERCE_ORDER_FULFILLED` | Pedido entregue |
| `ECOMMERCE_SHIPMENT_DELIVERED` | Envio entregue |
| `ECOMMERCE_ORDER_CANCELLED` | Pedido cancelado |
| `ECOMMERCE_ORDER_REFUNDED` | Pedido reembolsado |

**event_family:** Sempre `"CDP"`

**Payload para CONVERSION:** Mesmo do endpoint /platform/conversions.

**Payload para OPPORTUNITY:**
```json
{
  "event_type": "OPPORTUNITY",
  "event_family": "CDP",
  "payload": {
    "email": "lead@email.com",
    "funnel_name": "default"
  }
}
```

**NOTA:** Eventos de chat (CHAT_STARTED, CHAT_FINISHED) NAO aceitam campos personalizados (cf_*).

---

## 1.5 Endpoint: Batch Events

### POST /platform/events/batch

```
POST https://api.rd.services/platform/events/batch
Authorization: Bearer {access_token}
Content-Type: application/json
```

**Body:** Array de objetos evento (mesmo formato de /platform/events)

```json
[
  {
    "event_type": "CONVERSION",
    "event_family": "CDP",
    "payload": { "conversion_identifier": "form-1", "email": "a@b.com" }
  },
  {
    "event_type": "CONVERSION",
    "event_family": "CDP",
    "payload": { "conversion_identifier": "form-2", "email": "c@d.com" }
  }
]
```

**Limite:** Body maximo de 0.2 MB (200 KB).

**Response 200:**
```json
{
  "event_batch_uuid": "uuid-do-batch",
  "events": [
    { "event_uuid": "uuid-evento-1" },
    { "event_uuid": "uuid-evento-2" }
  ]
}
```

**NOTA:** A ordem dos UUIDs no response corresponde a ordem do array no request.

---

## 1.6 Endpoint: Contacts

### GET /platform/contacts/{identifier}:{value}

Consultar contato por UUID ou email.

```
GET https://api.rd.services/platform/contacts/email:lead@email.com
Authorization: Bearer {access_token}
```

**Path Params:**

| Param | Valores |
|-------|---------|
| `identifier` | `uuid` ou `email` |
| `value` | UUID ou endereco de e-mail |

**Response 200:**
```json
{
  "uuid": "abc-123-def",
  "name": "Nome do Lead",
  "email": "lead@email.com",
  "job_title": "Cargo",
  "birthdate": "1990-05-15",
  "bio": "Notas sobre o contato",
  "website": "https://site.com",
  "personal_phone": "11999999999",
  "mobile_phone": "11988888888",
  "city": "Sao Paulo",
  "state": "SP",
  "country": "Brasil",
  "twitter": "https://twitter.com/handle",
  "facebook": "https://facebook.com/profile",
  "linkedin": "https://linkedin.com/in/profile",
  "tags": ["tag1", "tag2"],
  "extra_emails": ["outro@email.com"],
  "legal_bases": [
    { "category": "communications", "type": "consent", "status": "granted" }
  ],
  "links": [
    { "rel": "self", "href": "https://api.rd.services/platform/contacts/uuid:abc-123-def", "media": "application/json", "type": "GET" },
    { "rel": "CONTACTS.EVENTS", "href": "https://api.rd.services/platform/contacts/abc-123-def/events", "media": "application/json", "type": "GET" },
    { "rel": "CONTACTS.FUNNELS", "href": "https://api.rd.services/platform/contacts/abc-123-def/funnels/default", "media": "application/json", "type": "GET" }
  ]
}
```

**Erros:**
- 401 Unauthorized: Token invalido
- 404 Not Found: Contato nao encontrado

### PATCH /platform/contacts/{identifier}:{value}

Criar ou atualizar contato. Se o contato ja existe, atualiza; se nao, cria.

```
PATCH https://api.rd.services/platform/contacts/email:lead@email.com
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "name": "Nome Atualizado",
  "job_title": "Novo Cargo",
  "tags": ["nova-tag"],
  "cf_patrimonio": "200k_500k",
  "legal_bases": [
    { "category": "communications", "type": "consent", "status": "granted" }
  ]
}
```

**Campos aceitaveis:** name, email (somente se identifier for uuid), job_title, birthdate, bio, website, personal_phone, mobile_phone, city, state, country, twitter, facebook, linkedin, tags, legal_bases, cf_* (campos customizados).

**ATENCAO:**
- Ao atualizar por email, NAO enviar `email` no body
- Tags fazem REPLACE (substituem as existentes). Para adicionar, use o endpoint de tags
- Nenhum evento de conversao e registrado
- Rate limit: 24 requisicoes por contato a cada 24h

### POST /platform/contacts (Criar)

Cria um NOVO contato. Se o email ja existir, retorna erro `EMAIL_ALREADY_IN_USE`.

```
POST https://api.rd.services/platform/contacts
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "email": "novo@email.com",
  "name": "Novo Lead",
  "tags": ["origem-api"]
}
```

### POST /platform/contacts/{identifier}:{value}/tag

Adicionar tags (acumulativo, nao substitui).

```
POST https://api.rd.services/platform/contacts/email:lead@email.com/tag
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "tags": ["tag-adicional"]
}
```

**NOTA:** Retorna erro `RESOURCE_NOT_FOUND` se o contato nao existir.

---

## 1.7 Endpoint: Contact Fields (Custom Fields)

### GET /platform/contacts/fields

Lista todos os campos (padrao e customizados) da conta.

```
GET https://api.rd.services/platform/contacts/fields
Authorization: Bearer {access_token}
```

**Scopes necessarios:** `read:fields`

**Response 200:**
```json
{
  "fields": [
    {
      "uuid": "field-uuid",
      "api_identifier": "email",
      "custom_field": false,
      "data_type": "STRING",
      "name": { "default": "Email", "pt-BR": "E-mail" },
      "label": { "default": "Email", "pt-BR": "E-mail" },
      "presentation_type": "EMAIL_INPUT",
      "validation_rules": {}
    },
    {
      "uuid": "cf-uuid",
      "api_identifier": "cf_patrimonio",
      "custom_field": true,
      "data_type": "STRING[]",
      "name": { "default": "Patrimonio", "pt-BR": "Patrimonio" },
      "label": { "default": "Patrimonio", "pt-BR": "Patrimonio" },
      "presentation_type": "COMBO_BOX",
      "validation_rules": {
        "valid_options": ["ate_50k", "50k_100k", "100k_200k", "200k_500k", "acima_500k"]
      }
    }
  ]
}
```

#### Data Types

| Tipo | Descricao |
|------|-----------|
| `BOOLEAN` | Verdadeiro/Falso |
| `STRING` | Texto |
| `STRING[]` | Multipla escolha |
| `INTEGER` | Numero inteiro |

#### Presentation Types

| Tipo | Descricao |
|------|-----------|
| `TEXT_INPUT` | Campo de texto |
| `EMAIL_INPUT` | Campo de e-mail |
| `COMBO_BOX` | Select/dropdown |
| `URL_INPUT` | Campo de URL |
| `TEXT_AREA` | Textarea |
| `RADIO_BUTTON` | Radio buttons |
| `PHONE_INPUT` | Campo de telefone |
| `NUMBER_INPUT` | Campo numerico |
| `CHECK_BOX` | Checkbox |
| `MULTIPLE_CHOICE` | Multipla escolha |

**Regra de nomenclatura cf_*:** prefixo `cf_` + lowercase + underscore. Maximo 64 caracteres.

### POST /platform/contacts/fields

Criar campo customizado.

### PATCH /platform/contacts/fields/{uuid}

Atualizar campo customizado.

---

## 1.8 Endpoint: Contact Events

### GET /platform/contacts/{uuid}/events

```
GET https://api.rd.services/platform/contacts/{UUID}/events?event_type=CONVERSION&order=created_at&direction=desc&page=1
Authorization: Bearer {access_token}
```

**Path Params:**

| Param | Tipo | Descricao |
|-------|------|-----------|
| `uuid` | string | UUID do contato |

**Query Params:**

| Param | Tipo | Obrigatorio | Valores | Descricao |
|-------|------|-------------|---------|-----------|
| `event_type` | string | SIM | `CONVERSION`, `OPPORTUNITY` | Tipo de evento |
| `order` | string | nao | `created_at` | Campo de ordenacao |
| `direction` | string | nao | `asc`, `desc` | Direcao |
| `page` | integer | nao | 1, 2, 3... | Pagina (default: 1) |

**Paginacao:** 10 eventos por pagina (default).

**Response 200:**
```json
[
  {
    "event_type": "CONVERSION",
    "event_family": "CDP",
    "event_identifier": "lp-ebook-portfolio",
    "event_timestamp": "2026-05-19T09:49:54.101-03:00",
    "payload": {
      "name": "Nome do Lead",
      "email": "lead@email.com",
      "conversion_identifier": "lp-ebook-portfolio",
      "cf_patrimonio": "100k_200k",
      "traffic_source": "meta",
      "traffic_medium": "paid-social",
      "traffic_campaign": "c4-conversao"
    }
  }
]
```

**Erros:**
- 401: Token invalido
- 404: Contato nao encontrado

---

## 1.9 Endpoint: Contact Funnels

### GET /platform/contacts/{identifier}:{value}/funnels/default

Consultar posicao do contato no funil.

```
GET https://api.rd.services/platform/contacts/email:lead@email.com/funnels/default
Authorization: Bearer {access_token}
```

### PUT /platform/contacts/{identifier}:{value}/funnels/default

Atualizar estagio do funil do contato.

---

## 1.10 Endpoint: Segmentations

### GET /platform/segmentations

Lista todas as segmentacoes da conta.

```
GET https://api.rd.services/platform/segmentations
Authorization: Bearer {access_token}
```

### GET /platform/segmentations/{id}/contacts

Lista contatos de uma segmentacao especifica.

```
GET https://api.rd.services/platform/segmentations/{segmentation_id}/contacts
Authorization: Bearer {access_token}
```

**Rate limits:** 120 req/min (Light/Basic/Pro) ou 240 req/min (Advanced)

---

## 1.11 Endpoint: Analytics

### GET /platform/analytics/funnel

Estatisticas do funil de vendas.

```
GET https://api.rd.services/platform/analytics/funnel?start_date=2026-05-01&end_date=2026-05-31&grouped_by=daily
Authorization: Bearer {access_token}
```

**Query Params:**

| Param | Tipo | Obrigatorio | Valores |
|-------|------|-------------|---------|
| `start_date` | string | SIM | yyyy-mm-dd |
| `end_date` | string | SIM | yyyy-mm-dd |
| `grouped_by` | string | nao | `daily`, `weekly`, `monthly` (default: daily) |

**Response 200:**
```json
{
  "account_id": 12345,
  "query_date": { "start_date": "2026-05-01", "end_date": "2026-05-31" },
  "grouped_by": "daily",
  "funnel": [
    {
      "reference_day": "2026-05-01",
      "visitors_count": 1500,
      "contacts_count": 85,
      "qualified_contacts_count": 23,
      "opportunities_count": 8,
      "sales_count": 2
    }
  ]
}
```

**Restricoes:**
- Plano: SOMENTE Advanced
- Dados disponiveis a partir de junho/2022

### GET /platform/analytics/conversions

Estatisticas de ativos de conversao (LPs, forms, pop-ups).

```
GET https://api.rd.services/platform/analytics/conversions?start_date=2026-05-01&end_date=2026-05-31&assets_type[]=LandingPage&assets_type[]=Forms
Authorization: Bearer {access_token}
```

**Query Params:**

| Param | Tipo | Obrigatorio | Descricao |
|-------|------|-------------|-----------|
| `start_date` | string | SIM | yyyy-mm-dd |
| `end_date` | string | SIM | yyyy-mm-dd |
| `assets_type[]` | array | nao | `LandingPage`, `Popup`, `Forms` |
| `asset_id` | array[int] | nao | IDs de ativos especificos |

**Response 200:**
```json
{
  "account_id": 12345,
  "query_date": { "start_date": "2026-05-01", "end_date": "2026-05-31" },
  "assets_type": ["LandingPage"],
  "conversions": [
    {
      "asset_id": 789,
      "asset_identifier": "lp-ebook-portfolio",
      "asset_type": "LandingPage",
      "asset_created_at": "2026-03-15T10:00:00.000Z",
      "asset_updated_at": "2026-05-20T15:30:00.000Z",
      "visits_count": 3500,
      "conversions_count": 280,
      "conversion_rate": 0.08
    }
  ]
}
```

**Restricoes:**
- Plano: Pro (ultimos 45 dias) ou Advanced (sem limite)
- Dados consolidados em UTC, atualizacao em ate 24h
- Dados disponiveis: LPs desde jun/2022, Forms/Pop-ups desde out/2022

### GET /platform/analytics/emails

Estatisticas de e-mail marketing.

```
GET https://api.rd.services/platform/analytics/emails?start_date=2026-05-01&end_date=2026-05-31
Authorization: Bearer {access_token}
```

**Response 200:**
```json
{
  "account_id": 12345,
  "query_date": { "start_date": "2026-05-01", "end_date": "2026-05-31" },
  "emails": [
    {
      "campaign_id": 456,
      "campaign_name": "[ebook-1] entrega_portfolio_20260512",
      "send_at": "2026-05-12T14:00:00.000Z",
      "contacts_count": 500,
      "email_dropped_count": 10,
      "email_delivered_count": 485,
      "email_bounced_count": 5,
      "email_opened_count": 195,
      "email_clicked_count": 67,
      "email_unsubscribed_count": 3,
      "email_spam_reported_count": 0,
      "email_delivered_rate": 0.97,
      "email_opened_rate": 0.402,
      "email_clicked_rate": 0.138,
      "email_spam_reported_rate": 0.0
    }
  ]
}
```

**Restricoes:**
- Plano: Pro (45 dias) ou Advanced (sem limite)
- Dados desde agosto/2021, consolidados ate D-1 em UTC

---

## 1.12 Webhooks (Marketing)

### Criar Webhook

```
POST https://api.rd.services/integrations/webhooks
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "event_type": "WEBHOOK.CONVERTED",
  "entity_type": "CONTACT",
  "url": "https://meu-server.com/webhook",
  "http_method": "POST",
  "event_identifiers": ["lp-ebook-portfolio", "lp-analise"],
  "include_relations": ["COMPANY", "CONTACT_FUNNEL"]
}
```

**Body Params:**

| Campo | Tipo | Obrigatorio | Descricao |
|-------|------|-------------|-----------|
| `event_type` | string | SIM | Tipo de evento (ver tabela) |
| `entity_type` | string | SIM | Somente `CONTACT` |
| `url` | string | SIM | URL destino (deve retornar 2xx na validacao) |
| `http_method` | string | SIM | Somente `POST` |
| `event_identifiers` | array[string] | nao | Filtro de conversoes (so para WEBHOOK.CONVERTED) |
| `include_relations` | array[string] | nao | `COMPANY`, `CONTACT_FUNNEL` |

**Eventos Disponiveis:**

| event_type | Descricao |
|------------|-----------|
| `WEBHOOK.CONVERTED` | Lead converte (LP, form, pop-up, integracao) |
| `WEBHOOK.MARKED_OPPORTUNITY` | Lead marcado como oportunidade |
| `crm_deal_created` | Deal criado no CRM |
| `crm_deal_updated` | Deal atualizado no CRM |
| `crm_deal_deleted` | Deal deletado no CRM |

**Response 201:**
```json
{
  "uuid": "webhook-uuid",
  "created_at": "2026-06-01T10:00:00.000Z",
  "updated_at": "2026-06-01T10:00:00.000Z",
  "platform_account_id": "account-id",
  "event_type": "WEBHOOK.CONVERTED",
  "entity_type": "CONTACT",
  "url": "https://meu-server.com/webhook",
  "http_method": "POST",
  "source": "MKT",
  "status": "active"
}
```

**Validacao:** O RD faz um request de validacao na URL ao criar. Se nao retornar 2xx, a criacao falha. URLs duplicadas para o mesmo event_type sao rejeitadas.

### Payload do Webhook WEBHOOK.CONVERTED

```json
{
  "event_type": "WEBHOOK.CONVERTED",
  "entity_type": "CONTACT",
  "event_identifier": "lp-analise",
  "timestamp": "2026-06-01T10:30:00.000-03:00",
  "event_timestamp": "2026-06-01T10:29:55.000-03:00",
  "contact": {
    "uuid": "contact-uuid",
    "email": "lead@email.com",
    "name": "Nome do Lead",
    "job_title": "Investidor",
    "bio": "",
    "website": "",
    "personal_phone": "",
    "mobile_phone": "11999999999",
    "city": "Sao Paulo",
    "facebook": "",
    "linkedin": "",
    "twitter": "",
    "tags": ["meta-ads", "ebook"],
    "company": { "name": "Empresa X" },
    "cf_patrimonio": "100k_200k",
    "cf_investe_cripto": "sim",
    "lifecycle_stage": "Lead",
    "opportunity": false,
    "contact_owner_email": "vendedor@boost.com",
    "interest": 75,
    "fit": 60,
    "origin": { "source": "meta", "medium": "paid-social" },
    "legal_bases": [
      { "category": "communications", "type": "consent", "status": "granted" }
    ]
  }
}
```

### Payload do Webhook WEBHOOK.MARKED_OPPORTUNITY

Mesmo formato, com `event_identifier` = `"default"` e `contact.opportunity` = `true`.

**IMPORTANTE:**
- Campos customizados (cf_*) so aparecem se preenchidos
- Payload e padrao e imutavel (nao customizavel)
- Importacoes manuais e atualizacoes manuais NAO disparam webhooks
- O webhook dispara TODA vez que o gatilho e cumprido (nao so na primeira vez)

### Requisitos do Endpoint Receptor

- Aceitar JSON
- Certificado SSL/TLS valido
- Responder com status 2xx a requests POST
- Processar o pacote completo de dados

### Autenticacao Customizada

```json
{
  "auth_header": "X-Custom-Auth",
  "auth_key": "minha-chave-secreta"
}
```

Se nao configurada, apenas `Content-Type: application/json` e enviado.

### Gerenciamento de Webhooks

| Operacao | Metodo | Endpoint |
|----------|--------|----------|
| Listar | GET | `/integrations/webhooks` |
| Obter | GET | `/integrations/webhooks/{uuid}` |
| Criar | POST | `/integrations/webhooks` |
| Atualizar | PUT | `/integrations/webhooks/{uuid}` |
| Deletar | DELETE | `/integrations/webhooks/{uuid}` |

---

# 2. API RD CRM v1

**Base URL:** `https://crm.rdstation.com/api/v1`

## 2.1 Autenticacao

Token de instancia, unico e imutavel por usuario.

**Uso:** Query parameter `?token=SEU_TOKEN` ou header `Authorization: Bearer SEU_TOKEN`

**Obtencao:** Painel do RD CRM > Configuracoes > Token de integracao.

**Verificacao do token:**
```
GET https://crm.rdstation.com/api/v1/token/check?token=SEU_TOKEN
```

**Permissoes:** O token herda o nivel de visibilidade do usuario:
- **Restrito:** So ve proprios registros
- **Equipe:** Ve registros da equipe
- **Geral:** Acesso total

**Planos:** Basic, Pro ou Advanced.

**Rate limit:** 120 requisicoes/minuto.

**Limite de registros:** 10.000 registros por requisicao (para deals, organizations, contacts).

---

## 2.2 Deals (Negociacoes)

### GET /deals -- Listar

```
GET https://crm.rdstation.com/api/v1/deals?token=XXX&page=1&limit=50&win=null
```

**Query Params:**

| Param | Tipo | Default | Descricao |
|-------|------|---------|-----------|
| `page` | string | 1 | Numero da pagina |
| `limit` | string | 20 (max 200) | Resultados por pagina |
| `order` | string | created_at | Campo de ordenacao |
| `direction` | string | desc | asc ou desc |
| `name` | string | - | Filtrar por nome |
| `exact_name` | string | false | Match exato do nome |
| `win` | string | - | true (ganho), false (perdido), null (aberto) |
| `user_id` | string | - | ID do responsavel |
| `closed_at` | string | - | true (fechados), false (abertos/pausados) |
| `closed_at_period` | string | - | Habilita filtro de data de fechamento |
| `created_at_period` | string | - | Habilita filtro de data de criacao |
| `prediction_date_period` | string | - | Habilita filtro de data de previsao |
| `start_date` | string | - | Inicio do periodo (2020-12-14T15:00:00) |
| `end_date` | string | - | Fim do periodo |
| `campaign_id` | string | - | ID da campanha |
| `deal_stage_id` | string | - | ID da etapa do funil |
| `deal_lost_reason_id` | string | - | ID do motivo de perda |
| `deal_pipeline_id` | string | - | ID do funil |
| `organization` | string | - | ID da empresa |
| `hold` | string | - | true = somente pausados |
| `product_presence` | string | - | false/true ou IDs separados por virgula |
| `next_page` | string | - | Cursor de paginacao (do response anterior) |

**Response 200:**
```json
{
  "deals": [
    {
      "_id": "64a1b2c3d4e5f6g7h8i9j0k1",
      "id": "64a1b2c3d4e5f6g7h8i9j0k1",
      "name": "Deal Andre - Crypto Intelligence PRO",
      "closed_at": null,
      "deal_lost_reason_id": null,
      "prediction_date": "2026-06-15",
      "win": null
    }
  ],
  "has_more": true,
  "total": 875
}
```

**Paginacao:** Use `next_page` do response para navegar. Alternativa: incrementar `page`.

### GET /deals/{deal_id} -- Obter Deal

```
GET https://crm.rdstation.com/api/v1/deals/{deal_id}?token=XXX
```

### POST /deals -- Criar Deal

```
POST https://crm.rdstation.com/api/v1/deals?token=XXX
Content-Type: application/json

{
  "deal": {
    "name": "Novo Deal via API",
    "deal_stage_id": "stage-id",
    "prediction_date": "2026-07-01"
  },
  "contacts": [
    {
      "name": "Lead Teste",
      "emails": [{ "email": "lead@email.com" }],
      "phones": [{ "phone": "11999999999", "type": "home" }]
    }
  ],
  "organization": {
    "name": "Empresa X"
  },
  "deal_source": { },
  "deal_products": [],
  "campaign": {}
}
```

**RESTRICAO:** So deals com status `ongoing` podem ser criados.

**Campos do objeto deal:**

| Campo | Tipo | Descricao |
|-------|------|-----------|
| `name` | string | Nome do deal |
| `deal_stage_id` | string | ID da etapa do funil |
| `prediction_date` | string | Data prevista de fechamento |
| `user_id` | string | Responsavel |
| `deal_pipeline_id` | string | ID do funil de vendas |

### PUT /deals/{deal_id} -- Atualizar Deal

```
PUT https://crm.rdstation.com/api/v1/deals/{deal_id}?token=XXX
Content-Type: application/json

{
  "deal": {
    "deal_stage_id": "novo-stage-id",
    "win": true,
    "closed_at": "2026-06-01T10:00:00-03:00",
    "name": "Nome Atualizado"
  }
}
```

Todos os campos sao opcionais. Envie apenas os que deseja alterar.

### GET /deals/{deal_id}/contacts -- Contatos do Deal

```
GET https://crm.rdstation.com/api/v1/deals/{deal_id}/contacts?token=XXX&page=1&limit=20
```

**Response 200:**
```json
{
  "contacts": [
    {
      "_id": "contact-id",
      "id": "contact-id",
      "name": "Lead Teste",
      "title": "Investidor",
      "emails": [{ "email": "lead@email.com", "_id": "email-id" }],
      "phones": [{ "phone": "11999999999", "type": "home", "whatsapp": true }],
      "facebook": "",
      "linkedin": "",
      "skype": "",
      "organization_id": "org-id",
      "organization": { "_id": "org-id", "name": "Empresa X" },
      "deal_ids": ["deal-id-1", "deal-id-2"],
      "contact_custom_fields": [
        { "custom_field_id": "field-id", "value": "valor" }
      ],
      "legal_bases": [
        { "category": "communications", "status": "granted", "type": "consent" }
      ],
      "birthday": { "day": 15, "month": 5, "year": 1990 },
      "created_at": "2026-03-01T10:00:00.000Z",
      "updated_at": "2026-05-30T15:00:00.000Z"
    }
  ],
  "has_more": false,
  "total": 1
}
```

---

## 2.3 Pipelines (Funis de Venda)

### GET /deal_pipelines -- Listar

```
GET https://crm.rdstation.com/api/v1/deal_pipelines?token=XXX&page=1&limit=20
```

### GET /deal_pipelines/{id} -- Obter

```
GET https://crm.rdstation.com/api/v1/deal_pipelines/{deal_pipeline_id}?token=XXX
```

### PUT /deal_pipelines/{id} -- Atualizar

```
PUT https://crm.rdstation.com/api/v1/deal_pipelines/{deal_pipeline_id}?token=XXX
```

---

## 2.4 Stages (Etapas do Funil)

### GET /deal_stages -- Listar

```
GET https://crm.rdstation.com/api/v1/deal_stages?token=XXX
```

### GET /deal_stages/{id} -- Obter

```
GET https://crm.rdstation.com/api/v1/deal_stages/{deal_stage_id}?token=XXX
```

### POST /deal_stages -- Criar

```
POST https://crm.rdstation.com/api/v1/deal_stages?token=XXX
```

### PUT /deal_stages/{id} -- Atualizar

```
PUT https://crm.rdstation.com/api/v1/deal_stages/{deal_stage_id}?token=XXX
```

---

## 2.5 Contacts (CRM)

### GET /contacts -- Listar

```
GET https://crm.rdstation.com/api/v1/contacts?token=XXX&page=1&limit=50
```

### POST /contacts -- Criar

```
POST https://crm.rdstation.com/api/v1/contacts?token=XXX
Content-Type: application/json

{
  "contact": {
    "name": "Nome do Contato",
    "title": "Cargo",
    "emails": [{ "email": "contato@email.com" }],
    "phones": [{ "phone": "11999999999", "type": "home" }],
    "organization_id": "org-id",
    "facebook": "",
    "linkedin": "",
    "skype": "",
    "birthday": { "day": 15, "month": 5, "year": 1990 },
    "contact_custom_fields": [
      { "custom_field_id": "field-id", "value": "valor" }
    ],
    "deal_ids": ["deal-id"],
    "legal_bases": [
      { "category": "communications", "status": "granted", "type": "consent" }
    ]
  }
}
```

### PUT /contacts/{id} -- Atualizar

```
PUT https://crm.rdstation.com/api/v1/contacts/{contact_id}?token=XXX
```

---

## 2.6 Webhooks (CRM v1)

### POST /webhooks -- Criar

```
POST https://crm.rdstation.com/api/v1/webhooks?token=XXX
Content-Type: application/json

{
  "event_type": "crm_deal_created",
  "url": "https://meu-server.com/crm-webhook",
  "http_method": "POST"
}
```

**Entidades com webhooks disponiveis:**

| Entidade | Eventos |
|----------|---------|
| Deals (Negociacoes) | created, updated, deleted |
| Contacts (Contatos) | created, updated, deleted |
| Organizations (Empresas) | created, updated, deleted |
| Activities (Anotacoes) | created, updated, deleted |
| Campaigns (Campanhas) | created, updated, deleted |
| Sources (Fontes) | created, updated, deleted |
| Tasks (Tarefas) | created, updated, deleted |
| Products (Produtos) | created, updated, deleted |
| Deal Lost Reasons | created, updated, deleted |
| Deal Pipelines | created, updated, deleted |
| Deal Stages | created, updated, deleted |
| Custom Fields | created, updated, deleted |
| Teams (Equipes) | created, updated, deleted |
| Users (Usuarios) | created, updated, deleted |

**Payload padrao do webhook CRM:**
```json
{
  "event_name": "crm_deal_updated",
  "event_timestamp": "2026-06-01T10:30:00.000Z",
  "transaction_uuid": "uuid-unico-do-evento",
  "document": {
    "_id": "deal-id",
    "name": "Nome do Deal",
    "deal_stage": { "_id": "stage-id", "name": "Reuniao Marcada" },
    "win": null,
    "closed_at": null,
    "contacts": [...],
    "user": { "_id": "user-id", "name": "Vendedor" }
  }
}
```

**NOTAS:**
- Payload contem TODAS as informacoes disponiveis (nao customizavel)
- Requisitos: URL HTTPS com certificado SSL, aceitar POST, retornar 2xx
- Webhooks podem ser SUSPENSOS automaticamente se entregas falharem
- Existe retry strategy (detalhes exatos nao documentados publicamente)

### Gerenciamento

| Operacao | Metodo | Endpoint |
|----------|--------|----------|
| Listar todos | GET | `/webhooks?token=XXX` |
| Obter um | GET | `/webhooks/{uuid}?token=XXX` |
| Criar | POST | `/webhooks?token=XXX` |
| Atualizar | PUT | `/webhooks/{uuid}?token=XXX` |
| Deletar | DELETE | `/webhooks/{uuid}?token=XXX` |

---

# 3. API RD CRM v2

**Base URL:** `https://api.rd.services/crm/v2`

## 3.1 Autenticacao

OAuth2 (mesmo fluxo do Marketing, porem credenciais SEPARADAS por produto).

**Token endpoint:** `https://api.rd.services/oauth2/`
**Expiracao access_token:** 7200 segundos (2 horas) -- DIFERENTE do Marketing que e 24h.

**IMPORTANTE:** A cada renovacao, um NOVO refresh_token e retornado. Armazenar sempre o mais recente.

## 3.2 Endpoints Principais

### GET /crm/v2/deals -- Listar

```
GET https://api.rd.services/crm/v2/deals
Authorization: Bearer {access_token}
```

### GET /crm/v2/deals/{id} -- Obter

```
GET https://api.rd.services/crm/v2/deals/{id}
Authorization: Bearer {access_token}
```

### POST /crm/v2/deals -- Criar

```
POST https://api.rd.services/crm/v2/deals
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "data": {
    "name": "Deal via API v2",
    "status": "ongoing",
    "stage_id": "stage-uuid",
    "pipeline_id": "pipeline-uuid",
    "contact_id": "contact-uuid",
    "organization_id": "org-uuid",
    "owner_id": "user-uuid",
    "expected_close_date": "2026-07-01",
    "rating": 5,
    "campaign_id": "campaign-uuid"
  }
}
```

**RESTRICAO:** Somente status `ongoing` na criacao.

**Response codes:** 201 (criado), 400, 401, 403, 422, 429, 500.

### PUT /crm/v2/deals/{id} -- Atualizar

```
PUT https://api.rd.services/crm/v2/deals/{id}
Authorization: Bearer {access_token}
```

### GET /crm/v2/pipelines -- Listar Funis

```
GET https://api.rd.services/crm/v2/pipelines
Authorization: Bearer {access_token}
```

### PUT /crm/v2/pipelines/{id} -- Atualizar Funil

### PUT /crm/v2/pipelines/{id}/stages/{stage_id} -- Atualizar Stage

### Outros Endpoints v2

| Metodo | Endpoint | Descricao |
|--------|----------|-----------|
| GET | /crm/v2/deals/{id}/notes | Listar anotacoes |
| GET | /crm/v2/deals/{id}/files | Listar arquivos |
| PUT | /crm/v2/deals/{id}/products/{product_id} | Atualizar produto do deal |
| GET | /crm/v2/users | Listar usuarios |
| GET | /crm/v2/teams | Listar equipes |

---

# 4. API RD CONVERSAS

**Base URL:** `https://api.tallos.com.br`

**Autenticacao:** JWT Bearer Token

**Canais suportados:** WhatsApp (principal), Instagram, Messenger, Telegram, E-mail

**Plano minimo:** Varia por endpoint (vários requerem Advanced)

## 4.1 Autenticacao

Todas as requisicoes v2 requerem JWT Bearer Token no header:

```
Authorization: Bearer {jwt_token}
```

**Seguranca adicional:**
- Criptografia disponivel para endpoints especificos (plano Professional)
- CORS implementado conforme especificacoes W3C

## 4.2 Endpoints de Contatos

### GET /v2/customers -- Listar Contatos

```
GET https://api.tallos.com.br/v2/customers?limit=50&page=1&channels=whatsapp
Authorization: Bearer {jwt_token}
```

**Query Params:**

| Param | Tipo | Default | Descricao |
|-------|------|---------|-----------|
| `limit` | integer | - | Items por pagina |
| `page` | integer | 1 | Pagina |
| `channels` | string | - | `telegram`, `whatsapp` (separados por virgula) |

**Response Headers:**
- `X-Limit`: Items na pagina atual
- `X-Page`: Pagina atual
- `X-Pages`: Total de paginas
- `X-Total`: Total de items

### GET /v2/contacts/cpf/{cpf} -- Buscar por CPF

### GET /v2/contacts/phone/{phone} -- Buscar por Telefone

### POST /v2/contacts/whatsapp-business-by-brokers -- Criar Contato WhatsApp (Recomendado)

```
POST https://api.tallos.com.br/v2/contacts/whatsapp-business-by-brokers
Authorization: Bearer {jwt_token}
Content-Type: application/json
```

### PUT /v2/contacts/whatsapp-business-by-brokers -- Atualizar Contato WhatsApp

### POST /v2/contacts/bulk -- Criar Multiplos Contatos (Assincrono)

Plano: Advanced. Processamento assincrono.

### DELETE /v2/contacts -- Deletar Multiplos Contatos

Plano: Advanced. Filtro por tag, integracao ou todos.

### PUT /v2/contacts/phone/{phone} -- Atualizar por Telefone

Plano: Advanced.

**NOTA:** Endpoints `POST /v1/contacts` e `POST /v2/contacts/whatsapp-business` estao DEPRECATED.

## 4.3 Endpoints de Mensagens

### POST /v2/messages -- Enviar Mensagem

```
POST https://api.tallos.com.br/v2/messages
Authorization: Bearer {jwt_token}
Content-Type: application/json
```

Envia mensagem de texto para um contato.

### POST /v3/messages/templates -- Enviar Mensagem Template (Recomendado)

```
POST https://api.tallos.com.br/v3/messages/templates
Authorization: Bearer {jwt_token}
Content-Type: application/json
```

**IMPORTANTE:** Disponivel na v3. NAO cria atendimento. Use para mensagens ativas (active messages) via templates aprovados do WhatsApp Business.

### GET /v2/templates -- Listar Templates

```
GET https://api.tallos.com.br/v2/templates
Authorization: Bearer {jwt_token}
```

Lista templates de mensagem disponiveis.

### GET /v2/messages/history -- Historico de Conversas

```
GET https://api.tallos.com.br/v2/messages/history?customer_id=MONGO_ID&limit=50&page=1&channel=whatsapp
Authorization: Bearer {jwt_token}
```

**Query Params:**

| Param | Tipo | Obrigatorio | Default | Max | Descricao |
|-------|------|-------------|---------|-----|-----------|
| `customer_id` | string | SIM | - | - | ID do contato (MongoDB ObjectId) |
| `limit` | integer | nao | 15 | 100 | Registros por pagina |
| `page` | integer | nao | 1 | - | Pagina |
| `channel` | array | nao | whatsapp | - | email, instagram, megasac, messenger, telegram, whatsapp |
| `start_date` | date | nao | - | - | ISO 8601 |
| `end_date` | date | nao | - | - | ISO 8601 |
| `sent_by` | array | nao | customer, operator | - | bot, customer, operator |
| `type` | array | nao | text | - | audio, document, image, text, video |

**IMPORTANTE:** Plano Advanced only. Response retorna dados CRIPTOGRAFADOS.

## 4.4 Endpoints de Workflows e Flows

### GET /v2/workflows -- Listar Workflows

```
GET https://api.tallos.com.br/v2/workflows
Authorization: Bearer {jwt_token}
```

### GET /v2/flows -- Listar Flows

```
GET https://api.tallos.com.br/v2/flows
Authorization: Bearer {jwt_token}
```

### POST /v2/flows/reset -- Resetar Processos de Flow

Reinicia processos de flow para contatos.

## 4.5 Atendimento

### POST /v2/attendances/forward -- Encaminhar Atendimento

```
POST https://api.tallos.com.br/v2/forward-to-customer
Authorization: Bearer {jwt_token}
Content-Type: application/json

{
  "customer": "customer-id",
  "flow": "flow-id"
}
```

## 4.6 Campanhas

### GET /v1/campaigns -- Listar Campanhas

```
GET https://api.tallos.com.br/v1/campaigns
Authorization: Bearer {jwt_token}
```

### GET /v1/campaign/{id} -- Detalhes da Campanha

Retorna segmentacao, template e estatisticas agregadas.

## 4.7 Custom Fields

### GET /v2/custom-fields -- Listar (Beta)
### POST /v2/custom-fields -- Criar (Beta)
### GET /v2/custom-fields/{id} -- Obter (Beta)
### PUT /v2/custom-fields/{id} -- Atualizar (Beta)
### DELETE /v2/custom-fields/{id} -- Deletar (Beta)

## 4.8 Relatorios

### GET /v4/reports -- Listar Relatorios

```
GET https://api.tallos.com.br/v4/reports?start_date=2026-05-01&end_date=2026-05-31&channel=whatsapp&status=closed&type=operators
Authorization: Bearer {jwt_token}
```

**Query Params:**

| Param | Tipo | Obrigatorio | Descricao |
|-------|------|-------------|-----------|
| `start_date` | date | SIM | Inicio do periodo |
| `end_date` | date | SIM | Fim (max 3 meses) |
| `page` | int | nao | Pagina |
| `limit` | int | nao | Registros por pagina |
| `department` | string | nao | Filtro por departamento |
| `channel` | string | nao | email, instagram, whatsapp, etc |
| `employee` | string | nao | ID do operador |
| `status` | string | nao | opened ou closed |
| `type` | string | nao | customers, operators, chatbots, rejecteds |

## 4.9 Analytics

### GET /v1/analytics/attendances/retention -- Retencao de Atendimentos
### GET /v1/analytics/attendances/reviews-average -- Media de Avaliacoes
### GET /v1/analytics/attendances/summary -- Resumo de Atendimentos
### GET /v1/analytics/contacts/origin -- Origem dos Contatos

## 4.10 Integracoes WhatsApp

### GET /v2/integrations/whatsapp -- Listar Integracoes
### GET /v2/integrations/whatsapp/official -- Listar Integracoes Oficiais

## 4.11 Wallets (Carteiras)

### GET /v2/wallets -- Listar Carteiras
### POST /v2/wallets/{id}/contacts -- Adicionar Contato a Carteira
### DELETE /v2/wallets/{id}/contacts/{contact_id} -- Remover Contato

## 4.12 Funcionarios

### GET /v2/employees -- Listar Funcionarios
### POST /v2/employees -- Criar Funcionarios (Assincrono, Advanced)
### GET /v1/employee/{id} -- Obter Funcionario
### PUT /v2/employees/{id}/activate -- Ativar
### PUT /v2/employees/{id}/deactivate -- Desativar
### PUT /v2/employees/deactivate -- Desativar Multiplos (Advanced)

## 4.13 Jobs (Operacoes Assincronas)

### GET /v2/jobs/{job_id} -- Verificar Status

Verificar status de operacoes assincronas (bulk contacts, etc).

---

# 5. INTEGRACOES ENTRE MODULOS

## 5.1 Marketing → CRM (Integracao Nativa)

### Como Funciona

A integracao nativa entre RD Station Marketing e CRM permite envio automatico de leads como oportunidades.

**3 Tipos de Gatilho:**

| Gatilho | Descricao |
|---------|-----------|
| Marcacao de Oportunidade | TODOS os leads marcados como oportunidade no Marketing sao enviados ao CRM |
| Conversao Especifica | Escolher LP/Pop-up/Form especifico cujas conversoes vao pro CRM |
| Automacao de Fluxo | Via fluxo de automacao, ao atingir criterio (ex: lead scoring >= X) |

### Fluxo de Dados

```
Lead converte no Marketing
  → Lead scoring avalia (interesse + perfil)
    → Se >= threshold OU marcado manualmente como oportunidade
      → Cria negociacao (deal) no CRM
        → Deal vinculado a Cliente + Contato
```

### Mapeamento de Campos

Configuravel na interface de integracao:
- Campos padrao sao mapeados automaticamente (email, nome, telefone, empresa)
- Campos personalizados (cf_*) devem ser mapeados manualmente
- Campos obrigatorios no CRM DEVEM ser mapeados para evitar erro

### Campo from_rdsm_integration

Deals criados pela integracao automatica Marketing → CRM recebem uma marcacao interna indicando que vieram da integracao. Isso permite filtrar deals automaticos vs manuais.

## 5.2 CRM → Marketing (Retorno de Status)

O CRM envia status de volta para o Marketing:
- Lead ganhou → Marcado como "cliente" no Marketing
- Lead perdeu → Marcado como "oportunidade perdida"
- Informacoes de venda retornam para alimentar o funil

## 5.3 Conversas → Marketing

### Via Webhook do Conversas

Configurar webhook no RD Conversas e inserir URL gerada no fluxo de automacao do Marketing.

### Eventos de Chat

```
POST https://api.rd.services/platform/events
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "event_type": "CHAT_STARTED",
  "event_family": "CDP",
  "payload": {
    "email": "lead@email.com",
    "chat_subject": "Atendimento WhatsApp"
  }
}
```

**NOTA:** Eventos de chat NAO aceitam campos personalizados (cf_*).

## 5.4 Conversas → CRM

Via integracao nativa: possivel criar deals/oportunidades diretamente do chat no Conversas para o CRM.

## 5.5 Sincronizacao de Tags

- Marketing → CRM: Tags NAO sao sincronizadas automaticamente
- Tags devem ser gerenciadas separadamente em cada modulo
- Para sincronizar: usar API de ambos os lados + webhook como trigger

## 5.6 Fluxo Completo Recomendado (API)

```
1. Lead converte na LP (CAPI/Form)
   → POST /platform/conversions (Marketing API)

2. Webhook WEBHOOK.CONVERTED dispara
   → Receber payload com dados do lead

3. Processar e qualificar (seu backend)
   → Verificar patrimonio, cf_* fields

4. Se qualificado, marcar oportunidade
   → POST /platform/events (event_type: OPPORTUNITY)

5. Integracao nativa cria deal no CRM
   OU
   → POST /crm/v1/deals (criar deal manualmente)

6. Notificar equipe via Conversas
   → POST /v3/messages/templates (enviar template WhatsApp)
```

---

# 6. REFERENCIA RAPIDA DE RATE LIMITS

## Marketing (api.rd.services)

| Endpoint | Limite | Plano |
|----------|--------|-------|
| Contacts (todos metodos) | 120 req/min | Light, Basic, Pro |
| Contacts (todos metodos) | 500 req/min | Advanced |
| PATCH contacts (por contato) | 24 req/24h | Todos |
| Tags POST (por contato) | 24 req/24h | Todos |
| Tags POST (por conta) | 15.000 req/dia | Todos |
| Events POST (por lead) | 120 req/24h | Todos |
| Events POST (por conta) | 120 req/min | Light, Basic, Pro |
| Events POST (por conta) | 500 req/min | Advanced |
| Segmentations GET | 120 req/min | Light, Basic, Pro |
| Segmentations GET | 240 req/min | Advanced |
| Analytics GET | 60 req/h | Pro, Advanced |
| Landing Pages/Pop-ups/Forms | 60 req/h | Todos |
| Workflows - Listar | 40 req/h | Todos |
| Workflows - Obter | 1-15 req/h | Varia por plano |
| Workflows - Inserir leads | 1-100 req/h | Varia por plano |
| Workflows - Consultar leads | 1-12 req/h | Varia por plano |

## CRM v1 (crm.rdstation.com)

| Endpoint | Limite |
|----------|--------|
| Todos os endpoints | 120 req/min |
| Max registros por requisicao | 10.000 |

## CRM v2 (api.rd.services/crm/v2)

| Endpoint | Limite |
|----------|--------|
| Todos os endpoints | 120 req/min (estimado, nao documentado explicitamente) |

## Conversas (api.tallos.com.br)

Nao documentados publicamente com valores especificos.

---

# 7. REFERENCIA DE ERROS

## 7.1 Erros de Autenticacao (Marketing)

| HTTP | error_type | Causa | Solucao |
|------|-----------|-------|---------|
| 401 | `UNAUTHORIZED` | Token invalido ou malformado | Gerar novo access_token |
| 401 | `ACCESS_DENIED` | client_id ou client_secret incorretos | Verificar credenciais no App Store |
| 401 | `ACCESS_DENIED` | redirect_uri invalida ou nao propagada | Aguardar 1h, verificar URL |
| 401 | `EXPIRED_CODE_GRANT` | Code expirado (> 60 min ou ja usado) | Gerar novo authorization code |
| 401 | - | access_token expirado (> 24h) | Usar refresh_token |
| 401 | `INVALID_REFRESH_TOKEN` | refresh_token invalido ou revogado | Reiniciar fluxo OAuth2 completo |
| 401 | `invalid_request` | access_token ausente no header | Incluir Authorization: Bearer |

## 7.2 Erros Comuns (Todos os Endpoints)

| HTTP | Descricao | Causa |
|------|-----------|-------|
| 400 | Bad Request | Payload mal formado, campos invalidos |
| 401 | Unauthorized | Token ausente, invalido ou expirado |
| 403 | Forbidden | Permissoes insuficientes (visibilidade do token) |
| 404 | Not Found | Recurso nao encontrado (contato, deal, etc) |
| 414 | URI Too Long | URL excede 8 KB |
| 422 | Unprocessable Entity | Validacao falhou (campo obrigatorio, formato errado) |
| 429 | Too Many Requests | Rate limit excedido |
| 500 | Internal Server Error | Erro no servidor RD |

## 7.3 Response 429 (Rate Limit)

```json
{
  "error": "Too Many Requests",
  "max_requests": 120,
  "current_usage": 121,
  "remaining_time_ms": 45000
}
```

Campos retornados:
- `max_requests`: Limite maximo permitido
- `current_usage`: Uso atual
- `remaining_time_ms`: Tempo restante em ms ate reset

## 7.4 Erros Especificos de Contatos

| error_type | Descricao |
|------------|-----------|
| `EMAIL_ALREADY_IN_USE` | POST /contacts quando email ja existe (use PATCH) |
| `RESOURCE_NOT_FOUND` | Tag em contato inexistente |
| `CONFLICTING_FIELD` | Campo nao reconhecido no payload |

---

# APENDICE A: Resumo de URLs Base

| Produto | URL Base | Autenticacao |
|---------|----------|--------------|
| RD Marketing | `https://api.rd.services` | OAuth2 Bearer Token ou API Key (?api_key=) |
| RD CRM v1 | `https://crm.rdstation.com/api/v1` | Token de instancia (?token= ou Bearer) |
| RD CRM v2 | `https://api.rd.services/crm/v2` | OAuth2 Bearer Token (2h expiry) |
| RD Conversas | `https://api.tallos.com.br` | JWT Bearer Token |

# APENDICE B: Versoes de API

| Produto | Versao Atual | Legado |
|---------|-------------|--------|
| Marketing | v2 (api.rd.services) | v1 DEPRECATED |
| CRM | v1 (crm.rdstation.com) + v2 (api.rd.services) | Ambos ativos |
| Conversas | v2 (base) + v3 (templates) + v4 (reports) | v1 deprecated em partes |

# APENDICE C: Diagrama de Integracao

```
                    ┌─────────────────────┐
                    │   RD MARKETING      │
                    │  api.rd.services    │
                    │                     │
                    │ /platform/contacts  │
                    │ /platform/events    │
                    │ /platform/conversions│
                    │ /platform/analytics │
                    │ /integrations/webhooks│
                    └────────┬────────────┘
                             │
                    Integracao Nativa
                    (marcacao oportunidade)
                             │
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
┌─────────────────┐ ┌────────────────┐ ┌────────────────┐
│   RD CRM v1     │ │  RD CRM v2     │ │ RD CONVERSAS   │
│ crm.rdstation   │ │ api.rd.services│ │ api.tallos.com │
│ .com/api/v1     │ │ /crm/v2        │ │ .br            │
│                 │ │                │ │                │
│ /deals          │ │ /deals         │ │ /v2/customers  │
│ /contacts       │ │ /pipelines     │ │ /v2/messages   │
│ /deal_pipelines │ │ /users         │ │ /v3/templates  │
│ /deal_stages    │ │ /teams         │ │ /v4/reports    │
│ /webhooks       │ │                │ │ /v2/workflows  │
└─────────────────┘ └────────────────┘ └────────────────┘
```

---

**FIM DO DOCUMENTO**

Sources:
- [Autenticacao OAuth2](https://developers.rdstation.com/reference/autentica%C3%A7%C3%A3o)
- [Obter Tokens](https://developers.rdstation.com/reference/obter-tokens-acesso)
- [Atualizar Access Token](https://developers.rdstation.com/reference/atualizar-access-token)
- [FAQ Autenticacao](https://developers.rdstation.com/reference/faq-autenticacao)
- [Erros de Autenticacao](https://developers.rdstation.com/reference/erros-autenticacao)
- [Conversao via API Key](https://developers.rdstation.com/reference/conversao)
- [Evento de Conversao Padrao](https://developers.rdstation.com/reference/evento-de-conversao-padrao)
- [Batch de Eventos](https://developers.rdstation.com/reference/batch-eventos)
- [Contatos](https://developers.rdstation.com/reference/contatos?lng=pt-BR)
- [Consultar Contato](https://developers.rdstation.com/reference/get_platform-contacts-identifier-value)
- [Atualizar Contato](https://developers.rdstation.com/reference/patch_platform-contacts-identifier-value)
- [Campos Personalizados](https://developers.rdstation.com/reference/get_platform-contacts-fields)
- [Eventos do Contato](https://developers.rdstation.com/reference/get_platform-contacts-uuid-events)
- [Segmentacoes](https://developers.rdstation.com/reference/segmenta%C3%A7%C3%B5es)
- [Analytics Funil](https://developers.rdstation.com/reference/get_platform-analytics-funnel)
- [Analytics Conversoes](https://developers.rdstation.com/reference/get_platform-analytics-conversions)
- [Analytics Emails](https://developers.rdstation.com/reference/get_platform-analytics-emails)
- [Rate Limits](https://developers.rdstation.com/reference/limite-de-requisicoes-da-api)
- [Webhooks Marketing](https://developers.rdstation.com/reference/webhooks)
- [Webhooks MKT Payload](https://developers.rdstation.com/reference/webhooks-payload-mkt)
- [Criar Webhook Marketing](https://developers.rdstation.com/reference/post_integrations-webhooks)
- [CRM v1 Token](https://developers.rdstation.com/reference/crm-v1-token)
- [CRM v1 Intro](https://developers.rdstation.com/reference/crm-v1-introducao-e-requisitos)
- [CRM v1 Listar Deals](https://developers.rdstation.com/reference/crm-v1-list-deals)
- [CRM v1 Criar Deal](https://developers.rdstation.com/reference/crm-v1-create-deal)
- [CRM v1 Atualizar Deal](https://developers.rdstation.com/reference/crm-v1-update-deal)
- [CRM v1 Contatos do Deal](https://developers.rdstation.com/reference/crm-v1-list-contacts-from-deal)
- [CRM v1 Criar Contato](https://developers.rdstation.com/reference/crm-v1-create-contact)
- [CRM v1 Listar Pipelines](https://developers.rdstation.com/reference/crm-v1-list-pipelines)
- [CRM v1 Webhooks](https://developers.rdstation.com/reference/crm-v1-webhooks)
- [CRM v1 Criar Webhook](https://developers.rdstation.com/reference/crm-v1-create-webhook)
- [CRM v2 Autenticacao](https://developers.rdstation.com/reference/crm-v2-authentication)
- [CRM v2 Criar Deal](https://developers.rdstation.com/reference/crm-v2-create-deal)
- [CRM v2 Listar Pipelines](https://developers.rdstation.com/reference/crm-v2-list-pipelines)
- [Conversas v2 Introducao](https://developers.rdstation.com/reference/conversas-v2-introduction)
- [Conversas v2 Contatos](https://developers.rdstation.com/reference/conversas-v2-list-contacts)
- [Conversas v2 Historico](https://developers.rdstation.com/reference/conversas-v2-list-messages-history)
- [Conversas v2 Workflows](https://developers.rdstation.com/reference/conversas-v2-list-workflows)
- [Conversas v2 Forward](https://developers.rdstation.com/reference/conversas-v2-forward-to-customer)
- [Conversas v4 Reports](https://developers.rdstation.com/reference/conversas-v2-list-reports)
- [Eventos de Chat](https://developers.rdstation.com/reference/eventos-chat)
- [LLMs.txt (Indice Completo)](https://developers.rdstation.com/llms.txt)
- [Integrar Marketing e CRM](https://ajuda.rdstation.com/s/article/Como-integrar-o-RD-Station-Marketing-e-RD-Station-CRM?language=pt_BR)
