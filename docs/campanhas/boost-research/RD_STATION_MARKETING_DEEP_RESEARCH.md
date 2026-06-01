# RD Station Marketing - Deep Research Completo

> Pesquisa realizada em 01/jun/2026. Fontes: developers.rdstation.com, ajuda.rdstation.com, blog.rdstation.com e documentacao oficial.

---

## INDICE

1. [Automacao](#1-automacao)
2. [Lead Scoring](#2-lead-scoring)
3. [Segmentacao](#3-segmentacao)
4. [Campos Personalizados](#4-campos-personalizados)
5. [Email Marketing](#5-email-marketing)
6. [Landing Pages](#6-landing-pages)
7. [Formularios](#7-formularios)
8. [Webhooks](#8-webhooks)
9. [Integracoes](#9-integracoes)
10. [Lead Tracking](#10-lead-tracking)
11. [Conversoes](#11-conversoes)
12. [Pop-ups e Botoes](#12-pop-ups-e-botoes)
13. [Tags](#13-tags)
14. [Base de Leads](#14-base-de-leads)
15. [Relatorios e Analises](#15-relatorios-e-analises)

---

## 1. AUTOMACAO

### Como Funciona

A Automacao de Marketing do RD Station cria experiencias personalizadas em escala por meio de acoes automatizadas de acordo com interacoes e informacoes fornecidas pelos leads. Os Fluxos de Automacao usam gatilhos, condicionais e eventos relacionados ao Lead para direcionar o caminho de emails e acoes.

### Gatilhos de Entrada (Triggers)

Os gatilhos determinam QUANDO um lead entra no fluxo:

| Gatilho | Descricao |
|---------|-----------|
| Conversao em Landing Page | Lead converteu em uma LP especifica |
| Conversao em Formulario | Lead preencheu um formulario embedded |
| Conversao em Pop-up | Lead converteu via pop-up |
| Entrou em Segmentacao | Lead passou a fazer parte de uma segmentacao dinamica |
| Conversao via API | Lead registrado via evento de conversao da API |
| Pontuacao de Lead Scoring | Lead atingiu determinada pontuacao |
| Visita em pagina especifica | Lead visitou uma URL rastreada (Lead Tracking) |

### Condicoes (Se/Senao)

Permitem ramificar o fluxo com base em criterios:

- **Dados do Lead**: campo igual/diferente de valor (ex: estado = SP)
- **Tags**: lead possui ou nao determinada tag
- **Lead Scoring**: nota de perfil (A/B/C/D) ou pontuacao de interesse
- **Estagio do Funil**: Lead, Lead Qualificado, Cliente
- **Conversao anterior**: lead ja converteu em determinado evento
- **Email anterior**: lead abriu/clicou email anterior do fluxo

A ramificacao cria dois caminhos (sim/nao). E possivel UNIR caminhos depois da ramificacao para leads de pontos diferentes seguirem o mesmo caminho.

### Acoes Disponiveis nos Fluxos

| Acao | Descricao |
|------|-----------|
| **Enviar Email** | Disparo automatico de email personalizado |
| **Adicionar Tag** | Adiciona uma ou mais tags ao lead |
| **Remover Tag** | Remove tags especificas do lead |
| **Marcar como Oportunidade** | Marca o lead como oportunidade de venda |
| **Alterar Estagio do Funil** | Muda para Lead, Lead Qualificado ou Cliente |
| **Marcar Dono do Lead** | Atribui um usuario como responsavel pelo lead |
| **Notificar Dono do Lead** | Envia email de notificacao ao dono do lead |
| **Enviar para Integracao/URL** | Envia dados do lead para sistema externo (webhook) |
| **Enviar para CRM** | Cria/atualiza negociacao no RD Station CRM |
| **Alterar Campo** | Modifica valor de campo padrao ou personalizado |
| **Enviar WhatsApp** | Envia mensagem via RD Conversas |
| **Seguir no Twitter** | Conta da empresa segue o lead no Twitter |
| **Teste A/B** | Divide leads em caminhos para testar variacoes |

### Acao de Espera (Delay)

- Configura tempo de espera entre acoes
- Pode ser em minutos, horas ou dias
- Importante para nao sobrecarregar o lead com comunicacoes simultaneas
- O calculo de tempo deve considerar dias uteis vs corridos

### Teste A/B em Fluxos

- Divide leads em 2+ caminhos
- Cada caminho pode ter acoes diferentes (ex: emails com assuntos distintos)
- Permite otimizar com base nos resultados

### Modelos de Automacao

RD Station oferece modelos pre-prontos para cenarios comuns:
- Boas-vindas apos conversao
- Nutrição de leads
- Reengajamento de leads inativos
- Qualificacao automatica
- Fluxo pos-venda

### Estatisticas do Fluxo

Metricas disponiveis por fluxo:
- Leads que entraram no fluxo
- Leads que receberam cada acao
- Leads que encerraram o fluxo
- Taxas de abertura/clique dos emails do fluxo

### Limitacoes

- Lead so entra no mesmo fluxo UMA vez (a menos que seja removido e re-entre)
- Nao ha acao nativa de "remover de outro fluxo"
- Emails de automacao nao permitem teste A/B de assunto (apenas fluxos separados)
- Acoes condicionais dependem de campos preenchidos — campos vazios podem causar comportamento inesperado

### Boas Praticas

1. Definir objetivo claro para cada fluxo
2. Usar delays entre acoes (minimo 1-2 dias entre emails)
3. Segmentar bem o gatilho de entrada para evitar leads irrelevantes
4. Monitorar estatisticas e ajustar
5. Limitar quantidade de emails por fluxo (3-5 ideal)
6. Usar tags para controlar quem ja passou pelo fluxo

---

## 2. LEAD SCORING

### Como Funciona

O Lead Scoring atribui automaticamente um valor numerico (score) aos leads avaliando dois aspectos independentes: **Perfil** e **Interesse**. Disponivel a partir do **Plano Pro**.

### Perfil (Nota Alfabetica: A, B, C, D)

Representa as caracteristicas demograficas do lead:

| Nota | Significado |
|------|-------------|
| **A** | Perfil ideal — mais proximo do cliente ideal (ICP) |
| **B** | Bom perfil — proximo do ideal |
| **C** | Perfil medio — alguma aderencia |
| **D** | Perfil ruim — longe do alvo |

**Como funciona o calculo:**
- Voce define quais CAMPOS serao usados (ex: cargo, estado, setor, faixa de investimento)
- Cada campo recebe um PESO (importancia relativa)
- Cada opcao de resposta dentro do campo recebe ESTRELAS (0 a 10)
- O sistema calcula o peso real proporcionalmente ao total de todos os campos

**Exemplo de calculo:**
- Campo "Cargo" com peso 2, campo "Estado" com peso 1
- Peso real Cargo = 67%, peso real Estado = 33%
- Lead com Cargo = "Presidente" (10 estrelas) e Estado = "RJ" (5 estrelas)
- Score = (67 * 10) + (33 * 5) = 670 + 165 = 835

Os campos usados para perfil devem ser campos personalizados identicos aos usados nos formularios.

### Interesse (Nota Numerica: 0 ate infinito)

Mede o engajamento e interacoes do lead com suas campanhas:

**Acoes que geram pontos de interesse:**
- Conversao em Landing Page
- Conversao em Formulario
- Conversao em Pop-up
- Clique em email
- Completar fluxo de automacao
- Visita a paginas especificas (Lead Tracking)

**Regras importantes:**
- Pontos sao CUMULATIVOS (cada acao soma)
- Se o lead atender o MESMO criterio mais de uma vez, pontua APENAS na primeira vez
- Cada nova conversao, clique ou fluxo completado gera novos pontos
- Voce define quantos pontos cada acao vale

### Qualificacao Automatica (MQL)

- Leads com Perfil A, B ou C sao considerados MQLs
- Leads com Perfil D NAO sao categorizados como MQLs
- E possivel criar automacoes baseadas na pontuacao (ex: se interesse >= 100 E perfil = A, marcar como oportunidade)

### Como Configurar

1. Menu superior > Marketing > Lead Scoring
2. Aba **Perfil**: selecionar campos, definir pesos, atribuir estrelas para cada opcao
3. Aba **Interesse**: selecionar acoes (LPs, emails, fluxos), definir pontos para cada
4. Salvar e ativar

### Limitacoes

- Disponivel apenas no Plano Pro e superiores
- Perfil usa apenas campos que existem como campos personalizados no RD Station
- Nao e possivel usar campos de conversao diretamente (precisa ser campo do lead)
- Alteracoes nas regras NAO recalculam scores retroativamente de forma imediata
- Maximo de campos para perfil nao e documentado oficialmente, mas recomenda-se ate 5-7

### Integracao com API

Via API, os dados de scoring estao disponiveis no objeto `funnel`:
```json
"funnel": {
  "name": "default",
  "lifecycle_stage": "Lead",
  "opportunity": false,
  "contact_owner_email": "owner@example.org",
  "interest": 20,
  "fit": 0,
  "origin": "Busca Paga | Google"
}
```
- `interest`: pontuacao de interesse (numerico)
- `fit`: pontuacao de perfil (numerico interno)

---

## 3. SEGMENTACAO

### Como Funciona

Segmentacao e um filtro que seleciona leads da base com informacoes especificas, criando uma **lista dinamica**. Leads entram e saem automaticamente conforme atendem ou deixam de atender os criterios.

### Tipos de Segmentacao

#### Por Dados do Lead
- Qualquer campo padrao ou personalizado
- Operadores: igual, diferente, contem, nao contem, esta vazio, nao esta vazio
- Filtros de data: "no dia", "entre datas", "antes ou igual a", "apos ou igual a"

#### Por Conversao
- Identificador de conversao (conversion_identifier)
- Canal de origem (Busca Organica, Midia Paga, etc.)
- Recurso usado (Landing Page, Formulario, API)
- UTM Source e UTM Medium da conversao
- "Qualquer conversao" ou conversao especifica

#### Por Lead Scoring
- Nota de perfil (A, B, C, D)
- Pontuacao de interesse (maior que, menor que, igual a)

#### Por Lead Tracking
- Visitou pagina especifica
- Visitou "qualquer pagina"
- Numero de visitas

#### Por Funil
- Estagio atual: Lead, Lead Qualificado, Cliente
- Marcado como Oportunidade (sim/nao)

#### Por Email Marketing
- Recebeu email especifico
- Abriu email
- Clicou em email
- Converteu a partir de email

#### Por Fluxo de Automacao
- Entrou em fluxo especifico
- Completou fluxo
- Esta em determinada etapa do fluxo

#### Por Tags
- Possui tag especifica
- Nao possui tag

#### Por Ecommerce (se habilitado)
- Eventos de carrinho, pedido, pagamento

### Combinacao de Filtros

- Filtros dentro do MESMO grupo = logica AND (E)
- Grupos DIFERENTES = logica OR (OU)
- Permite criar segmentacoes complexas combinando multiplos criterios

### Segmentacao via API

**Endpoint:** `GET https://api.rd.services/platform/segmentations`

Permite:
- Listar todas as segmentacoes existentes
- Navegar pelos leads de uma segmentacao
- Consultar e extrair contatos

### Limitacoes

- Segmentacoes sao sempre dinamicas (nao ha "lista estatica" nativa — usar tags para isso)
- Segmentacoes grandes (>100k leads) podem demorar para processar
- Limite de segmentacoes varia por plano
- Filtros de Lead Tracking dependem do codigo de monitoramento instalado

### Como Criar

1. Menu > Relacionar > Segmentacao de Leads
2. Canto superior direito > Criar Segmentacao
3. Dar nome
4. Adicionar filtros/criterios
5. Salvar

---

## 4. CAMPOS PERSONALIZADOS

### Como Funciona

Campos personalizados permitem capturar informacoes alem dos campos padrao. Sao identificados pelo prefixo obrigatorio `cf_` no api_identifier.

### Campos Padrao do RD Station (Built-in)

| Campo | api_identifier | Tipo |
|-------|---------------|------|
| Email | email | STRING |
| Nome | name | STRING |
| Cargo | job_title | STRING |
| Estado | state | STRING |
| Cidade | city | STRING |
| Pais | country | STRING |
| Telefone Fixo | personal_phone | STRING |
| Celular | mobile_phone | STRING |
| Twitter | twitter | STRING |
| Facebook | facebook | STRING |
| LinkedIn | linkedin | STRING |
| Website | website | STRING |
| Empresa | company_name | STRING |
| Site da Empresa | company_site | STRING |
| Endereco da Empresa | company_address | STRING |
| Tags | tags | ARRAY |

Campos padrao NAO podem ser excluidos ou modificados. Possuem `custom_field: false`.

### Tipos de Dados (data_type)

| Tipo | Descricao |
|------|-----------|
| `STRING` | Texto simples |
| `INTEGER` | Numero inteiro |
| `BOOLEAN` | Verdadeiro/Falso |
| `STRING[]` | Array de strings (multipla escolha) |

### Tipos de Apresentacao (presentation_type)

| Tipo | Uso |
|------|-----|
| `TEXT_INPUT` | Campo de texto simples |
| `EMAIL_INPUT` | Campo de email |
| `COMBO_BOX` | Dropdown/Select |
| `URL_INPUT` | Campo de URL |
| `TEXT_AREA` | Area de texto longa |
| `RADIO_BUTTON` | Botoes de radio (escolha unica) |
| `PHONE_INPUT` | Campo de telefone |
| `NUMBER_INPUT` | Campo numerico |
| `CHECK_BOX` | Checkbox (sim/nao) |
| `MULTIPLE_CHOICE` | Multipla escolha |

### Criar Campo Personalizado via API

**Endpoint:** `POST https://api.rd.services/platform/contacts/fields`

**Request Body:**
```json
{
  "api_identifier": "cf_patrimonio_investido",
  "data_type": "STRING",
  "name": {"pt-BR": "Patrimonio Investido"},
  "label": {"pt-BR": "Qual seu patrimonio investido?"},
  "presentation_type": "COMBO_BOX",
  "validation_rules": {
    "valid_options": [
      {"value": "ate_50k", "label": {"pt-BR": "Ate R$50 mil"}},
      {"value": "50k_200k", "label": {"pt-BR": "R$50 mil a R$200 mil"}},
      {"value": "acima_200k", "label": {"pt-BR": "Acima de R$200 mil"}}
    ]
  }
}
```

**Regras do api_identifier:**
- DEVE comecar com `cf_`
- Apenas letras minusculas, numeros e underscores
- Maximo 64 caracteres
- NAO pode ser alterado apos criacao
- NAO pode usar palavras reservadas (~150 termos bloqueados incluindo: `_is`, `account_id`, `address`, `email`, `name`, `cf_order_id`, `cf_order_total_items`, `cf_cart_id`, `cf_product_sku`, etc.)

**Response (201 Created):** Retorna o campo criado com UUID.

**Erros comuns (400):**
- Nome ou label em branco
- api_identifier sem prefixo cf_
- Nome ja em uso (TAKEN)
- Tipo de dado invalido para presentation_type
- Opcoes repetidas em validation_rules

### Listar Campos via API

**Endpoint:** `GET https://api.rd.services/platform/contacts/fields`

Retorna todos os campos (padrao + personalizados) com seus atributos.

### Uso em Formularios e Automacoes

- Campos personalizados podem ser usados em Landing Pages, Formularios, Pop-ups
- Podem ser usados como criterio em Segmentacoes
- Podem ser usados em regras de Lead Scoring (perfil)
- Podem ser alterados via acao de automacao "Alterar Campo"
- Podem ser usados como variaveis em emails

### Limitacoes

- api_identifier e imutavel apos criacao
- Campos padrao nao podem ser deletados
- Limite de campos personalizados varia por plano (nao documentado publicamente)
- Campos com opcoes (COMBO_BOX, RADIO_BUTTON) requerem que as opcoes sejam pre-definidas

---

## 5. EMAIL MARKETING

### Variaveis Disponiveis

**Variaveis padrao:**

| Variavel | Campo |
|----------|-------|
| `*\|NOME\|*` | Nome completo do lead |
| `*\|PRIMEIRO_NOME\|*` | Primeiro nome do lead |
| `*\|EMAIL\|*` | Email do lead |
| `*\|EMPRESA\|*` | Nome da empresa |

**Variaveis de campos personalizados:**
- QUALQUER campo personalizado pode ser usado como variavel
- Formato: nome do campo conforme cadastrado
- Permite personalizar com cargo, cidade, time de futebol, etc.

**Limitacoes de variaveis:**
- Maximo de **15 variaveis** no mesmo email
- Variaveis NAO funcionam em emails de agradecimento
- Se o campo estiver vazio no lead, a variavel fica em branco (ou exibe fallback se configurado)

### Criacao de Templates/Modelos

- Editor drag-and-drop nativo
- Templates pre-prontos disponiveis
- Possivel criar modelos customizados para reutilizacao
- Suporte a HTML customizado

### Teste A/B

- Disponivel para **assunto do email** (subject line)
- Divide a base em grupos de teste
- Envia variacoes do assunto para amostras
- Versao vencedora e enviada para o restante da base
- Metricas: taxa de abertura para determinar vencedor
- Teste A/B tambem possivel em fluxos de automacao (caminhos diferentes)

### Metricas de Email

| Metrica | Descricao |
|---------|-----------|
| Enviados | Total de emails disparados |
| Entregues | Emails que chegaram ao servidor destino |
| Aberturas | Emails abertos (tracking pixel) |
| Cliques | Cliques em links do email |
| Descadastros | Leads que pediram opt-out |
| Bounces | Emails rejeitados (soft/hard) |
| Spam | Marcados como spam |

### Deliverability / Entregabilidade

**Configuracao de dominio (obrigatoria):**
- Configurar subdominio de email (whitelabel)
- SPF: identifica servidores autorizados a enviar pelo dominio
- DKIM: assinatura digital que valida autenticidade
- DMARC: politica de autenticacao
- Entradas DNS devem ser configuradas no provedor de hospedagem

**Boas praticas:**
- NUNCA usar dominios gratuitos (@gmail.com, @hotmail.com) como remetente
- Dominio do remetente DEVE ser o mesmo do subdominio configurado
- Manter lista limpa (remover bounces e inativos)
- Evitar palavras de spam no assunto
- Manter proporcao texto/imagem equilibrada
- Configurar verificacao de email

**Regras de envio:**
- E-mails sao enviados apenas para leads com base legal adequada (LGPD)
- Leads marcados como "nao disponivel para mailing" nao recebem

### Regras Importantes (do Memory do Projeto)

- NUNCA usar base64 para imagens em emails RD Station
- Variavel de primeiro nome: `*|PRIMEIRO_NOME|*`
- Hospedar assets no GitHub Pages (nao usar raw do GitHub)
- Usar inline CSS
- Usar tabelas para layout (compatibilidade com clientes de email)

---

## 6. LANDING PAGES

### Como Funciona

Landing Pages sao paginas de conversao hospedadas no RD Station para captura de leads. Elementos principais: header, informacao da oferta, formulario, botao CTA, footer.

### Criacao

- Editor visual drag-and-drop
- Templates pre-prontos por categoria
- Edicao avancada com CSS/HTML customizado
- URL personalizada (subdominio da conta)

### Formulario da Landing Page

- Campos padrao e personalizados
- Campos obrigatorios/opcionais configuráveis
- Adicionar, remover e reposicionar campos
- Campo de verificacao (checkbox de consentimento)
- Campo em formato de data

### Formulario Inteligente (Progressive Profiling)

- Exibe campos DIFERENTES para leads que ja converteram antes
- Evita pedir informacoes ja conhecidas
- Permite coletar dados progressivamente a cada nova conversao
- Configuravel por Landing Page

### Logica de Campos

- Exibicao condicional de campos baseada em respostas anteriores
- Ex: se selecionou "Pessoa Juridica", exibir campo CNPJ

### Formulario em Duas Etapas

- Divide os campos em 2 steps
- Primeira etapa: campos essenciais (nome, email)
- Segunda etapa: campos complementares
- Melhora taxa de conversao

### Email Automatico de Agradecimento

- Configuravel por Landing Page
- Envia automaticamente apos conversao
- Pode conter link para download de material

### Edicao Avancada

- CSS customizado
- JavaScript customizado
- HTML direto
- Ocultar elementos da LP
- Direcionamento do CTA para o formulario

### Teste A/B em Landing Pages

- Criar variacoes da mesma LP
- Dividir trafego automaticamente
- Medir taxa de conversao de cada variacao

### Limitacoes

- Hospedadas no subdominio RD Station (nao em dominio customizado completo)
- Templates limitados ao editor nativo
- Edicao avancada requer conhecimento de CSS/HTML
- Nao suporta conteudo dinamico server-side

---

## 7. FORMULARIOS

### Tipos de Formulario

| Tipo | Descricao | Onde usar |
|------|-----------|-----------|
| **Formulario de Landing Page** | Integrado ao editor de LP | Dentro da Landing Page RD |
| **Formulario Embedded** | Codigo para incorporar em site externo | Qualquer pagina com HTML |
| **Pop-up** | Formulario exibido em pop-up | Paginas com codigo de monitoramento |
| **Botao WhatsApp** | Formulario vinculado a botao WA | Paginas com codigo de monitoramento |

### Formularios Embedded (para site externo)

**Criacao:**
1. Menu > Converter > Formularios
2. Criar novo formulario
3. Configurar campos
4. Gerar codigo de embed
5. Inserir codigo HTML no site

**Integracao via Codigo de Monitoramento:**
- Formularios existentes no site podem ser integrados ao RD Station
- Requer codigo de monitoramento instalado
- Captura dados do formulario e envia para base RD

### Campos Disponiveis

- Todos os campos padrao (nome, email, telefone, etc.)
- Todos os campos personalizados criados (cf_*)
- Campos de consentimento/verificacao (LGPD)
- Campos de data

### Campos Obrigatorios

- Email e SEMPRE obrigatorio (identificador unico do lead)
- Outros campos podem ser marcados como obrigatorios ou opcionais
- Campo de consentimento pode ser obrigatorio

### Hidden Fields (Campos Ocultos)

- Nao ha suporte NATIVO a hidden fields no editor visual do RD Station
- Workaround: usar edicao avancada (CSS) para ocultar campos visualmente
- Via API: enviar campos extras junto com a conversao sem exibi-los no formulario
- Campos de UTM podem ser passados via integracao JavaScript

### Formulario Inteligente

- Reconhece leads que ja converteram (via cookie)
- Substitui campos ja preenchidos por novos campos
- Permite progressive profiling automatico
- Configuravel individualmente por formulario/LP

### Atualizacao Automatica

- Formularios RD Station podem ser atualizados automaticamente
- Novos campos personalizados ficam disponiveis nos formularios existentes

### Campos de Protecao de Dados

- Inclusao automatica de campos de consentimento LGPD
- Configuravel para todas as ferramentas de conversao publicadas

---

## 8. WEBHOOKS

### Como Funciona

Webhooks automatizam o envio de dados de contatos e atividades do RD Station Marketing/CRM para sistemas externos. Quando um evento configurado ocorre, RD Station envia uma requisicao HTTP POST com payload JSON padronizado.

### Eventos Disponiveis

**RD Station Marketing:**

| Evento | event_type | Quando dispara |
|--------|-----------|----------------|
| Conversao | `WEBHOOK.CONVERTED` | Lead converte em LP, Formulario, Pop-up ou integracao |
| Oportunidade | `WEBHOOK.MARKED_OPPORTUNITY` | Lead marcado como oportunidade (manual, automacao ou integracao) |

**RD Station CRM (Plano Basic/Pro):**

| Evento | event_type | Quando dispara |
|--------|-----------|----------------|
| Deal criado | `crm_deal_created` | Nova negociacao criada |
| Deal atualizado | `crm_deal_updated` | Negociacao modificada |
| Deal deletado | `crm_deal_deleted` | Negociacao removida |

### Payload Completo (Marketing - WEBHOOK.CONVERTED)

```json
{
  "event_type": "WEBHOOK.CONVERTED",
  "entity_type": "CONTACT",
  "event_identifier": "nome-da-conversao",
  "timestamp": "2026-06-01T14:09:02.724-03:00",
  "event_timestamp": "2026-06-01T14:07:04.254-03:00",
  "contact": {
    "uuid": "c2f3d2b3-7250-4d27-97f4-eef38be32f7f",
    "email": "lead@exemplo.com",
    "name": "Nome do Lead",
    "job_title": "Cargo",
    "bio": "Anotacoes",
    "website": "http://site.com",
    "personal_phone": "48 30252598",
    "mobile_phone": "48 99999999",
    "city": "Florianopolis",
    "facebook": "perfil-facebook",
    "linkedin": "perfil-linkedin",
    "twitter": "perfil-twitter",
    "tags": ["tag1", "tag2"],
    "cf_campo_personalizado": ["Opcao1", "Opcao2"],
    "legal_bases": [
      {
        "category": "communications",
        "type": "consent",
        "status": "granted"
      }
    ],
    "company": {
      "name": "Nome da Empresa"
    },
    "funnel": {
      "name": "default",
      "lifecycle_stage": "Lead",
      "opportunity": false,
      "contact_owner_email": "owner@example.org",
      "interest": 20,
      "fit": 0,
      "origin": "Busca Paga | Google"
    }
  }
}
```

### Campos do Payload

| Campo | Tipo | Descricao |
|-------|------|-----------|
| event_type | String | WEBHOOK.CONVERTED ou WEBHOOK.MARKED_OPPORTUNITY |
| entity_type | String | Sempre "CONTACT" |
| event_identifier | String | Nome da conversao; "default" para oportunidade |
| timestamp | DateTime | Quando o webhook foi disparado |
| event_timestamp | DateTime | Quando o evento real ocorreu |
| uuid | String | ID unico do contato |
| tags | Array | Tags do contato |
| cf_* | Variavel | Campos personalizados (prefixo cf_) |
| legal_bases | Array | Bases legais LGPD |
| funnel.interest | Number | Pontuacao de interesse (Lead Scoring) |
| funnel.fit | Number | Pontuacao de perfil |
| funnel.origin | String | Origem do lead (utm_medium + utm_source) |
| funnel.lifecycle_stage | String | Lead, Lead Qualificado, Cliente |
| funnel.opportunity | Boolean | Se e oportunidade |

### Criar Webhook via API

**Endpoint:** `POST https://api.rd.services/integrations/webhooks`

**Request:**
```json
{
  "event_type": "WEBHOOK.CONVERTED",
  "entity_type": "CONTACT",
  "event_identifiers": ["nome-conversao-especifica"],
  "url": "https://seu-sistema.com/webhook",
  "http_method": "POST",
  "include_relations": ["COMPANY", "CONTACT_FUNNEL"]
}
```

**Parametros:**

| Parametro | Obrigatorio | Descricao |
|-----------|-------------|-----------|
| event_type | Sim | Tipo de evento |
| entity_type | Sim | "CONTACT" |
| url | Sim | URL de destino |
| http_method | Sim | "POST" (unico suportado) |
| event_identifiers | Nao | Filtrar por conversoes especificas (so para WEBHOOK.CONVERTED) |
| include_relations | Nao | Dados adicionais: "COMPANY", "CONTACT_FUNNEL" |

### Requisitos Tecnicos

- URL destino deve aceitar POST com JSON
- Certificado SSL/TLS valido
- Deve responder com status 2xx
- Na criacao, RD Station valida a URL enviando request de teste
- Se validacao falhar, retorna 401

### Autenticacao (CRM)

Webhooks do CRM suportam header customizado:
- `auth_header`: nome do header
- `auth_key`: valor do header

### Gerenciamento de Webhooks

| Operacao | Endpoint |
|----------|----------|
| Listar todos | `GET /integrations/webhooks` |
| Criar | `POST /integrations/webhooks` |
| Consultar um | `GET /integrations/webhooks/{uuid}` |
| Atualizar | `PUT /integrations/webhooks/{uuid}` |
| Deletar | `DELETE /integrations/webhooks/{uuid}` |

### Limitacoes Importantes

- Payload e IMUTAVEL — nao e possivel customizar quais campos sao enviados
- Campos personalizados vazios NAO sao incluidos no payload
- Importacoes manuais e atualizacoes em massa NAO disparam webhooks
- Um mesmo lead convertendo multiplas vezes dispara multiplos webhooks
- URLs duplicadas para o mesmo event_type nao sao permitidas
- Nao ha retry automatico documentado para falhas

---

## 9. INTEGRACOES

### Facebook Lead Ads

**Como funciona:**
- Integracao nativa entre Meta Lead Ads e RD Station Marketing
- Leads gerados em formularios do Facebook/Instagram sao enviados automaticamente para a base do RD Station

**Requisitos:**
- Conta de Administrador da pagina do Facebook
- Formulario do Lead Ad DEVE conter campo "email" ou "work_email"
- Campos do formulario devem ter IDs identicos aos campos no RD Station

**Configuracao:**
1. RD Station > Integracoes > Facebook Lead Ads
2. Login na conta do Facebook Business
3. Selecionar pagina e formularios para sincronizar
4. Mapear campos: IDs do formulario Facebook = nomes dos campos RD Station

**Mapeamento de campos:**
- Em Opcoes Avancadas do formulario Facebook, marcar "Editar IDs de campo"
- Os IDs devem ser EXATAMENTE iguais aos api_identifiers do RD Station
- Campos personalizados: usar o cf_nome_do_campo

**IMPORTANTE (do Memory do projeto):**
- Lead Form Meta envia VALUES internos (ex: "50k_200k"), RD espera texto exato
- Integracao nativa NAO traduz labels → os Labels no Meta devem ser IDENTICOS as opcoes do campo RD
- Macros {{campaign.name}} sao gravadas como texto literal — SEMPRE usar UTM fixos
- Lead Form NUNCA tem cf_utm_* (campos de UTM personalizados)

### Facebook CAPI (Conversions API)

- Configuracao de envio de eventos do Facebook Ads
- Permite enviar eventos de conversao do RD Station para o Meta Ads
- Usado para otimizacao de campanhas e atribuicao

### LinkedIn Lead Ads

- Integracao disponivel via menu Integracoes
- Leads de formularios LinkedIn Lead Gen Forms sincronizados com base RD
- Mapeamento de campos similar ao Facebook Lead Ads

### Google Ads

- Auto-tagging do Google Ads e reconhecido pelo RD Station
- Leads vindos de Google Ads sao classificados como "Midia Paga" automaticamente
- Nao requer UTMs para medicao basica (mas UTMs sao recomendados para detalhamento)
- Integracao com Google Analytics para dados de comportamento

### WhatsApp

**Botao de WhatsApp:**
- Pop-up com botao flutuante no site
- Formulario de captura antes de redirecionar para WhatsApp
- Dados salvos na base de leads
- Mensagem padrao configuravel

**RD Station Conversas:**
- Produto separado para gestao de conversas via WhatsApp
- Integracao com RD Station Marketing e CRM
- Fluxos de atendimento automatizados
- Bot de atendimento

**Integracao WhatsApp Web:**
- Conexao direta com WhatsApp Web
- Envio de mensagens a partir do CRM

### Google Forms

- Integracao disponivel para enviar respostas do Google Forms para RD Station

### Pluga (Intermediario)

- Plataforma intermediaria para integracoes
- Conecta Facebook Lead Ads, CRM e outros sistemas
- Alternativa quando integracao nativa nao atende

---

## 10. LEAD TRACKING

### Como Funciona

O Lead Tracking monitora as paginas visitadas por leads da sua base. Quando um lead identificado visita uma pagina com o codigo de monitoramento, todas as URLs sao registradas na timeline do lead.

### Ativacao

1. Instalar o Codigo de Monitoramento do RD Station em todas as paginas do site/blog
2. O Lead Tracking e ativado automaticamente em todas as paginas com o codigo

### Cookies Utilizados

| Cookie | Funcao |
|--------|--------|
| `_rdtrk` | Identificacao do visitante/lead (tracking ID) |
| `__trf.src` | Origem do trafego (codificado em base64) |

**Como funciona a identificacao:**
1. Visitante acessa pagina com codigo de monitoramento
2. Cookie `_rdtrk` e criado no navegador
3. Visitante converte em formulario/LP (fornece email)
4. RD Station associa o cookie ao email do lead
5. A partir dai, TODAS as visitas (inclusive anteriores a conversao) sao vinculadas ao lead

### Dados Rastreados

- URLs visitadas
- Data/hora de cada visita
- Origem do trafego (UTMs, referrer)
- Paginas mais visitadas

### Uso em Segmentacao

Possivel segmentar por:
- Visitou pagina especifica (URL)
- Visitou qualquer pagina
- Numero de paginas visitadas
- Data da visita

### Uso em Automacao

- Gatilho: lead visitou pagina especifica
- Permite criar fluxos baseados em comportamento de navegacao

### Integracao com API

Parametros relacionados ao tracking no evento de conversao:
- `client_tracking_id`: valor do cookie `_rdtrk`
- `traffic_source`: valor do cookie `__trf.src` ou UTM source
- `traffic_medium`: UTM medium
- `traffic_campaign`: UTM campaign
- `traffic_value`: UTM term

### Limitacoes

- Depende do codigo de monitoramento instalado em TODAS as paginas
- Cookies podem ser bloqueados por ad blockers ou configuracoes de privacidade
- Visitantes anonimos so sao identificados APOS a primeira conversao
- Visitas anteriores a conversao sao retroativamente associadas SE o cookie existir
- Paginas sem codigo de monitoramento nao sao rastreadas
- LGPD: necessario consentimento para uso de cookies

---

## 11. CONVERSOES

### O que e uma Conversao

Uma conversao no RD Station e qualquer evento em que um visitante fornece suas informacoes e se torna (ou atualiza dados como) um lead. Cada conversao e identificada por um `conversion_identifier` unico.

### Tipos de Conversao

| Tipo | conversion_identifier |
|------|----------------------|
| Landing Page | Nome/slug da LP |
| Formulario | Nome do formulario |
| Pop-up | Nome do pop-up |
| API | Valor enviado no campo conversion_identifier |
| Importacao | Nome da importacao |
| Facebook Lead Ads | Nome do formulario do Lead Ad |

### Diferenca: Dados de Conversao vs Dados do Lead

- **Dados de conversao**: informacoes registradas no MOMENTO da conversao (podem ser diferentes a cada conversao)
- **Dados do lead**: informacoes atuais/consolidadas do lead (ultima versao)

### Evento de Conversao via API (OAuth2)

**Endpoint:** `POST https://api.rd.services/platform/events?event_type=conversion`

```json
{
  "event_type": "CONVERSION",
  "event_family": "CDP",
  "payload": {
    "conversion_identifier": "nome-da-conversao",
    "email": "lead@exemplo.com",
    "name": "Nome",
    "job_title": "Cargo",
    "state": "SP",
    "city": "Sao Paulo",
    "company_name": "Empresa",
    "tags": ["tag1"],
    "cf_patrimonio": "acima_200k",
    "traffic_source": "google",
    "traffic_medium": "cpc",
    "traffic_campaign": "campanha-teste",
    "legal_bases": [
      {"category": "communications", "type": "consent", "status": "granted"}
    ]
  }
}
```

**Response:** `{"event_uuid": "5408c5a3-4711-4f2e-8d0b-13407a3e30f3"}`

### Evento de Conversao via API Key (Simplificado)

**Endpoint:** `POST https://api.rd.services/platform/conversions?api_key=SUA_API_KEY`

Mesma estrutura de payload. Autenticacao via query parameter `api_key` em vez de Bearer token OAuth2.

### Campos Disponiveis no Payload de Conversao

**Obrigatorios:**
- `conversion_identifier` (string, max 255 chars)
- `email` (string, max 255 chars)

**Contato:**
- name, job_title, state, city, country
- personal_phone, mobile_phone
- twitter, facebook, linkedin, website

**Empresa:**
- company_name, company_site, company_address

**Tracking:**
- `client_tracking_id` — cookie _rdtrk
- `traffic_source` — UTM source ou cookie __trf.src
- `traffic_medium` — UTM medium
- `traffic_campaign` — UTM campaign
- `traffic_value` — UTM term

**Controle:**
- `available_for_mailing` — boolean
- `legal_bases` — array de objetos LGPD
- `tags` — array de strings
- `cf_*` — qualquer campo personalizado

### Batch de Eventos

**Endpoint:** `POST https://api.rd.services/platform/events/batch`

- Enviar multiplos eventos em uma unica requisicao
- Limite de body: **0.2 MB**
- Array de objetos evento
- Suporta TODOS os tipos de evento:
  - CONVERSION, OPPORTUNITY, SALE, OPPORTUNITY_LOST
  - CHAT_STARTED, CHAT_FINISHED, CALL_FINISHED
  - MEDIA_PLAYBACK_STARTED, MEDIA_PLAYBACK_STOPPED
  - ECOMMERCE_CHECKOUT_STARTED, ECOMMERCE_CART_ABANDONED
  - ECOMMERCE_ORDER_PLACED, ECOMMERCE_ORDER_PAID
  - ECOMMERCE_ORDER_FULFILLED, ECOMMERCE_SHIPMENT_DELIVERED
  - ECOMMERCE_ORDER_CANCELLED, ECOMMERCE_ORDER_REFUNDED

### Eventos de Qualificacao

| Evento | Descricao |
|--------|-----------|
| OPPORTUNITY | Marcar como oportunidade |
| OPPORTUNITY_LOST | Oportunidade perdida (com motivo) |
| SALE (OPPORTUNITY_WON) | Oportunidade ganha/venda |

**Restricoes:**
- NAO aceitam campos personalizados (cf_*)
- Requerem contato PRE-EXISTENTE na base
- Recomendado aguardar alguns minutos apos criar contato via conversao

### Consultar Eventos de um Contato

**Endpoint:** `GET https://api.rd.services/platform/contacts/{uuid}/events?event_type=CONVERSION`

**Paginacao:**
- `order`: created_at
- `direction`: asc ou desc
- `page`: numero da pagina
- Default: 10 conversoes por pagina

---

## 12. POP-UPS E BOTOES

### Tipos de Pop-up

| Tipo | Gatilho | Descricao |
|------|---------|-----------|
| **Rolagem (Scroll)** | Visitante desce 50% da pagina | Oferecer conteudo rico ou direcionar para pagina especifica |
| **Saida (Exit-intent)** | Visitante tenta sair da pagina | Recuperar visitante antes de abandonar |
| **Botao de WhatsApp** | Clique no botao flutuante | Contato via WhatsApp com formulario previo |

### Gatilhos Adicionais (encontrados em fontes externas)

- **Exitintent**: aparece quando usuario move cursor para fechar aba
- **Clique**: aparece apos acao de clique do visitante
- **Welcomemat**: pop-up fullscreen ao entrar na pagina

### Regras de Exibicao

**Paginas:**
- Exibir em TODAS as paginas do site
- Exibir apenas em paginas especificas (lista de URLs)
- NAO exibir em paginas especificas (lista de exclusao)

**Frequencia:**
- Configurar intervalo entre exibicoes (a cada X dias)
- Evitar mostrar repetidamente ao mesmo visitante

**Dispositivos:**
- Pop-ups de Rolagem e Botao WhatsApp: exibidos em mobile E desktop
- Pop-ups de Saida: apenas desktop (mobile nao tem "exit intent" confiavel)

### Formulario do Pop-up

- Mesmos campos disponiveis dos formularios normais
- Campos padrao e personalizados
- Conversion identifier unico por pop-up
- Dados salvos na base de leads como qualquer outra conversao

### Botao de WhatsApp

- Botao flutuante no site
- Mensagem padrao configuravel para iniciar conversa
- Formulario de captura (nome, email) ANTES de redirecionar para WA
- Dados do formulario salvos na base de leads
- Regras de exibicao por pagina

### Pop-ups via API

**Endpoint:** `GET /pop-ups`

Permite consultar:
- Identificador interno
- Conversion identifier
- Configuracoes de exibicao
- Status de publicacao

### Limitacoes

- Apenas 3 tipos principais (scroll, exit, whatsapp)
- Nao ha pop-up por tempo (ex: apos 30 segundos)
- Exit-intent nao funciona em mobile
- Design limitado ao editor nativo
- Um unico pop-up ativo por tipo por vez (verificar)
- Depende do codigo de monitoramento instalado

---

## 13. TAGS

### Como Funciona

Tags sao etiquetas/rotulos que podem ser associados aos leads para organizacao, segmentacao e automacao. Sao strings livres (texto).

### Formas de Adicionar Tags

| Metodo | Descricao |
|--------|-----------|
| Manual | Editar lead individualmente e adicionar tags |
| Importacao | Coluna "Tag" no arquivo CSV de importacao |
| Automacao | Acao "Adicionar Tag" em fluxo de automacao |
| API (PATCH) | Atualizar contato — tags SUBSTITUEM as existentes |
| API (POST tag) | Adicionar tag — tags sao ACUMULADAS |
| Conversao | Tags enviadas no payload de conversao |

### ATENCAO: Comportamento da API

| Endpoint | Comportamento das Tags |
|----------|----------------------|
| `POST /platform/contacts` (criar) | Tags sao ADICIONADAS |
| `PATCH /platform/contacts/{id}` (atualizar) | Tags SUBSTITUEM as existentes |
| `POST /platform/contacts/{id}/tag` (adicionar tag) | Tags sao ACUMULADAS (append) |

Isso e CRITICO: se voce usar PATCH com tags, vai APAGAR as tags anteriores.

### Endpoint: Adicionar Tag

**POST** `https://api.rd.services/platform/contacts/{identifier}:{value}/tag`

```json
{
  "tags": ["nova-tag-1", "nova-tag-2"]
}
```

- Requer contato pre-existente (retorna 404 se nao existir)
- Tags sao acumuladas (nao substitui)
- Nao gera evento de conversao

### Uso em Segmentacao

- Filtro: "Lead possui tag X"
- Filtro: "Lead NAO possui tag X"
- Combinavel com outros criterios (AND/OR)

### Uso em Automacao

- Acao: Adicionar Tag
- Acao: Remover Tag
- Condicao: Lead possui/nao possui tag
- Gatilho: nao disponivel diretamente por tag (usar segmentacao)

### Tags na Importacao

- Coluna "Tag" no CSV
- Todos os leads importados recebem a tag automaticamente
- Nome da importacao tambem gera tag automatica
- Permite segmentar leads importados facilmente

### Boas Praticas

1. Usar nomenclatura padronizada (ex: prefixo por tipo: `campanha_`, `interesse_`, `status_`)
2. Evitar tags duplicadas com variacoes de escrita
3. Usar tags para marcar leads que passaram por fluxos especificos
4. Usar tags como "lista estatica" (ja que segmentacoes sao dinamicas)
5. Documentar convencao de tags da equipe

---

## 14. BASE DE LEADS

### Conceito

No RD Station, todos os registros de pessoas sao chamados de **Contatos**, independente do estagio (Lead, Lead Qualificado, Cliente). O **email** e o identificador unico.

### Estagios do Funil

| Estagio | Descricao |
|---------|-----------|
| Lead | Contato inicial |
| Lead Qualificado | Qualificado por marketing (MQL) |
| Cliente | Fechou negocio |

### Importacao de Leads

**Formato:** CSV
**Limite:** 20 MB por arquivo
**Requisitos:**
- Todas as colunas devem ter cabecalho
- Coluna "Email" obrigatoria
- Colunas devem corresponder a campos padrao ou personalizados do RD

**Opcoes na importacao:**
- Definir nome da importacao (gera tag automatica)
- Mapear colunas para campos RD
- Definir base legal (LGPD)
- Coluna "Tag" para adicionar tags em massa

**Erros comuns:**
- Emails invalidos
- Colunas sem cabecalho
- Formato de arquivo incorreto
- Campos incompativeis

### Exportacao de Leads

**Opcoes de exportacao:**
- Base completa
- Leads de uma Landing Page especifica
- Leads de uma segmentacao
- Conversoes de leads

**Formato:** CSV
**Campos exportados:** todos os campos padrao + personalizados

### Gestao de Contatos via API

| Operacao | Endpoint | Metodo |
|----------|----------|--------|
| Criar contato | `/platform/contacts` | POST |
| Atualizar por UUID/email | `/platform/contacts/{identifier}:{value}` | PATCH |
| Adicionar tags | `/platform/contacts/{identifier}:{value}/tag` | POST |
| Consultar eventos | `/platform/contacts/{uuid}/events` | GET |
| Listar campos | `/platform/contacts/fields` | GET |
| Criar campo | `/platform/contacts/fields` | POST |

### Bases Legais (LGPD)

Todo contato pode ter bases legais associadas:

| Campo | Valores |
|-------|---------|
| category | `communications` |
| type | `pre_existent_contract`, `consent`, `legitimate_interest`, `judicial_process`, `vital_interest`, `public_interest` |
| status | `granted`, `declined` |

### Dono do Lead (Lead Owner)

- Cada lead pode ter um "dono" (usuario responsavel)
- Atribuido manualmente, por automacao ou importacao
- Dono recebe notificacoes sobre o lead
- Consultavel e atualizavel via API

### Limitacoes

- Email e o unico identificador (nao ha merge automatico por telefone/nome)
- Deletar leads nao e possivel via API publica (apenas via interface)
- Importacoes nao disparam webhooks
- Limite de leads varia por plano

---

## 15. RELATORIOS E ANALISES

### Dashboard Principal

Indicadores gerais da conta:
- Total de visitantes
- Total de leads
- Total de leads qualificados
- Total de oportunidades
- Total de vendas
- Taxas de conversao entre etapas

### Analise de Canais

Canais de midia padrao do RD Station:

| Canal | Classificacao |
|-------|--------------|
| Busca Organica | Trafego de buscadores sem anuncio |
| Midia Paga | Google Ads (auto-tagging), utm_medium=cpc/ppc/paid |
| Social | Trafego de redes sociais |
| Email | Trafego de campanhas de email |
| Referencia (Referral) | Trafego de outros sites |
| Trafego Direto | Acesso direto (sem referrer) |
| Display | Banners e midia programatica |

**Classificacao automatica:**
- Google Ads com auto-tagging = Midia Paga (sem necessidade de UTMs)
- `utm_medium=paid-social` = classificado como Midia Paga (OBRIGATORIO para Meta/LinkedIn/TikTok segundo padrao do projeto)
- `utm_medium=cpc` = Busca Paga (apenas Google Search)

### Analise de Funil

- Modelo de analise que mostra performance das etapas do funil
- Usa TAG como atributo para agrupar
- Metricas por etapa: leads gerados, taxa de conversao
- Disponivel nos Dashboards Personalizados (Plano Advanced)

### Relatorio de Conversoes

- Total de conversoes por periodo
- Conversoes por Landing Page, Formulario, Pop-up
- Conversoes por canal de origem
- Conversoes por campanha (UTM)

### Relatorio de Email Marketing

| Metrica | Descricao |
|---------|-----------|
| Enviados | Total disparado |
| Entregues | Chegaram ao destino |
| Taxa de abertura | % que abriram |
| Taxa de clique | % que clicaram |
| Descadastros | Opt-outs |
| Bounces | Rejeicoes |
| Spam complaints | Marcados como spam |

### Relatorio de Negocios por Origem

- Oportunidades agrupadas por fonte e campanha
- Permite rastrear ROI por canal
- Cruza dados de marketing com vendas (CRM)

### Dashboards Personalizados (Plano Advanced)

- Construcao de analises customizadas
- Editor drag-and-drop de indicadores
- Filtros por etapa do funil, responsavel, periodo
- Modelos prontos ou criacao do zero
- Graficos, tabelas e indicadores
- Acompanhar leads, qualificacoes, oportunidades e vendas por canal, ponto de conversao ou campanha

### Metricas Disponiveis nos Dashboards

- Tags dos Leads
- Identificador da conversao
- Canal de origem da conversao
- Recurso usado na conversao (LP, Formulario, API)
- UTM Source e UTM Medium
- Estagio do funil
- Responsavel/dono do lead

### Lista Inteligente de Leads

- Analisa caracteristicas de todos os leads
- Compara com base de +35 mil clientes RD Station
- Retorna para cada lead:
  - **Chance do Lead Comprar** (score preditivo)
  - **Valor Estimado de Compra**
  - **Posicao do Lead** (ranking)

### Significado das Metricas nos Relatorios

Metricas padrao documentadas pela RD Station para interpretacao correta dos dados em cada tipo de relatorio.

### Limitacoes

- Dashboards Personalizados so disponivel no Plano Advanced
- Analise de Funil usa TAG como atributo principal (limitacao)
- Relatorios nao fazem atribuicao multi-touch nativa
- Dados de ROI dependem da integracao com CRM e marcacao correta de vendas
- Exportacao de relatorios pode ter limites de volume

---

## APENDICE: AUTENTICACAO DA API

### OAuth2 (API v2 - Recomendado)

1. Criar App na Appstore do RD Station Marketing
2. Obter `client_id` e `client_secret`
3. Fluxo OAuth2 para obter access_token
4. Header: `Authorization: Bearer {access_token}`
5. Tokens tem tempo limitado — necessario refresh

### API Key (Simplificado)

1. Criar App na Appstore
2. Gerar chave de API
3. Enviar como query parameter: `?api_key=SUA_CHAVE`
4. Chaves sao estaticas, nao expiram
5. Uso recomendado: integracoes de formulario de conversao

### Migracao API v1 para v2

| Aspecto | v1 | v2 |
|---------|----|----|
| Autenticacao | Token publico/privado | OAuth2 |
| Base URL | rdstation.com.br/api/1.x | api.rd.services |
| Conversao | /api/1.3/conversions | /platform/events |
| Atualizacao | /api/1.2/leads/{email} | /platform/events |
| Campos custom | Nome de exibicao | api_identifier (cf_*) |
| Estrutura | Flat | event_type + event_family + payload |

**Mapeamento de campos v1 → v2:**
- `token_rdstation` → removido (usar OAuth)
- `identificador` → `conversion_identifier`
- `cargo` → `job_title`
- `estado` → `state`
- `cidade` → `city`
- `c_utmz` → `traffic_source`
- `client_id` → `client_tracking_id`

---

## APENDICE: TODOS OS TIPOS DE EVENTO (API v2)

| event_type | Descricao |
|-----------|-----------|
| CONVERSION | Conversao padrao |
| OPPORTUNITY | Marcar como oportunidade |
| SALE / OPPORTUNITY_WON | Venda/oportunidade ganha |
| OPPORTUNITY_LOST | Oportunidade perdida |
| CHAT_STARTED | Chat iniciado |
| CHAT_FINISHED | Chat finalizado |
| CALL_FINISHED | Ligacao finalizada |
| MEDIA_PLAYBACK_STARTED | Reproducao de midia iniciada |
| MEDIA_PLAYBACK_STOPPED | Reproducao de midia parada |
| ECOMMERCE_CHECKOUT_STARTED | Checkout iniciado |
| ECOMMERCE_CART_ABANDONED | Carrinho abandonado |
| ECOMMERCE_ORDER_PLACED | Pedido realizado |
| ECOMMERCE_ORDER_PAID | Pedido pago |
| ECOMMERCE_ORDER_FULFILLED | Pedido cumprido |
| ECOMMERCE_SHIPMENT_DELIVERED | Entrega realizada |
| ECOMMERCE_ORDER_CANCELLED | Pedido cancelado |
| ECOMMERCE_ORDER_REFUNDED | Pedido reembolsado |

O campo `event_family` deve ser SEMPRE `"CDP"`.

---

## FONTES

### Developers (API)
- [Fluxos de Automacao](https://developers.rdstation.com/reference/fluxos-de-automa%C3%A7%C3%A3o)
- [Campos Personalizados](https://developers.rdstation.com/reference/campos-personalizados)
- [Criar Campo](https://developers.rdstation.com/reference/post_platform-contacts-fields)
- [Listar Campos](https://developers.rdstation.com/reference/get_platform-contacts-fields)
- [Segmentacoes](https://developers.rdstation.com/reference/segmenta%C3%A7%C3%B5es)
- [Funis de Contatos](https://developers.rdstation.com/reference/funis-de-contatos)
- [Contatos](https://developers.rdstation.com/reference/contatos)
- [Criar Contato](https://developers.rdstation.com/reference/post_platform-contacts)
- [Atualizar Contato](https://developers.rdstation.com/reference/patch_platform-contacts-identifier-value)
- [Criar Tag](https://developers.rdstation.com/reference/post_platform-contacts-identifier-value-tag)
- [Evento de Conversao Padrao](https://developers.rdstation.com/reference/evento-de-conversao-padrao)
- [Conversao via API Key](https://developers.rdstation.com/reference/conversao)
- [Evento de Qualificacao](https://developers.rdstation.com/reference/evento-de-qualificacao-do-contato)
- [Batch de Eventos](https://developers.rdstation.com/reference/batch-eventos)
- [Webhooks Service](https://developers.rdstation.com/reference/webhooks)
- [Webhooks MKT Payload](https://developers.rdstation.com/reference/webhooks-payload-mkt)
- [Criar Webhook](https://developers.rdstation.com/reference/post_integrations-webhooks)
- [Pop-ups](https://developers.rdstation.com/reference/pop-ups)
- [Enriquecimento de Dados](https://developers.rdstation.com/reference/enriquecimento-de-dados-rdsm)
- [Migracao API v1 para v2](https://developers.rdstation.com/reference/rdsm-como-migrar-api-v1-para-api-v2)

### Central de Ajuda
- [Como funciona Automacao](https://ajuda.rdstation.com/s/article/Como-funciona-a-Automacao-de-Marketing?language=pt_BR)
- [Acoes do Fluxo](https://ajuda.rdstation.com/s/article/Criar-Automa%C3%A7%C3%B5es-de-Marketing-simples?language=pt_BR)
- [Tipos de Acoes e Sequencias](https://ajuda.rdstation.com/s/article/tipos-acoes-asequencias-fluxo-Automacao?language=pt_BR)
- [Lead Scoring](https://ajuda.rdstation.com/s/article/Como-funciona-o-Lead-Scoring?language=pt_BR)
- [Configurar Lead Scoring](https://ajuda.rdstation.com/s/article/Configurar-regras-do-Lead-Scoring?language=pt_BR)
- [Segmentacao por Dados](https://ajuda.rdstation.com/s/article/Criar-segmenta%C3%A7%C3%A3o-por-Dados-do-Lead?language=pt_BR)
- [Criar Campos Personalizados](https://ajuda.rdstation.com/s/article/Criar-Campos-Personalizados?language=pt_BR)
- [Campos Padrao](https://ajuda.rdstation.com/s/article/Campos-padr%C3%A3o?language=pt_BR)
- [Variaveis em Email](https://ajuda.rdstation.com/s/article/Usar-vari%C3%A1veis-em-emails?language=pt_BR)
- [Teste A/B Email](https://ajuda.rdstation.com/s/article/Fazer-teste-A-B-para-assunto-do-email?language=pt_BR)
- [Criar Landing Page](https://ajuda.rdstation.com/s/article/Criar-Landing-Page-conversao-de-leads?language=pt_BR)
- [Formulario Inteligente](https://ajuda.rdstation.com/s/article/Configura%C3%A7%C3%A3o-de-Formul%C3%A1rio-Inteligente?language=pt_BR)
- [Formularios Embedded](https://ajuda.rdstation.com/s/article/Criar-formul%C3%A1rios-para-adicionar-em-seu-site?language=pt_BR)
- [Pop-ups](https://ajuda.rdstation.com/s/article/Criar-e-configurar-pop-ups?language=pt_BR)
- [Regras Pop-up](https://ajuda.rdstation.com/s/article/Regras-de-exibi%C3%A7%C3%A3o-do-Pop-up?language=pt_BR)
- [Facebook Lead Ads](https://ajuda.rdstation.com/s/article/Integra%C3%A7%C3%A3o-com-o-Facebook-Lead-Ads?language=pt_BR)
- [LinkedIn Lead Ads](https://ajuda.rdstation.com/s/article/integrar-RD-Station-Marketing-com-LinkedIn-Lead-Ads?language=pt_BR)
- [Lead Tracking](https://ajuda.rdstation.com/s/article/Como-funciona-o-Lead-Tracking?language=pt_BR)
- [Cookies](https://ajuda.rdstation.com/s/article/Orienta%C3%A7%C3%B5es-sobre-o-uso-de-cookies?language=pt_BR)
- [Tags](https://ajuda.rdstation.com/s/article/Inserir-tags-em-leads?language=pt_BR)
- [Importacao](https://ajuda.rdstation.com/s/article/Importa%C3%A7%C3%A3o-de-Leads-e-Bases-Legais?language=pt_BR)
- [Exportacao](https://ajuda.rdstation.com/s/article/Exportar-leads?language=pt_BR)
- [Canais de Midia](https://ajuda.rdstation.com/s/article/Quais-s%C3%A3o-os-canais-de-m%C3%ADdia-padr%C3%A3o-do-RD-Station-Marketing?language=pt_BR)
- [Analise de Canais](https://ajuda.rdstation.com/s/article/Como-utilizar-a-An%C3%A1lise-de-Canais?language=pt_BR)
- [Analise de Funil](https://ajuda.rdstation.com/s/article/An%C3%A1lise-de-Funil-nos-Dashboards-Personalizados-do-RD-Station-Marketing)
- [Deliverability](https://ajuda.rdstation.com/s/article/Como-melhorar-a-Entregabilidade-das-suas-campanhas-de-Email?language=pt_BR)
- [Subdominio Email](https://ajuda.rdstation.com/s/article/Configurar-subdom%C3%ADnio-de-Email?language=pt_BR)
- [DNS](https://ajuda.rdstation.com/s/article/Configurar-registros-DNS?language=pt_BR)

### Blog e Outros
- [Acoes de Automacao no Funil](https://blog.rdstation.com/2-exemplos-como-usar-as-acoes-de-automacao-de-marketing-para-avancar-leads-no-funil-de-vendas/)
- [Lead Scoring Guia](https://www.rdstation.com/blog/marketing/o-que-e-lead-scoring/)
- [Variaveis de Email](https://blog.rdstation.com/variaveis-de-email-personalizaveis/)
- [Pop-ups Estrategia](https://www.rdstation.com/blog/marketing/pop-ups/)
- [Dashboards Personalizados](https://www.rdstation.com/produtos/marketing/analises/dashboards-personalizados/)
- [Origem de Contatos](https://medium.com/rd-shipit/enviar-a-origem-de-contatos-para-o-rd-station-marketing-bc7d5cede776)
