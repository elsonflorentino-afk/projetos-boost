# Briefing Visual — Meta Ads E-book 1 (Alocacao Inteligente)
## Boost Research | @rafa (Designer) | Abril 2026

**Produto:** E-book "Como Montar uma Carteira Cripto + Tradicional com Mais de R$100 mil"
**Objetivo:** Criativos para captura de leads qualificados via Meta Ads
**Aprovacao:** @helena (Creative Director) antes de producao e antes de upload

---

## 1. SPECS OFICIAIS META ADS 2026

### 1.1 Dimensoes por formato

| Formato | Dimensao (px) | Aspect Ratio | Uso principal |
|---------|--------------|--------------|---------------|
| **Feed Quadrado** | 1080 x 1080 | 1:1 | Facebook Feed, Instagram Feed |
| **Feed Vertical** | 1080 x 1350 | 4:5 | Instagram Feed (mais area visual) |
| **Stories/Reels** | 1080 x 1920 | 9:16 | Instagram Stories, Facebook Stories, Reels |
| **Carrossel** | 1080 x 1080 | 1:1 (obrigatorio) | Feed Facebook + Instagram |

### 1.2 Limites tecnicos

| Parametro | Valor |
|-----------|-------|
| **Tamanho maximo imagem** | 30 MB |
| **Tamanho maximo video** | 4 GB |
| **Formatos imagem** | JPG (fotos), PNG (texto/logos com transparencia) |
| **Formatos video** | MP4, MOV |
| **Resolucao recomendada** | 1440 x 1440 (1:1) ou 1440 x 1800 (4:5) para qualidade maxima |
| **Cards no carrossel** | 2 a 10 (todos com mesmo aspect ratio) |

### 1.3 Regra de texto na imagem

A regra dos 20% de texto foi **oficialmente removida** pelo Meta. Porem, o algoritmo **ainda penaliza** criativos com muito texto sobreposicao, reduzindo o alcance no leilao. Recomendacao pratica:

- Manter texto na imagem **abaixo de 20%** da area total
- Headlines curtas e impactantes (max 2 linhas)
- Usar o campo Primary Text do anuncio para texto longo, NAO a imagem

### 1.4 Safe Zones — Stories/Reels (9:16)

```
┌──────────────────────────────┐  0px
│     SAFE ZONE TOPO (14%)     │
│     270px — perfil + nome    │
│     NAO COLOCAR TEXTO AQUI  │
├──────────────────────────────┤  270px
│                              │
│                              │
│   ZONA SEGURA PARA CONTEUDO │
│   (51% da tela)             │
│   270px a 1250px            │
│                              │
│                              │
├──────────────────────────────┤  1250px
│     SAFE ZONE BASE (35%)     │
│     670px — CTA + legenda    │
│     NAO COLOCAR TEXTO AQUI  │
│     (Reels usa mais que      │
│      Stories — usar 35%      │
│      como margem segura)     │
└──────────────────────────────┘  1920px
```

**Margens laterais:** 6% de cada lado (65px) — NAO colocar elementos criticos

**Zona segura efetiva Stories/Reels:**
- Horizontal: 65px a 1015px (excluindo 6% cada lado)
- Vertical: 270px a 1250px (excluindo 14% topo e 35% base)

---

## 2. IDENTIDADE VISUAL BOOST RESEARCH

### 2.1 Paleta de cores

```css
/* Fundos */
--bg-primary:     #12191D;   /* fundo escuro padrao Boost */
--bg-deep:        #000000;   /* fundo preto puro (alternativa) */
--bg-overlay:     rgba(0, 0, 0, 0.75);  /* overlay sobre foto */

/* Destaques */
--green-boost:    #03E4D0;   /* verde ciano — destaques, acentos */
--green-cta:      #00B37E;   /* verde escuro — botoes CTA */

/* Texto */
--text-primary:   #FFFFFF;   /* titulos, headlines */
--text-secondary: #A0A0C0;  /* subtitulos, descricoes */
--text-muted:     #6B7280;  /* disclaimer, footnotes */

/* Accent */
--gold-author:    #C9A84C;   /* nome do autor, badge premium */
--gold-light:     #D4AF37;   /* variacao dourada para detalhe */
```

### 2.2 Tipografia

| Elemento | Fonte | Peso | Tamanho | Observacao |
|----------|-------|------|---------|------------|
| **Headline** | Inter | Black (900) | 56-72pt | Legivel em thumbnail 200x200px |
| **Subheadline** | Inter | SemiBold (600) | 28-36pt | Complemento da headline |
| **Corpo** | Inter | Regular (400) | 20-24pt | Descricoes, bullets |
| **CTA no criativo** | Inter | Bold (700) | 22-28pt | Com seta → |
| **Disclaimer** | Inter | Regular (400) | 14-16pt | Minimo legivel, rodape |
| **Badge/Label** | Inter | Medium (500) | 18-22pt | "GRATUITO", "E-BOOK" |

**Teste de legibilidade:** Toda headline DEVE ser legivel quando a peca e reduzida para 200x200px (tamanho de thumbnail no feed mobile). Se nao for, a headline esta grande demais ou com pouco contraste.

### 2.3 Assets obrigatorios

| Asset | Arquivo | Uso |
|-------|---------|-----|
| Logo Boost horizontal | `boost-logo-horizontal.png` / `.svg` | Canto superior esquerdo, branco |
| Logo Boost vertical | `boost-logo.png` | Quando espaco horizontal e limitado |
| Fotos Andre Franco | `/Volumes/SSD Externo/Boost Research/Fotos Andre/` | KV com autor |
| Mock 3D e-book | **A PRODUZIR** | KV com produto |

---

## 3. GRID E LAYOUT POR FORMATO

### 3.1 FEED QUADRADO (1080 x 1080) — Layout E-book

```
┌──────────────────────────────────────┐
│  margin: 60px all sides              │
│                                      │
│  [LOGO BOOST]          [BADGE]       │
│  branco, 120x40px      "E-BOOK      │
│  canto sup esq          GRATUITO"    │
│                         #03E4D0      │
│                                      │
│         ┌──────────────┐             │
│         │              │             │
│         │   MOCK 3D    │             │
│         │   E-BOOK     │             │
│         │              │             │
│         │  380x480px   │             │
│         │  centraliz.  │             │
│         └──────────────┘             │
│                                      │
│  ─── HEADLINE (max 2 linhas) ───    │
│  Inter Black 56pt, #FFFFFF           │
│  Palavra-chave em #03E4D0           │
│                                      │
│  Subheadline                         │
│  Inter SemiBold 28pt, #A0A0C0       │
│                                      │
│  ─────────────────────────────────   │
│  Disclaimer 14pt #6B7280            │
└──────────────────────────────────────┘
```

**Margens:** 60px em todos os lados (5,5% da largura)
**Espaco entre elementos:** 24px minimo

### 3.2 FEED QUADRADO (1080 x 1080) — Layout Andre + E-book

```
┌──────────────────────────────────────┐
│  margin: 60px                        │
│                                      │
│  [LOGO BOOST]                        │
│                                      │
│  ┌────────────┐  ┌──────────────┐   │
│  │            │  │              │   │
│  │  FOTO      │  │  MOCK 3D    │   │
│  │  ANDRE     │  │  E-BOOK     │   │
│  │  recorte   │  │             │   │
│  │  busto     │  │  280x350px  │   │
│  │            │  │             │   │
│  │  420x520   │  └──────────────┘   │
│  └────────────┘                      │
│                                      │
│  HEADLINE 2 linhas                   │
│  #FFFFFF + palavra em #03E4D0       │
│                                      │
│  "Andre Franco" #C9A84C             │
│  "Boost Research" #A0A0C0           │
│                                      │
│  Disclaimer #6B7280                 │
└──────────────────────────────────────┘
```

### 3.3 FEED VERTICAL (1080 x 1350) — 4:5

```
┌──────────────────────────────────────┐
│  margin: 60px                        │
│                                      │
│  [LOGO BOOST]          [BADGE]       │
│                         "GRATUITO"   │
│                                      │
│         ┌──────────────┐             │
│         │              │             │
│         │   MOCK 3D    │             │
│         │   E-BOOK     │             │
│         │              │             │
│         │  440x560px   │             │
│         │  centraliz.  │             │
│         └──────────────┘             │
│                                      │
│  ─── HEADLINE ───                   │
│  Inter Black 64pt, #FFFFFF           │
│  ate 3 linhas (mais espaco)          │
│                                      │
│  Subheadline                         │
│  Inter SemiBold 30pt, #A0A0C0       │
│                                      │
│  → Baixe gratis                      │
│  Inter Bold 24pt, #00B37E           │
│                                      │
│  Andre Franco · Boost Research       │
│  #C9A84C        #A0A0C0             │
│                                      │
│  ─────────────────────────────────   │
│  Disclaimer 14pt #6B7280            │
└──────────────────────────────────────┘
```

### 3.4 STORIES/REELS (1080 x 1920) — 9:16

```
┌──────────────────────────────────────┐  0px
│                                      │
│  ▒▒▒ SAFE ZONE TOPO (270px) ▒▒▒     │
│  [LOGO BOOST — unico elem. aqui]    │
│  posicao: 65px left, 80px top       │
│  (fica ACIMA do perfil no story)    │
│                                      │
├──────────────────────────────────────┤  270px
│                                      │
│  [BADGE "E-BOOK GRATUITO"]          │
│  pill shape, bg #03E4D0, txt #000   │
│  posicao: centralizado, y=310px     │
│                                      │
│         ┌──────────────┐             │
│         │              │             │
│         │   MOCK 3D    │             │
│         │   E-BOOK     │             │
│         │              │             │
│         │  500x640px   │             │
│         │  centraliz.  │             │
│         └──────────────┘             │
│                                      │
│  ─── HEADLINE ───                   │
│  Inter Black 60pt, #FFFFFF           │
│  centralizado, max 3 linhas         │
│  posicao: y=950px a y=1120px        │
│                                      │
│  Subheadline                         │
│  Inter SemiBold 28pt, #A0A0C0       │
│  posicao: y=1140px                   │
│                                      │
│  "Andre Franco · Boost Research"     │
│  #C9A84C, y=1200px                   │
│                                      │
├──────────────────────────────────────┤  1250px
│                                      │
│  ▒▒▒ SAFE ZONE BASE (670px) ▒▒▒     │
│  NAO colocar conteudo critico       │
│  CTA do Meta aparece aqui           │
│                                      │
└──────────────────────────────────────┘  1920px
```

**Margens laterais:** 65px cada lado (6%)

### 3.5 CARROSSEL (1080 x 1080 x N cards)

**Estrutura recomendada: 5 cards**

| Card | Conteudo | Elementos |
|------|----------|-----------|
| **1 — Capa** | Hook + mock e-book | Logo, badge "GRATUITO", mock 3D, headline curta |
| **2 — Dor** | Problema do publico | Icone/ilustracao, headline da dor, stat de mercado |
| **3 — Solucao** | O que o e-book ensina | 3-4 bullets com checkmarks verdes (#03E4D0) |
| **4 — Autoridade** | Andre Franco | Foto recorte busto, nome #C9A84C, credenciais |
| **5 — CTA** | Chamada final | Mock e-book + "Baixe gratis →" + logo Boost |

**Layout de cada card do carrossel:**

```
┌──────────────────────────────────────┐
│  margin: 48px all sides              │
│                                      │
│  [LOGO BOOST — 100x34px]            │
│  canto sup esq, TODOS os cards      │
│                                      │
│  ┌──────────────────────────────┐   │
│  │                              │   │
│  │   AREA DE CONTEUDO           │   │
│  │   (varia por card)           │   │
│  │                              │   │
│  │   650 x 700px util           │   │
│  │                              │   │
│  └──────────────────────────────┘   │
│                                      │
│  HEADLINE / CTA                      │
│  Inter Black 48pt                    │
│                                      │
│  ─────────── linha #03E4D0 ──────   │
│  Indicador visual de continuidade    │
└──────────────────────────────────────┘
```

**Regra de continuidade:** usar uma linha horizontal #03E4D0 na mesma posicao (y=980px) em todos os cards para criar efeito de "fita" ao passar os cards.

---

## 4. HIERARQUIA VISUAL

### Ordem de leitura (prioridade de percepcao)

| Prioridade | Elemento | Justificativa |
|------------|----------|---------------|
| **1o** | Mock 3D do e-book OU foto Andre | O visual anchor — o que prende o olho no scroll |
| **2o** | Headline | A promessa ou gancho — Inter Black, #FFFFFF |
| **3o** | Badge "GRATUITO" | Elimina barreira de custo — #03E4D0 chamativo |
| **4o** | Subheadline | Contexto adicional — #A0A0C0 |
| **5o** | Nome do autor | Credibilidade — #C9A84C dourado |
| **6o** | Logo Boost | Branding — branco, discreto mas presente |
| **7o** | Disclaimer | Compliance — #6B7280, rodape |

### Contraste e peso visual

- **Maior peso:** Mock 3D ou foto (50-60% da area visual)
- **Segundo peso:** Headline em branco sobre fundo escuro (contraste maximo)
- **Verde #03E4D0:** usado APENAS para 1-2 palavras-chave na headline + badge. NAO usar em blocos grandes de texto
- **Dourado #C9A84C:** EXCLUSIVO para nome "Andre Franco". NAO usar em outros elementos

---

## 5. REGRAS DE ESPACAMENTO E MARGENS

### Sistema de espacamento (multiplos de 12px)

| Token | Valor | Uso |
|-------|-------|-----|
| `space-xs` | 12px | Entre label e valor |
| `space-sm` | 24px | Entre elementos do mesmo grupo |
| `space-md` | 36px | Entre grupos de informacao |
| `space-lg` | 48px | Entre secoes principais |
| `space-xl` | 60px | Margem externa (feed) |

### Margens por formato

| Formato | Margem externa | Margem do logo | Margem do disclaimer |
|---------|---------------|----------------|---------------------|
| Feed 1:1 | 60px | 60px top, 60px left | 60px bottom |
| Feed 4:5 | 60px | 60px top, 60px left | 60px bottom |
| Stories 9:16 | 65px lateral | 65px left, 80px top | NAO usar (safe zone) |
| Carrossel | 48px | 48px top, 48px left | 48px bottom |

### Alinhamento

- **Feed:** texto alinhado a esquerda (mais natural para leitura)
- **Stories:** texto centralizado (formato vertical pede simetria)
- **Carrossel:** texto alinhado a esquerda (consistencia entre cards)

---

## 6. TRATAMENTO DA FOTO DO ANDRE FRANCO

### 6.1 Recorte e posicao

| Parametro | Especificacao |
|-----------|--------------|
| **Tipo de recorte** | Busto (do peito para cima) com fundo removido (PNG transparente) |
| **Posicao no feed 1:1** | Lado esquerdo, 40-50% da largura, alinhado a base do terco medio |
| **Posicao no feed 4:5** | Terco superior, 55% da altura, com gradiente de transicao para texto |
| **Posicao no stories** | NAO usar foto do Andre nos criativos de e-book stories (priorizar mock) |
| **Tamanho minimo** | Rosto deve ocupar no minimo 15% da area total da peca |

### 6.2 Tratamento de imagem

- **Color grading:** Leve desaturacao para harmonizar com fundo #12191D
- **Overlay:** Gradiente linear de #12191D (ou #000) na base da foto, 30% de altura, para transicao suave ao bloco de texto
- **Brilho do rosto:** Manter iluminacao natural. NAO escurecer o rosto
- **Contorno:** Sem stroke, sem glow. A foto se integra naturalmente ao fundo escuro

### 6.3 Fotos recomendadas para e-book ads

| Foto | Mood | Quando usar |
|------|------|-------------|
| `af-240.jpg` | Autoridade (NYSE, bracos cruzados) | Headline sobre institucional/mercado |
| `af-106.jpg` | Casual pensativo (cafe, smartwatch) | Headline sobre rotina/dia-a-dia |
| `af-275.jpg` | Moderno institucional (Oculus WTC) | Headline sobre carteira/portfolio |

### 6.4 Credencial visual

Quando a foto do Andre aparece, SEMPRE incluir abaixo:
```
Andre Franco          ← Inter SemiBold 22pt, #C9A84C
Analista desde 2016 · Boost Research  ← Inter Regular 18pt, #A0A0C0
```

---

## 7. REPRESENTACAO DO E-BOOK

### 7.1 Formato do mockup

**Formato principal: Mock 3D perspectiva**

- Livro em perspectiva 3/4 (virado levemente para a direita)
- Sombra suave projetada (#000, 20% opacidade, offset 8px, blur 24px)
- Leve reflexo na base (espelhamento com 10% opacidade, gradiente fade)
- Capa do e-book real visivel (titulo + identidade Boost)

**Formato alternativo: iPad/tablet mockup**

- Para variacao A/B: e-book aberto em tela de tablet
- Tablet em angulo 15-20 graus
- Tela mostrando pagina interna com graficos/tabelas
- Moldura do device em cinza escuro (#2D2D2D) para nao competir com o conteudo

### 7.2 Dimensoes do mock por formato

| Formato | Tamanho do mock | Posicao |
|---------|----------------|---------|
| Feed 1:1 | 380 x 480px | Centro, levemente acima do meio |
| Feed 4:5 | 440 x 560px | Centro, terco superior |
| Stories 9:16 | 500 x 640px | Centro, entre badge e headline |
| Carrossel | 320 x 400px | Centro (card 1 e 5) |

### 7.3 Capa do e-book (para o mock)

A capa que aparece NO mock deve conter:
- Fundo: gradiente #12191D → #000000
- Titulo: "Como Montar uma Carteira Cripto + Tradicional" em Inter Black, branco
- Subtitulo: "com Mais de R$100 mil" em #03E4D0
- Logo Boost Research: rodape da capa, branco
- Elemento decorativo: linha ou barra #03E4D0 separando titulo de subtitulo

NAO incluir foto do Andre na capa do mock (capa limpa, tipografica).

---

## 8. DISCLAIMER E COMPLIANCE

### Texto padrao

```
Conteudo educativo. Nao constitui recomendacao de investimento.
Boost Research — boostresearch.com.br
```

### Especificacoes

| Parametro | Valor |
|-----------|-------|
| Fonte | Inter Regular |
| Tamanho | 14pt (minimo legivel) |
| Cor | #6B7280 |
| Posicao Feed | Rodape, 60px da base, alinhado a esquerda |
| Posicao Stories | NAO incluir (safe zone da base; usar legenda do ad) |
| Posicao Carrossel | Rodape do card 5 (CTA final) |

---

## 9. VARIACOES PARA TESTE A/B

### Eixo visual (KV — Key Visual)

| Variacao | KV principal | Publico-alvo |
|----------|-------------|--------------|
| **A — E-book Hero** | Mock 3D do e-book centralizado | Lead frio, curioso pelo conteudo |
| **B — Andre + E-book** | Foto Andre lado esquerdo + mock lado direito | Lead que conhece o Andre |
| **C — Data Visual** | Grafico/tabela estilizado + mock pequeno | Lead analitico, quer dados |

### Eixo de headline (testar com mesmo visual)

| Variacao | Headline |
|----------|----------|
| **H1 — Dor** | "Sua carteira esta alocada errado" |
| **H2 — Filtro** | "Voce tem R$100k+? Baixe o metodo Andre Franco" |
| **H3 — Dado** | "84% dos brasileiros nao rebalanceiam. E voce?" |
| **H4 — Pergunta** | "Cripto + tradicional: qual o % ideal?" |

### Matriz de teste recomendada

Minimo 6 combinacoes: 3 KVs x 2 headlines = 6 criativos iniciais.

---

## 10. CHECKLIST DE PRODUCAO

### Antes de comecar (pre-producao)

- [ ] Mock 3D do e-book produzido (com capa real)
- [ ] Fotos do Andre recortadas (PNG transparente, alta resolucao)
- [ ] Logo Boost em SVG/PNG branco disponivel
- [ ] Fonts Inter instaladas (todos os pesos: Regular, Medium, SemiBold, Bold, Black)

### Por peca (producao)

- [ ] Dimensoes corretas para o formato
- [ ] Margens respeitadas conforme secao 5
- [ ] Safe zones de Stories respeitadas (topo 14%, base 35%, laterais 6%)
- [ ] Hierarquia visual correta (secao 4)
- [ ] Texto na imagem abaixo de 20% da area
- [ ] Headline legivel em 200x200px
- [ ] Logo Boost presente
- [ ] Disclaimer presente (exceto Stories)
- [ ] Cores exatas da paleta (#12191D, #03E4D0, #00B37E, #FFFFFF, #A0A0C0, #C9A84C)
- [ ] Exportado em PNG (para texto nitido) ou JPG alta qualidade (90%+)
- [ ] Arquivo menor que 30MB

### Antes de upload (@helena audit)

- [ ] Aprovacao da @helena (checklist 12 pontos)
- [ ] Vocabulario Boost correto (NAO usar "research house", "advisor", "mentoria")
- [ ] Credencial Andre: "analista desde 2016" (padronizado)
- [ ] Todas as variacoes com mesma qualidade visual
- [ ] Arquivo nomeado: `ebook1-{formato}-{variacao}-v{versao}.png`

---

## 11. NOMENCLATURA DE ARQUIVOS

```
ebook1-feed-1x1-ebook-hero-v1.png
ebook1-feed-1x1-andre-ebook-v1.png
ebook1-feed-4x5-ebook-hero-v1.png
ebook1-story-9x16-ebook-hero-v1.png
ebook1-carousel-card1-capa-v1.png
ebook1-carousel-card2-dor-v1.png
ebook1-carousel-card3-solucao-v1.png
ebook1-carousel-card4-autoridade-v1.png
ebook1-carousel-card5-cta-v1.png
```

---

## FONTES E REFERENCIAS

### Specs oficiais Meta Ads 2026
- [Meta Ads Guide — Formatos e Specs](https://www.facebook.com/business/ads-guide/update)
- [Carousel Ad Specs](https://www.facebook.com/business/help/1114358518575630)
- [Aspect Ratios por Placement](https://www.facebook.com/business/help/682655495435254)
- [Safe Zones Stories e Reels](https://www.facebook.com/business/help/980593475366490)
- [Design Requirements Instagram Stories](https://www.facebook.com/business/help/2222978001316177)

### Guias de terceiros consultados
- [Surfside PPC — Meta Ad Image Sizes 2026](https://surfsideppc.com/blogs/meta-ads-blog-by-surfside-ppc/meta-ad-image-sizes-specs-2026)
- [AdManage — Meta Carousel Specs 2026](https://admanage.ai/blog/meta-carousel-ad-specs)
- [Billo — Meta Ads Safe Zones 2026](https://billo.app/blog/meta-ads-safe-zones/)
- [AdsUploader — Meta Ads Size Guide](https://adsuploader.com/blog/meta-ads-size)
- [Buffer — Facebook Ad Specs 2026](https://buffer.com/resources/facebook-ad-specs-image-sizes/)

---

*Briefing produzido por @rafa (Designer) — Boost Research Creative Squad*
*Sujeito a aprovacao @helena (Creative Director) antes de producao*
