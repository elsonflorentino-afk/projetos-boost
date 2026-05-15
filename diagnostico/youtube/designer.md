# Boost Research — Design System Reference

Extraido de boostresearch.com.br em 15/mai/2026.
Usar como referencia obrigatoria para todas as LPs e paginas do projeto.

## Cores

| Token | Valor | Uso |
|-------|-------|-----|
| --teal | #19c6b5 | Acento primario, CTAs, graficos, destaques |
| --teal-light | #e6fffb | Background de labels, pills, badges |
| --teal-dark | #0f766e | Texto sobre teal-light |
| --text | #FFFFFF | Headlines, texto principal |
| --text-secondary | rgba(255,255,255,0.7) | Corpo de texto |
| --text-muted | rgba(255,255,255,0.4) | Texto secundario, captions |
| --bg | #0B0E13 | Background principal (dark) |
| --bg-alt | #111620 | Background alternativo (secoes pares) |
| --bg-dark | #080A0F | Background mais escuro |
| --card-bg | rgba(255,255,255,0.04) | Background de cards |
| --border | rgba(255,255,255,0.1) | Bordas de cards, divisores, inputs |
| --divider | rgba(255,255,255,0.06) | Linhas divisorias finas |

## Tipografia

| Nivel | Tamanho | Peso | Fonte |
|-------|---------|------|-------|
| Display/Hero | 42px | 700-800 | Arial, sans-serif |
| H1 | 36px | 700 | Arial, sans-serif |
| H2 | 20px | 700 | Arial, sans-serif |
| Body | 16px (1rem) | 400 | Arial, sans-serif |
| Small | 13px | 400 | Arial, sans-serif |
| Caption/Label | 11px | 600 | Arial, sans-serif |
| Line-height body | 1.6 | — | — |

## Espacamento

| Token | Valor | Uso |
|-------|-------|-----|
| --space-xs | 0.44rem (7px) | Gaps minimos |
| --space-s | 0.67rem (11px) | Padding interno de pills |
| --space-m | 1rem (16px) | Gaps de grid, padding de campos |
| --space-l | 1.5rem (24px) | Gap padrao entre blocos (24px) |
| --space-xl | 2.25rem (36px) | Padding de secoes |
| --space-xxl | 3.38rem (54px) | Espacamento entre secoes |
| --space-xxxl | 5.06rem (81px) | Padding de hero |

## Layout

| Propriedade | Valor |
|-------------|-------|
| Container max-width | 1200px |
| Container narrow | 800px |
| Grid gap padrao | 24px |
| Breakpoints | 767px, 880px, 1024px, 1200px, 1366px |
| Container padding lateral | 20-40px |

## Bordas e Raios

| Componente | Border-radius | Border |
|------------|--------------|--------|
| Cards | 16px | 1px solid #e5e5e5 |
| Botoes primarios | 0 (retangular) | 0 |
| Pills/Labels | 20px | nenhuma |
| Inputs/Busca | 50px (fully rounded) | 1px solid #e5e5e5 |
| Imagens/Mockups | 16px | nenhuma |

## Sombras

| Tipo | Valor | Uso |
|------|-------|-----|
| Natural | 6px 6px 9px rgba(0,0,0,0.2) | Cards elevados |
| Sutil | 0 1px 3px rgba(0,0,0,0.06) | Cards rasos |
| Deep | 12px 12px 50px rgba(0,0,0,0.4) | Destaque hero |

## Botoes

### Primario
- Background: #32373c (cinza escuro)
- Color: #fff
- Padding: calc(0.667em + 2px) calc(1.333em + 2px)
- Border: 0
- Border-radius: 0 (retangular, sem arredondamento)
- Font: herdado, sem decoracao

### CTA Teal
- Background: #19c6b5
- Color: #fff
- Padding: 12px 28px
- Border-radius: 8px
- Font-weight: 700

## Cards

- Background: #ffffff
- Border: 1px solid #e5e5e5
- Border-radius: 16px
- Padding: 24px (interno) ou 12px (compacto)
- Shadow: 0 1px 3px rgba(0,0,0,0.06)

## Navbar

- Background: branco (com blur no scroll)
- Altura: ~64px
- Logo: PNG horizontal, ~40-48px altura
- CTA: botao primario ou link teal
- Mobile: hamburger com mega-menu

## Secoes

- Alternancia: branco (#fff) e cinza claro (#f5f5f5)
- Separacao: espacamento (24px+) ou borda sutil
- Padding vertical: 60-80px por secao
- Max-width do conteudo: 1200px

## Imagens

- object-fit: cover
- border-radius: 16px
- Mockups: largura responsiva, max-width 100%
- Fotos de equipe: 70x70 a 300x400 dependendo do contexto

## Animacoes

- Transicoes: 0.2-0.3s ease
- Graficos SVG: stroke-dasharray 1.8s ease-out
- Labels: fade-in 0.6s com delay
- Hover: filter brightness ou opacity

## Principios visuais

1. **Minimalismo limpo**: muito white space, cor seletiva
2. **Autoridade profissional**: cinza escuro + teal = confianca
3. **Acessibilidade**: alto contraste, fontes legiveis, hierarquia clara
4. **Mobile-first**: breakpoints Elementor, responsivo
5. **Foco em dados**: graficos SVG animados = credibilidade analitica
6. **Consistencia**: sistema de spacing, cores preset, tipografia sistematica
