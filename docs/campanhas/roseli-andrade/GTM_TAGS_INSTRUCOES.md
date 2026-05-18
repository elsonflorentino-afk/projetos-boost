# GTM Tags — Clínica Roseli Andrade
## Container: GTM-5C7C3HGQ

---

## TAG 1: UTM Cookie Writer (Custom HTML)

**Nome no GTM:** `UTM Cookie Writer - RA`
**Tipo:** Custom HTML
**Trigger:** All Pages (ou Page View)

Cole este código:

```html
<script>
(function() {
  'use strict';

  var COOKIE_DAYS = 30;
  var PREFIX = '_ra_';

  function setCookie(name, value, days) {
    if (!value) return;
    var d = new Date();
    d.setTime(d.getTime() + (days * 24 * 60 * 60 * 1000));
    document.cookie = name + '=' + encodeURIComponent(value) +
      ';expires=' + d.toUTCString() +
      ';path=/;SameSite=Lax';
  }

  function getParam(name) {
    var params = new URLSearchParams(window.location.search);
    return params.get(name) || '';
  }

  // Salvar gclid (Google Click ID)
  var gclid = getParam('gclid');
  if (gclid) {
    setCookie(PREFIX + 'gclid', gclid, COOKIE_DAYS);
  }

  // Salvar UTMs
  var utms = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content'];
  for (var i = 0; i < utms.length; i++) {
    var val = getParam(utms[i]);
    if (val) {
      setCookie(PREFIX + utms[i], val, COOKIE_DAYS);
    }
  }

  // Gerar visitor_id se não existir
  if (!document.cookie.match(/(^| )_ra_visitor=/)) {
    setCookie(PREFIX + 'visitor', 'v_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6), COOKIE_DAYS);
  }

})();
</script>
```

---

## TAG 2: WA Link Interceptor (Custom HTML)

**Nome no GTM:** `WA Link Interceptor - RA`
**Tipo:** Custom HTML
**Trigger:** All Pages (ou DOM Ready)

Cole este código:

```html
<script>
(function() {
  'use strict';

  // URL da Bridge Page no GitHub Pages
  var BRIDGE_URL = 'https://elsonflorentino.github.io/clinicaroseli/bridge.html';

  function interceptWaLinks() {
    // Selecionar todos os links que apontam para WhatsApp
    var links = document.querySelectorAll('a[href*="wa.me"], a[href*="whatsapp.com"], a[href*="api.whatsapp.com"]');

    for (var i = 0; i < links.length; i++) {
      links[i].addEventListener('click', function(e) {
        e.preventDefault();
        window.location.href = BRIDGE_URL;
      });
    }

    // Também interceptar links tel: para o número da clínica (se WhatsApp)
    var telLinks = document.querySelectorAll('a[href*="13-3224-2131"], a[href*="1332242131"]');
    for (var j = 0; j < telLinks.length; j++) {
      telLinks[j].addEventListener('click', function(e) {
        e.preventDefault();
        window.location.href = BRIDGE_URL;
      });
    }
  }

  // Executar quando DOM estiver pronto
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', interceptWaLinks);
  } else {
    interceptWaLinks();
  }

})();
</script>
```

---

## RESUMO DE CONFIGURAÇÃO NO GTM

| Tag | Tipo | Trigger | Prioridade |
|-----|------|---------|------------|
| UTM Cookie Writer - RA | Custom HTML | All Pages | Alta (executa primeiro) |
| WA Link Interceptor - RA | Custom HTML | DOM Ready | Normal |
| Conversion Linker | (já existe) | All Pages | - |

### Checklist de implementação:

1. [ ] Criar tag "UTM Cookie Writer - RA" → All Pages
2. [ ] Criar tag "WA Link Interceptor - RA" → DOM Ready
3. [ ] Verificar Conversion Linker já existe
4. [ ] Preview no GTM → testar com `?gclid=test123&utm_source=google&utm_campaign=botox`
5. [ ] Verificar cookies `_ra_gclid`, `_ra_utm_source`, etc no DevTools
6. [ ] Clicar no botão WhatsApp → deve ir para bridge.html
7. [ ] Bridge Page deve redirecionar para wa.me com protocolo RA-XXXXX
8. [ ] Verificar registro no Supabase (tabela wa_conversions)
9. [ ] Publicar container GTM

### Reversibilidade:

Se algo der errado: pausar as 2 tags novas no GTM + reativar fluxo antigo em 2 minutos. Zero impacto ao site.
