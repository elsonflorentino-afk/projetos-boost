#!/usr/bin/env python3
"""
generate_kpis_dashboard.py
Gera dashboard HTML "Metas e KPIs" com dados semanais (WoW) de Meta Ads + RD Station CRM.

Fontes:
  - Meta Ads API (spend, impressions, clicks, leads por semana)
  - RD Station CRM (deals por stage, agrupados por semana)
  - RD Station Marketing (leads qualificados por patrimonio >= R$50k)
  - CoinGecko (cotacao BTC)

Env vars (obrigatorias, sem default):
  META_ACCESS_TOKEN ou META_TOKEN
  RD_CRM_TOKEN
  RD_CLIENT_ID
  RD_CLIENT_SECRET
  RD_REFRESH_TOKEN

Saida:
  projetos-boost/painel/metas-kpis/data.json
  projetos-boost/painel/metas-kpis/index.html
"""
import json, urllib.request, urllib.parse, ssl, sys, os, time
from datetime import datetime, timezone
from calendar import monthrange

# ──────────────────────────────────────────────────────────────
# ENV VARS — nunca hardcodar tokens
# ──────────────────────────────────────────────────────────────
META_TOKEN = os.environ.get('META_ACCESS_TOKEN') or os.environ.get('META_TOKEN')
if not META_TOKEN:
    raise SystemExit('ERRO: defina META_ACCESS_TOKEN (ou META_TOKEN) no ambiente')

RD_CRM_TOKEN = os.environ.get('RD_CRM_TOKEN')
if not RD_CRM_TOKEN:
    raise SystemExit('ERRO: defina RD_CRM_TOKEN no ambiente')

RD_CLIENT_ID = os.environ.get('RD_CLIENT_ID')
RD_CLIENT_SECRET = os.environ.get('RD_CLIENT_SECRET')
RD_REFRESH_TOKEN = os.environ.get('RD_REFRESH_TOKEN')
if not all([RD_CLIENT_ID, RD_CLIENT_SECRET, RD_REFRESH_TOKEN]):
    raise SystemExit('ERRO: defina RD_CLIENT_ID, RD_CLIENT_SECRET e RD_REFRESH_TOKEN no ambiente')

ACCOUNT = 'act_844208497068966'
META_BASE = 'https://graph.facebook.com/v19.0'
RD_CRM_BASE = 'https://crm.rdstation.com/api/v1'
RD_MKT_BASE = 'https://api.rd.services'

# Stage IDs do CRM (Funil Padrao — 9 estagios)
STAGE_CONTATO = '69a1cc698603df001711f704'        # Contato feito
STAGE_FUP_POS = '69d3fe91adb03e0014314e14'        # FUP - Pos contato feito
STAGE_INTERESSE = '69a1cc698603df001711f705'       # Identificacao do interesse
STAGE_REUNIAO_MARCADA = '69a1cc698603df001711f706' # Reuniao Marcada
STAGE_REUNIAO_REALIZADA = '69a1cc698603df001711f707' # Reuniao Realizada
STAGE_FUP = '69c07d74a56e7c0016468e4f'            # FUP
STAGE_NEGOCIACAO = '69c07dc7f9379f0013c95a4b'      # Negociacao Fechada

# Stages que contam como "contato" (passou do "Sem contato")
STAGES_CONTATO = {STAGE_CONTATO, STAGE_FUP_POS, STAGE_INTERESSE,
                  STAGE_REUNIAO_MARCADA, STAGE_REUNIAO_REALIZADA,
                  STAGE_FUP, STAGE_NEGOCIACAO}
# Stages que contam como "reuniao"
STAGES_REUNIAO = {STAGE_REUNIAO_MARCADA, STAGE_REUNIAO_REALIZADA}
# Stages que contam como "pedido/negociacao"
STAGES_PEDIDO = {STAGE_NEGOCIACAO}

# Supabase (para leads qualificados)
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://dvvfnrdvhkjfovhfqiow.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_ANON_KEY') or os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')

# Meses alvo
MONTHS = [(2026, 2), (2026, 3), (2026, 4)]

# ──────────────────────────────────────────────────────────────
# DADOS HISTORICOS (Report Marketing e Vendas - Fev/Mar 2026)
# Fonte: Report_Marketing e Vendas_2026.pdf
# Estes dados sao fixos — o report ja foi fechado.
# Abril em diante e puxado via API automaticamente.
# ──────────────────────────────────────────────────────────────
HISTORICAL_COMMERCIAL = {
    # Fevereiro 2026 — dados do funil do report
    # Total mensal: Contatos 224, Reunioes 4, Vendas 0, FUP 65, SQL 124
    'Fev': {'contatos': 224, 'reunioes': 4, 'pedidos': 0, 'vendas': 0},
    # Marco 2026 — Comercial Março slide
    # Leads 888, Contato 214 (24%), Reunioes 11, Vendas 3, Pedidos 3
    'Mar': {'contatos': 214, 'reunioes': 11, 'pedidos': 3, 'vendas': 3},
}

HISTORICAL_QUALIFIED = {
    # Fevereiro: SQL 124 do funil (report nao detalha por semana)
    'Fev': 124,
    # Marco: ~76 leads qualificados (63 form nativo + 13 LP C4)
    'Mar': 76,
}

# Diretorio de saida
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), 'painel', 'metas-kpis')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def week_label(year, month, day):
    """Retorna label de semana: 'Fev W1', 'Mar W3', etc."""
    month_names = {1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
                   7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'}
    if day <= 7:
        w = 1
    elif day <= 14:
        w = 2
    elif day <= 21:
        w = 3
    else:
        w = 4
    return f"{month_names[month]} W{w}"


def all_week_labels():
    """Gera lista ordenada de todos os labels de semana para os meses alvo."""
    labels = []
    for year, month in MONTHS:
        for w in range(1, 5):
            month_names = {2: 'Fev', 3: 'Mar', 4: 'Abr'}
            labels.append(f"{month_names[month]} W{w}")
    return labels


def parse_date(s):
    """Parse ISO date string para (year, month, day)."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        return dt.year, dt.month, dt.day
    except Exception:
        try:
            dt = datetime.strptime(s[:10], '%Y-%m-%d')
            return dt.year, dt.month, dt.day
        except Exception:
            return None


# ──────────────────────────────────────────────────────────────
# META ADS API
# ──────────────────────────────────────────────────────────────
def meta_api(path, params=None):
    params = params or {}
    params['access_token'] = META_TOKEN
    url = f'{META_BASE}{path}?{urllib.parse.urlencode(params)}'
    try:
        with urllib.request.urlopen(url, context=ctx, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f'  Meta API err {path}: {e}', file=sys.stderr)
        return {}


def get_actions(actions, key):
    for a in (actions or []):
        if a.get('action_type') == key:
            return int(float(a.get('value', 0)))
    return 0


def fetch_meta_weekly():
    """Busca dados semanais do Meta Ads para cada mes alvo."""
    all_weeks = {}  # week_label -> {spend, impressions, clicks, leads, ctr}

    for year, month in MONTHS:
        first_day = datetime(year, month, 1)
        last_day = datetime(year, month, monthrange(year, month)[1])
        print(f'  Meta: buscando {first_day.strftime("%Y-%m")}...')

        r = meta_api(f'/{ACCOUNT}/insights', {
            'level': 'account',
            'fields': 'spend,impressions,clicks,ctr,actions',
            'time_increment': 7,
            'time_range': json.dumps({
                'since': first_day.strftime('%Y-%m-%d'),
                'until': last_day.strftime('%Y-%m-%d')
            }),
            'limit': 10
        })

        for w in r.get('data', []):
            ds = w.get('date_start', '')
            parsed = parse_date(ds)
            if not parsed:
                continue
            wy, wm, wd = parsed
            label = week_label(wy, wm, wd)

            actions = w.get('actions', [])
            leads = get_actions(actions, 'onsite_conversion.lead_grouped') or get_actions(actions, 'lead')
            spend = float(w.get('spend', 0))

            if label not in all_weeks:
                all_weeks[label] = {'spend': 0, 'impressions': 0, 'clicks': 0, 'leads': 0, 'ctr': 0}

            all_weeks[label]['spend'] += spend
            all_weeks[label]['impressions'] += int(w.get('impressions', 0))
            all_weeks[label]['clicks'] += int(w.get('clicks', 0))
            all_weeks[label]['leads'] += leads

        time.sleep(0.3)

    # Recalcular CTR
    for label, d in all_weeks.items():
        if d['impressions'] > 0:
            d['ctr'] = round(d['clicks'] / d['impressions'] * 100, 2)

    return all_weeks


# ──────────────────────────────────────────────────────────────
# RD STATION CRM API
# ──────────────────────────────────────────────────────────────
def crm_get(path, params=None):
    params = params or {}
    params['token'] = RD_CRM_TOKEN
    url = f'{RD_CRM_BASE}{path}?{urllib.parse.urlencode(params)}'
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f'  CRM API err {path}: {e}', file=sys.stderr)
        return {}


def fetch_crm_data_from_supabase():
    """Busca dados comerciais do Supabase (tabela deals, ja sincronizada do RD CRM).

    Contatos = deals cujo stage_name NAO eh 'Sem contato' e NAO eh 'Perdido ou Fora do ICP'
    Reunioes = deals cujo stage passou por Reuniao Marcada ou Realizada (stage_id in set)
    Pedidos = deals cujo stage eh Negociacao Fechada
    Vendas = deals com deal_status = 'won'
    """
    if not SUPABASE_KEY:
        print('  AVISO: SUPABASE_SERVICE_ROLE_KEY nao definida', file=sys.stderr)
        return {}

    weeks = {}
    for label in all_week_labels():
        weeks[label] = {'contatos': 0, 'reunioes': 0, 'pedidos': 0, 'vendas': 0}

    # Filtra so meses que NAO sao historicos (Abr em diante)
    valid_months = set((yr, mo) for yr, mo in MONTHS if (yr, mo) >= (2026, 4))

    def sb_fetch(extra_filter=''):
        base = f'{SUPABASE_URL}/rest/v1/deals'
        qs = f'select=crm_created_at,closed_at,stage_id,stage_name,deal_status{extra_filter}&limit=2000'
        url = f'{base}?{urllib.parse.quote(qs, safe="=&,.")}'
        req = urllib.request.Request(url, headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
        })
        with urllib.request.urlopen(req, context=ctx, timeout=20) as r:
            return json.loads(r.read())

    try:
        # Contatos: todos que passaram de "Sem contato"
        contatos = sb_fetch('&stage_name=neq.Sem contato&stage_name=neq.Perdido ou Fora do ICP')
        print(f'  Supabase: {len(contatos)} deals com contato feito')
        for d in contatos:
            p = parse_date(d.get('crm_created_at'))
            if p and (p[0], p[1]) in valid_months:
                lbl = week_label(*p)
                if lbl in weeks:
                    weeks[lbl]['contatos'] += 1

        # Reunioes: stage Reuniao Marcada ou Realizada
        for sid in [STAGE_REUNIAO_MARCADA, STAGE_REUNIAO_REALIZADA]:
            reunioes = sb_fetch(f'&stage_id=eq.{sid}')
            print(f'  Supabase: {len(reunioes)} deals em reuniao (stage {sid[-6:]})')
            for d in reunioes:
                p = parse_date(d.get('crm_created_at'))
                if p and (p[0], p[1]) in valid_months:
                    lbl = week_label(*p)
                    if lbl in weeks:
                        weeks[lbl]['reunioes'] += 1

        # Pedidos: Negociacao Fechada
        pedidos = sb_fetch(f'&stage_id=eq.{STAGE_NEGOCIACAO}')
        print(f'  Supabase: {len(pedidos)} deals em negociacao fechada')
        for d in pedidos:
            p = parse_date(d.get('crm_created_at'))
            if p and (p[0], p[1]) in valid_months:
                lbl = week_label(*p)
                if lbl in weeks:
                    weeks[lbl]['pedidos'] += 1

        # Vendas: deal_status = won
        vendas = sb_fetch('&deal_status=eq.won')
        print(f'  Supabase: {len(vendas)} vendas (won)')
        for d in vendas:
            date_field = d.get('closed_at') or d.get('crm_created_at')
            p = parse_date(date_field)
            if p and (p[0], p[1]) in valid_months:
                lbl = week_label(*p)
                if lbl in weeks:
                    weeks[lbl]['vendas'] += 1

    except Exception as e:
        print(f'  Supabase CRM err: {e}', file=sys.stderr)

    return weeks


# ──────────────────────────────────────────────────────────────
# RD STATION MARKETING API — Leads Qualificados
# ──────────────────────────────────────────────────────────────
def rd_mkt_get_token():
    """Renova access token via refresh_token."""
    payload = json.dumps({
        'client_id': RD_CLIENT_ID,
        'client_secret': RD_CLIENT_SECRET,
        'refresh_token': RD_REFRESH_TOKEN
    }).encode()
    req = urllib.request.Request(
        f'{RD_MKT_BASE}/auth/token',
        data=payload,
        headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
        data = json.loads(r.read())
        token = data.get('access_token')
        if not token:
            raise Exception(f'Token nao retornado: {data}')
        print('  RD Marketing token renovado OK')
        return token


def rd_mkt_get(path, token, params=None):
    params = params or {}
    qs = urllib.parse.urlencode(params)
    url = f'{RD_MKT_BASE}{path}{"?" + qs if qs else ""}'
    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    })
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=20) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f'  RD MKT API err {path}: {e}', file=sys.stderr)
        return {}


def fetch_contacts_by_event(token, event_identifier, max_pages=20):
    """Busca contatos que tiveram evento de conversao especifico."""
    contacts = []
    page = 1
    while page <= max_pages:
        r = rd_mkt_get('/platform/contacts', token, {
            'event_type': 'CONVERSION',
            'event_identifier': event_identifier,
            'page': page,
            'page_size': 50
        })
        items = r.get('contacts', [])
        if not items:
            break
        contacts.extend(items)
        total = r.get('total', 0)
        if len(contacts) >= total:
            break
        page += 1
        time.sleep(0.2)
    return contacts


def extract_cf(contact, field):
    """Extrai campo personalizado (cf_) do contato."""
    for cf in contact.get('cf', []) or []:
        if cf.get('custom_field', {}).get('api_identifier') == field:
            return cf.get('value', '')
    return ''


def is_qualified(contact):
    """Lead eh qualificado se patrimonio cripto >= R$50k.

    Verifica campos cf_* do contato, incluindo valores internos do Meta Lead Form.
    """
    QUALIFIED_KEYWORDS = ['50', '200', '500', '800']
    META_INTERNAL_VALUES = ['50k_200k', '200k_500k', 'acima_500k', 'acima_de_r_500_mil']

    # Checar todos os cf_* que possam conter patrimonio
    for cf in contact.get('cf', []) or []:
        api_id = cf.get('custom_field', {}).get('api_identifier', '')
        val = (cf.get('value', '') or '').strip().lower()
        if not val:
            continue
        # Campos de patrimonio conhecidos
        if 'patrimonio' in api_id or 'patrimonio_ho' in api_id or api_id == 'cf_que_otimo_agora_preciso_entender_qual_seu_patrimonio_ho':
            if any(x in val for x in QUALIFIED_KEYWORDS):
                return True
            if any(x in val for x in META_INTERNAL_VALUES):
                return True
    return False


# ──────────────────────────────────────────────────────────────
# QUALIFIED LEADS CACHE (para evitar re-consultar RD API)
# ──────────────────────────────────────────────────────────────
CACHE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'painel', 'metas-kpis', 'qualified_cache.json'
)


def load_qualified_cache():
    """Carrega cache de leads ja verificados via RD API."""
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_qualified_cache(cache):
    """Salva cache de leads verificados."""
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def check_lead_via_rd_api(email, token):
    """Verifica se um lead eh qualificado consultando RD Marketing API.

    1. Busca contato por email
    2. Checa campos cf_* do contato
    3. Busca eventos de conversao e checa payload

    Returns: True se qualificado (patrimonio >= R$50k), False caso contrario.
    """
    QUALIFIED_KEYWORDS = ['50', '200', '500', '800']
    META_INTERNAL_VALUES = ['50k_200k', '200k_500k', 'acima_500k', 'acima_de_r_500_mil']

    # 1. Buscar contato por email
    contact = rd_mkt_get(f'/platform/contacts/email:{urllib.parse.quote(email)}', token)
    if not contact or not contact.get('uuid'):
        return False

    # 2. Checar cf_* do contato
    if is_qualified(contact):
        return True

    # 3. Buscar eventos de conversao
    uuid = contact['uuid']
    events = rd_mkt_get(f'/platform/contacts/{uuid}/events', token, {
        'event_type': 'CONVERSION'
    })

    for event in events.get('events', []) if isinstance(events, dict) else []:
        # Checar event_identifier
        ev_id = (event.get('event_identifier', '') or '').lower()
        # Checar payload/metadata por indicadores de patrimonio
        payload = event.get('payload', {}) or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}

        # Percorrer todos os valores do payload buscando indicadores de patrimonio
        for key, val in payload.items():
            val_lower = str(val).lower()
            if 'patrimonio' in key.lower() or 'patrimonio' in val_lower:
                if any(x in val_lower for x in QUALIFIED_KEYWORDS):
                    return True
                if any(x in val_lower for x in META_INTERNAL_VALUES):
                    return True

    return False


def enrich_leads_without_patrimonio(leads_without_pat, token):
    """Para leads sem patrimonio no Supabase, consulta RD API para verificar qualificacao.

    Args:
        leads_without_pat: lista de dicts com 'email' e 'created_at'
        token: RD Marketing API token

    Returns:
        lista de leads qualificados (com created_at para distribuicao por semana)
    """
    cache = load_qualified_cache()
    qualified = []
    checked_count = 0
    cached_count = 0
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    for lead in leads_without_pat:
        email = (lead.get('email') or '').strip().lower()
        if not email:
            continue

        # Checar cache
        if email in cache:
            cached_count += 1
            if cache[email].get('qualified'):
                qualified.append(lead)
            continue

        # Consultar RD API
        try:
            is_qual = check_lead_via_rd_api(email, token)
            cache[email] = {'qualified': is_qual, 'checked_at': today}
            if is_qual:
                qualified.append(lead)
            checked_count += 1
            time.sleep(0.2)  # Rate limit
        except Exception as e:
            print(f'    RD API err para {email[:20]}...: {e}', file=sys.stderr)
            cache[email] = {'qualified': False, 'checked_at': today}
            checked_count += 1
            time.sleep(0.2)

    # Salvar cache atualizado
    save_qualified_cache(cache)
    print(f'    RD API: {checked_count} consultados, {cached_count} do cache, {len(qualified)} qualificados')
    return qualified


def fetch_qualified_leads_by_week(token):
    """Busca leads qualificados via RD Marketing + fallback Supabase."""
    weeks = {}
    for label in all_week_labels():
        weeks[label] = 0

    # Tentar RD Marketing primeiro
    rd_total = 0
    events = ['lp_mentoria_boost', 'lp_ir_cripto']
    for event in events:
        print(f'  RD MKT: buscando {event}...')
        contacts = fetch_contacts_by_event(token, event)
        print(f'    {len(contacts)} contatos')
        for c in contacts:
            if not is_qualified(c):
                continue
            created = c.get('created_at')
            parsed = parse_date(created)
            if not parsed:
                continue
            y, m, d = parsed
            if (y, m) not in [(yr, mo) for yr, mo in MONTHS]:
                continue
            label = week_label(y, m, d)
            if label in weeks:
                weeks[label] += 1
                rd_total += 1

    if rd_total > 0:
        return weeks

    # Fallback: Supabase (tabela leads com patrimonio_cripto_min_k >= 50)
    print('  RD MKT retornou 0 — usando Supabase como fallback...')
    if not SUPABASE_KEY:
        print('  AVISO: SUPABASE_SERVICE_ROLE_KEY nao definida, pulando', file=sys.stderr)
        return weeks

    try:
        for year, month in MONTHS:
            first = f'{year}-{month:02d}-01'
            last_day = monthrange(year, month)[1]
            last = f'{year}-{month:02d}-{last_day}'

            url = (f'{SUPABASE_URL}/rest/v1/leads'
                   f'?select=created_at,patrimonio_cripto_min_k'
                   f'&patrimonio_cripto_min_k=gte.50'
                   f'&created_at=gte.{first}T00:00:00'
                   f'&created_at=lte.{last}T23:59:59'
                   f'&limit=1000')

            req = urllib.request.Request(url, headers={
                'apikey': SUPABASE_KEY,
                'Authorization': f'Bearer {SUPABASE_KEY}',
            })
            with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
                leads = json.loads(r.read())
                for lead in leads:
                    parsed = parse_date(lead.get('created_at'))
                    if not parsed:
                        continue
                    y, m, d = parsed
                    lbl = week_label(y, m, d)
                    if lbl in weeks:
                        weeks[lbl] += 1
                print(f'  Supabase {first[:7]}: {len(leads)} leads qualificados')

    except Exception as e:
        print(f'  Supabase err: {e}', file=sys.stderr)

    return weeks


# ──────────────────────────────────────────────────────────────
# BTC PRICE
# ──────────────────────────────────────────────────────────────
def fetch_btc_price():
    """Busca preco atual do BTC via CoinGecko."""
    url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=brl,usd'
    try:
        with urllib.request.urlopen(url, context=ctx, timeout=10) as r:
            data = json.loads(r.read())
            return {
                'brl': data.get('bitcoin', {}).get('brl', 0),
                'usd': data.get('bitcoin', {}).get('usd', 0)
            }
    except Exception as e:
        print(f'  BTC price err: {e}', file=sys.stderr)
        return {'brl': 0, 'usd': 0}


# ──────────────────────────────────────────────────────────────
# HTML GENERATION
# ──────────────────────────────────────────────────────────────
def generate_html(data):
    """Gera HTML self-contained do dashboard."""
    week_labels = all_week_labels()
    meta = data.get('meta_weekly', {})
    crm = data.get('crm_weekly', {})
    qual = data.get('qualified_weekly', {})
    btc = data.get('btc', {})
    updated = data.get('updated_at', '')

    # Calcular totais
    total_spend = sum(meta.get(w, {}).get('spend', 0) for w in week_labels)
    total_leads = sum(meta.get(w, {}).get('leads', 0) for w in week_labels)
    total_qual = sum(qual.get(w, 0) for w in week_labels)
    total_contatos = sum(crm.get(w, {}).get('contatos', 0) for w in week_labels)
    total_reunioes = sum(crm.get(w, {}).get('reunioes', 0) for w in week_labels)
    total_vendas = sum(crm.get(w, {}).get('vendas', 0) for w in week_labels)
    avg_cpl = round(total_spend / total_leads, 2) if total_leads else 0

    # Build table rows
    def fmt_brl(v):
        if v == 0:
            return '—'
        return f"R$ {v:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

    def fmt_int(v):
        if v == 0:
            return '—'
        return f"{v:,}".replace(',', '.')

    def fmt_pct(v):
        if v == 0:
            return '—'
        return f"{v:.2f}%".replace('.', ',')

    def build_row(label, values, formatter, css_class=''):
        cells = f'<td class="metric-name {css_class}">{label}</td>'
        for w in week_labels:
            val = values.get(w, 0)
            cells += f'<td class="{css_class}">{formatter(val)}</td>'
        return f'<tr>{cells}</tr>'

    # Prepare data dicts for each row
    spend_data = {w: meta.get(w, {}).get('spend', 0) for w in week_labels}
    impressions_data = {w: meta.get(w, {}).get('impressions', 0) for w in week_labels}
    clicks_data = {w: meta.get(w, {}).get('clicks', 0) for w in week_labels}
    leads_data = {w: meta.get(w, {}).get('leads', 0) for w in week_labels}
    qual_data = {w: qual.get(w, 0) for w in week_labels}
    ctr_data = {w: meta.get(w, {}).get('ctr', 0) for w in week_labels}
    cpl_data = {}
    for w in week_labels:
        s = meta.get(w, {}).get('spend', 0)
        l = meta.get(w, {}).get('leads', 0)
        cpl_data[w] = round(s / l, 2) if l else 0

    contatos_data = {w: crm.get(w, {}).get('contatos', 0) for w in week_labels}
    reunioes_data = {w: crm.get(w, {}).get('reunioes', 0) for w in week_labels}
    pedidos_data = {w: crm.get(w, {}).get('pedidos', 0) for w in week_labels}
    vendas_data = {w: crm.get(w, {}).get('vendas', 0) for w in week_labels}

    table_rows = '\n'.join([
        build_row('Investimento', spend_data, fmt_brl),
        build_row('Impressoes', impressions_data, fmt_int),
        build_row('Cliques', clicks_data, fmt_int),
        build_row('Leads', leads_data, fmt_int),
        build_row('Leads Qualificados', qual_data, fmt_int),
        build_row('CTR', ctr_data, fmt_pct),
        build_row('CPL', cpl_data, fmt_brl),
        build_row('Contatos', contatos_data, fmt_int, 'commercial'),
        build_row('Reunioes', reunioes_data, fmt_int, 'commercial'),
        build_row('Pedidos', pedidos_data, fmt_int, 'commercial'),
        build_row('Vendas', vendas_data, fmt_int, 'commercial'),
    ])

    week_headers = ''.join(f'<th>{w}</th>' for w in week_labels)

    # Month summary cards
    month_cards_html = ''
    for year, month in MONTHS:
        month_names = {2: 'Fevereiro', 3: 'Marco', 4: 'Abril'}
        mname = month_names[month]
        month_short = {2: 'Fev', 3: 'Mar', 4: 'Abr'}[month]
        m_weeks = [f"{month_short} W{i}" for i in range(1, 5)]
        m_spend = sum(meta.get(w, {}).get('spend', 0) for w in m_weeks)
        m_leads = sum(meta.get(w, {}).get('leads', 0) for w in m_weeks)
        m_qual = sum(qual.get(w, 0) for w in m_weeks)
        m_clicks = sum(meta.get(w, {}).get('clicks', 0) for w in m_weeks)
        m_impressions = sum(meta.get(w, {}).get('impressions', 0) for w in m_weeks)
        m_cpl = round(m_spend / m_leads, 2) if m_leads else 0
        m_ctr = round(m_clicks / m_impressions * 100, 2) if m_impressions else 0
        m_contatos = sum(crm.get(w, {}).get('contatos', 0) for w in m_weeks)
        m_reunioes = sum(crm.get(w, {}).get('reunioes', 0) for w in m_weeks)
        m_vendas = sum(crm.get(w, {}).get('vendas', 0) for w in m_weeks)

        month_cards_html += f'''
        <div class="month-card">
            <h3>{mname} {year}</h3>
            <div class="month-grid">
                <div class="month-stat"><span class="month-label">Investimento</span><span class="month-value">{fmt_brl(m_spend)}</span></div>
                <div class="month-stat"><span class="month-label">Leads</span><span class="month-value">{fmt_int(m_leads)}</span></div>
                <div class="month-stat"><span class="month-label">Leads Qualif.</span><span class="month-value">{fmt_int(m_qual)}</span></div>
                <div class="month-stat"><span class="month-label">CPL</span><span class="month-value">{fmt_brl(m_cpl)}</span></div>
                <div class="month-stat"><span class="month-label">CTR</span><span class="month-value">{fmt_pct(m_ctr)}</span></div>
                <div class="month-stat"><span class="month-label">Cliques</span><span class="month-value">{fmt_int(m_clicks)}</span></div>
                <div class="month-stat"><span class="month-label">Contatos</span><span class="month-value">{fmt_int(m_contatos)}</span></div>
                <div class="month-stat"><span class="month-label">Reunioes</span><span class="month-value">{fmt_int(m_reunioes)}</span></div>
                <div class="month-stat"><span class="month-label">Vendas</span><span class="month-value">{fmt_int(m_vendas)}</span></div>
            </div>
        </div>'''

    # Funil de conversao acumulado
    conv_lead_cont = round(total_contatos / total_leads * 100, 1) if total_leads else 0
    conv_cont_qual = round(total_qual / total_contatos * 100, 1) if total_contatos else 0
    conv_qual_reun = round(total_reunioes / total_qual * 100, 1) if total_qual else 0
    total_pedidos = sum(crm.get(w, {}).get('pedidos', 0) for w in week_labels)
    conv_reun_vend = round(total_vendas / total_reunioes * 100, 1) if total_reunioes else 0
    conv_total = round(total_vendas / total_leads * 100, 2) if total_leads else 0

    funnel_html = f'''
<div class="funnel-title">Funil de Conversao Acumulado</div>
<div class="funnel-section">
    <div class="funnel-container">
        <div class="vfunnel">
            <div class="vfunnel-stage">
                <div class="vfunnel-bar" style="width: 100%; background: linear-gradient(90deg, #00B37E, #00D195);">
                    <span class="vfunnel-label">Leads</span>
                    <span class="vfunnel-value">{fmt_int(total_leads)}</span>
                </div>
            </div>
            <div class="vfunnel-conv-row"><span class="vfunnel-pct">&#8595; {conv_lead_cont}% foram contatados</span></div>
            <div class="vfunnel-stage">
                <div class="vfunnel-bar" style="width: 75%; background: linear-gradient(90deg, #9B59B6, #C084FC);">
                    <span class="vfunnel-label">Contatos</span>
                    <span class="vfunnel-value">{fmt_int(total_contatos)}</span>
                </div>
            </div>
            <div class="vfunnel-conv-row"><span class="vfunnel-pct">&#8595; {conv_cont_qual}% sao qualificados</span></div>
            <div class="vfunnel-stage">
                <div class="vfunnel-bar" style="width: 55%; background: linear-gradient(90deg, #0090c0, #00b4d8);">
                    <span class="vfunnel-label">Leads Qualificados</span>
                    <span class="vfunnel-value">{fmt_int(total_qual)}</span>
                </div>
            </div>
            <div class="vfunnel-conv-row"><span class="vfunnel-pct">&#8595; {conv_qual_reun}% tiveram reuniao</span></div>
            <div class="vfunnel-stage">
                <div class="vfunnel-bar" style="width: 30%; background: linear-gradient(90deg, #E08A00, #FFA500);">
                    <span class="vfunnel-label">Reunioes</span>
                    <span class="vfunnel-value">{fmt_int(total_reunioes)}</span>
                </div>
            </div>
            <div class="vfunnel-conv-row"><span class="vfunnel-pct">&#8595; {conv_reun_vend}% fecharam</span></div>
            <div class="vfunnel-stage">
                <div class="vfunnel-bar" style="width: 18%; background: linear-gradient(90deg, #D94E7A, #FF6B9D);">
                    <span class="vfunnel-label">Vendas</span>
                    <span class="vfunnel-value">{fmt_int(total_vendas)}</span>
                </div>
            </div>
            <div class="vfunnel-bottom">
                <span>Taxa geral: <strong style="color:#00D195">{conv_total}%</strong> (leads &rarr; vendas)</span>
            </div>
        </div>
    </div>
</div>'''

    btc_brl = fmt_brl(btc.get('brl', 0)) if btc.get('brl') else '—'
    btc_usd = f"US$ {btc.get('usd', 0):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if btc.get('usd') else '—'

    html = f'''<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Metas e KPIs — Boost Research</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: 'Inter', sans-serif;
    background: #000;
    color: #FFF;
    min-height: 100vh;
    padding: 24px;
}}

.header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 32px;
    flex-wrap: wrap;
    gap: 16px;
}}
.header h1 {{
    font-size: 28px;
    font-weight: 700;
    color: #00D195;
}}
.header .meta {{
    display: flex;
    align-items: center;
    gap: 16px;
    color: #A0A0C0;
    font-size: 13px;
}}
.header .meta .btc {{
    background: #0A0A14;
    padding: 6px 14px;
    border-radius: 8px;
    border: 1px solid #1a1a2e;
    font-weight: 500;
    color: #FFA500;
}}
.btn-refresh {{
    background: #00B37E;
    color: #000;
    border: none;
    padding: 8px 20px;
    border-radius: 8px;
    font-weight: 600;
    cursor: pointer;
    font-size: 13px;
    font-family: 'Inter', sans-serif;
}}
.btn-refresh:hover {{ background: #00D195; }}

/* Summary cards */
.summary-row {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 16px;
    margin-bottom: 32px;
}}
.summary-card {{
    background: #0A0A14;
    border: 1px solid #1a1a2e;
    border-radius: 12px;
    padding: 20px;
    text-align: center;
}}
.summary-card .label {{
    font-size: 12px;
    color: #A0A0C0;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
}}
.summary-card .value {{
    font-size: 22px;
    font-weight: 700;
    color: #00D195;
}}
.summary-card .value.accent {{ color: #FFA500; }}
.summary-card .value.pink {{ color: #FF6B9D; }}

/* WoW Table */
.table-container {{
    overflow-x: auto;
    margin-bottom: 40px;
    border-radius: 12px;
    border: 1px solid #1a1a2e;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    min-width: 900px;
}}
th {{
    background: #0A0A14;
    color: #A0A0C0;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    padding: 14px 12px;
    text-align: right;
    border-bottom: 2px solid #1a1a2e;
    white-space: nowrap;
}}
th:first-child {{ text-align: left; }}
td {{
    padding: 12px;
    font-size: 14px;
    text-align: right;
    border-bottom: 1px solid #0d0d1a;
    color: #E0E0F0;
}}
td.metric-name {{
    text-align: left;
    font-weight: 600;
    color: #FFF;
    white-space: nowrap;
    position: sticky;
    left: 0;
    background: #000;
    z-index: 1;
}}
tr:hover td {{ background: #0A0A14; }}
tr:hover td.metric-name {{ background: #0A0A14; }}

/* Commercial rows */
td.commercial {{
    background: rgba(255, 107, 157, 0.06);
}}
td.metric-name.commercial {{
    background: rgba(255, 107, 157, 0.06);
    color: #FF6B9D;
}}
tr:hover td.commercial {{ background: rgba(255, 107, 157, 0.12); }}
tr:hover td.metric-name.commercial {{ background: rgba(255, 107, 157, 0.12); }}

/* Month cards */
.month-section {{
    margin-bottom: 40px;
}}
.month-section h2 {{
    font-size: 20px;
    margin-bottom: 20px;
    color: #A0A0C0;
    font-weight: 500;
}}
.month-cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 20px;
}}
.month-card {{
    background: #0A0A14;
    border: 1px solid #1a1a2e;
    border-radius: 12px;
    padding: 24px;
}}
.month-card h3 {{
    font-size: 18px;
    font-weight: 700;
    color: #00D195;
    margin-bottom: 16px;
    padding-bottom: 12px;
    border-bottom: 1px solid #1a1a2e;
}}
.month-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
}}
.month-stat {{
    display: flex;
    flex-direction: column;
    gap: 4px;
}}
.month-label {{
    font-size: 11px;
    color: #A0A0C0;
    text-transform: uppercase;
}}
.month-value {{
    font-size: 15px;
    font-weight: 600;
    color: #E0E0F0;
}}

/* Funil de Conversao */
.funnel-section {{ margin-bottom: 40px; }}
.funnel-title {{
    font-size: 13px; font-weight: 600; color: #A0A0C0;
    text-transform: uppercase; letter-spacing: 1px; margin-bottom: 16px;
    display: flex; align-items: center; gap: 10px;
}}
.funnel-title::after {{ content: ''; flex: 1; height: 1px; background: linear-gradient(to right, #1a1a2e, transparent); }}
.funnel-container {{
    background: #0A0A14; border: 1px solid #1a1a2e;
    border-radius: 12px; padding: 32px 48px;
}}
.vfunnel {{ display: flex; flex-direction: column; align-items: center; gap: 0; max-width: 800px; margin: 0 auto; }}
.vfunnel-stage {{ width: 100%; display: flex; justify-content: center; }}
.vfunnel-bar {{
    height: 52px; border-radius: 8px; display: flex; align-items: center;
    justify-content: space-between; padding: 0 24px; font-weight: 700;
    font-size: 16px; color: #FFF; font-variant-numeric: tabular-nums;
    transition: opacity 0.3s; min-width: 160px;
}}
.vfunnel-bar:hover {{ opacity: 0.85; }}
.vfunnel-label {{ font-size: 13px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }}
.vfunnel-value {{ font-size: 20px; font-weight: 800; }}
.vfunnel-conv-row {{ padding: 6px 0; display: flex; justify-content: center; }}
.vfunnel-pct {{
    font-size: 11px; font-weight: 500; color: #A0A0C0;
    background: rgba(160, 160, 192, 0.06); padding: 3px 12px; border-radius: 4px;
}}
.vfunnel-bottom {{
    margin-top: 20px; padding-top: 16px; border-top: 1px solid #1a1a2e;
    display: flex; justify-content: center; gap: 40px;
    font-size: 13px; color: #A0A0C0; width: 100%;
}}

.footer {{
    text-align: center;
    color: #555;
    font-size: 12px;
    margin-top: 40px;
    padding-top: 20px;
    border-top: 1px solid #1a1a2e;
}}
</style>
</head>
<body>

<div class="header">
    <h1>Metas e KPIs &mdash; Boost Research</h1>
    <div class="meta">
        <span class="btc">BTC: {btc_brl} / {btc_usd}</span>
        <span>Atualizado: {updated}</span>
        <button class="btn-refresh" onclick="location.reload()">Atualizar</button>
    </div>
</div>

<div class="summary-row">
    <div class="summary-card">
        <div class="label">Investimento Total</div>
        <div class="value accent">{fmt_brl(total_spend)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Total Leads</div>
        <div class="value">{fmt_int(total_leads)}</div>
    </div>
    <div class="summary-card">
        <div class="label">CPL Medio</div>
        <div class="value accent">{fmt_brl(avg_cpl)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Leads Qualificados</div>
        <div class="value">{fmt_int(total_qual)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Contatos</div>
        <div class="value pink">{fmt_int(total_contatos)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Reunioes</div>
        <div class="value pink">{fmt_int(total_reunioes)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Vendas</div>
        <div class="value pink">{fmt_int(total_vendas)}</div>
    </div>
</div>

<div class="table-container">
<table>
<thead>
<tr>
    <th>Metrica</th>
    {week_headers}
</tr>
</thead>
<tbody>
{table_rows}
</tbody>
</table>
</div>

<div class="month-section">
    <h2>Resumo Mensal</h2>
    <div class="month-cards">
        {month_cards_html}
    </div>
</div>

{funnel_html}

<div class="footer">
    Boost Research &mdash; Dashboard automatizado via AIOX
    &nbsp;|&nbsp; Proxima atualizacao: Segunda 23:59
</div>

</body>
</html>'''
    return html


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
def main():
    os.makedirs(BASE_DIR, exist_ok=True)
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    data = {
        'updated_at': now,
        'meta_weekly': {},
        'crm_weekly': {},
        'qualified_weekly': {},
        'btc': {},
    }

    # 1. Meta Ads
    print('=' * 60)
    print('1/4 — Meta Ads (dados semanais)')
    print('=' * 60)
    try:
        data['meta_weekly'] = fetch_meta_weekly()
        print(f'  OK: {len(data["meta_weekly"])} semanas com dados')
    except Exception as e:
        print(f'  FALHA Meta Ads: {e}', file=sys.stderr)

    # 2. CRM — Historico (Fev/Mar do report) + Abril via Supabase
    print()
    print('=' * 60)
    print('2/4 — CRM: historico Fev/Mar + Supabase Abr+')
    print('=' * 60)

    # Inicializar todas as semanas
    crm_weekly = {}
    for label in all_week_labels():
        crm_weekly[label] = {'contatos': 0, 'reunioes': 0, 'pedidos': 0, 'vendas': 0}

    # Distribuir dados historicos do report pelas semanas do mes
    for month_short, totals in HISTORICAL_COMMERCIAL.items():
        month_weeks = [f"{month_short} W{i}" for i in range(1, 5)]
        # Distribuir proporcionalmente ao numero de leads Meta por semana
        meta_leads_per_week = [data['meta_weekly'].get(w, {}).get('leads', 1) for w in month_weeks]
        total_meta_leads = sum(meta_leads_per_week) or 1

        for metric in ['contatos', 'reunioes', 'pedidos', 'vendas']:
            total_val = totals[metric]
            if total_val == 0:
                continue
            # Distribuir proporcional, arredondando e ajustando o resto
            distributed = [int(total_val * (ml / total_meta_leads)) for ml in meta_leads_per_week]
            remainder = total_val - sum(distributed)
            # Dar o resto pra semana com mais leads
            if remainder > 0:
                max_idx = meta_leads_per_week.index(max(meta_leads_per_week))
                distributed[max_idx] += remainder
            for i, w in enumerate(month_weeks):
                crm_weekly[w][metric] = distributed[i]

        print(f'  {month_short}: historico do report — {totals}')

    # Abril: buscar via Supabase
    try:
        abr_crm = fetch_crm_data_from_supabase()
        for w in ['Abr W1', 'Abr W2', 'Abr W3', 'Abr W4']:
            if w in abr_crm:
                crm_weekly[w] = abr_crm[w]
        abr_c = sum(abr_crm.get(f'Abr W{i}', {}).get('contatos', 0) for i in range(1, 5))
        abr_r = sum(abr_crm.get(f'Abr W{i}', {}).get('reunioes', 0) for i in range(1, 5))
        abr_v = sum(abr_crm.get(f'Abr W{i}', {}).get('vendas', 0) for i in range(1, 5))
        print(f'  Abr: Supabase — {abr_c} contatos, {abr_r} reunioes, {abr_v} vendas')
    except Exception as e:
        print(f'  FALHA Supabase Abr: {e}', file=sys.stderr)

    data['crm_weekly'] = crm_weekly

    # 3. Leads Qualificados — Historico + Supabase
    print()
    print('=' * 60)
    print('3/4 — Leads qualificados: historico + Supabase + RD API enrichment')
    print('=' * 60)

    qual_weekly = {}
    for label in all_week_labels():
        qual_weekly[label] = 0

    # Distribuir historico pelas semanas
    for month_short, total_q in HISTORICAL_QUALIFIED.items():
        month_weeks = [f"{month_short} W{i}" for i in range(1, 5)]
        meta_leads_per_week = [data['meta_weekly'].get(w, {}).get('leads', 1) for w in month_weeks]
        total_meta_leads = sum(meta_leads_per_week) or 1
        distributed = [int(total_q * (ml / total_meta_leads)) for ml in meta_leads_per_week]
        remainder = total_q - sum(distributed)
        if remainder > 0:
            max_idx = meta_leads_per_week.index(max(meta_leads_per_week))
            distributed[max_idx] += remainder
        for i, w in enumerate(month_weeks):
            qual_weekly[w] = distributed[i]
        print(f'  {month_short}: historico — {total_q} qualificados')

    # Abril+: RD Station Segmentacoes (fonte mais precisa — inclui Lead Form)
    # Segmentacoes criadas no RD:
    #   19356678 = Investidor R$50k-500k
    #   19356688 = Investidor Acima R$500k
    RD_SEG_IDS = [19356678, 19356688]

    try:
        rd_mkt_token = rd_mkt_get_token()
        all_qual_emails = {}  # email -> created_at (deduplicar)

        for seg_id in RD_SEG_IDS:
            page = 1
            seg_contacts = []
            while page <= 30:
                r = rd_mkt_get(f'/platform/segmentations/{seg_id}/contacts', rd_mkt_token, {
                    'page': page, 'page_size': 125
                })
                items = r.get('contacts', []) if isinstance(r, dict) else (r if isinstance(r, list) else [])
                if not items:
                    break
                seg_contacts.extend(items)
                total = r.get('total', 0) if isinstance(r, dict) else 0
                if len(seg_contacts) >= total or len(items) < 125:
                    break
                page += 1
                time.sleep(0.2)

            for c in seg_contacts:
                email = (c.get('email') or '').strip().lower()
                created = c.get('created_at', '')
                if email and created:
                    all_qual_emails[email] = created
            print(f'  RD Segmentacao {seg_id}: {len(seg_contacts)} leads')

        # Distribuir por semana (so meses nao-historicos = Abr+)
        valid_months_qual = set((yr, mo) for yr, mo in MONTHS if (yr, mo) >= (2026, 4))
        rd_count = 0
        for email, created in all_qual_emails.items():
            parsed = parse_date(created)
            if parsed and (parsed[0], parsed[1]) in valid_months_qual:
                lbl = week_label(*parsed)
                if lbl in qual_weekly:
                    qual_weekly[lbl] += 1
                    rd_count += 1

        print(f'  RD Segmentacoes total (dedup): {len(all_qual_emails)} | Abr+: {rd_count} qualificados')

    except Exception as e:
        print(f'  FALHA RD Segmentacoes: {e}', file=sys.stderr)
        print(f'  Fallback: usando Supabase para Abr+...')
        # Fallback: Supabase
        try:
            for year, month in MONTHS:
                if (year, month) < (2026, 4):
                    continue
                first = f'{year}-{month:02d}-01'
                last_day = monthrange(year, month)[1]
                last = f'{year}-{month:02d}-{last_day}'
                url = (f'{SUPABASE_URL}/rest/v1/leads'
                       f'?select=created_at,patrimonio_cripto_min_k'
                       f'&patrimonio_cripto_min_k=gte.50'
                       f'&created_at=gte.{first}T00:00:00'
                       f'&created_at=lte.{last}T23:59:59'
                       f'&limit=1000')
                req = urllib.request.Request(url, headers={
                    'apikey': SUPABASE_KEY,
                    'Authorization': f'Bearer {SUPABASE_KEY}',
                })
                with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
                    leads = json.loads(r.read())
                for lead in leads:
                    parsed = parse_date(lead.get('created_at'))
                    if parsed:
                        lbl = week_label(*parsed)
                        if lbl in qual_weekly:
                            qual_weekly[lbl] += 1
        except Exception as e2:
            print(f'  FALHA Supabase fallback: {e2}', file=sys.stderr)

    data['qualified_weekly'] = qual_weekly
    total_q = sum(qual_weekly.values())
    print(f'  Total: {total_q} leads qualificados no periodo')

    # 4. BTC
    print()
    print('=' * 60)
    print('4/4 — Cotacao BTC')
    print('=' * 60)
    data['btc'] = fetch_btc_price()
    if data['btc'].get('brl'):
        print(f'  BTC: R$ {data["btc"]["brl"]:,.2f} / US$ {data["btc"]["usd"]:,.2f}')
    else:
        print('  BTC: indisponivel')

    # Salvar data.json
    data_path = os.path.join(BASE_DIR, 'data.json')
    with open(data_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f'\ndata.json salvo em {data_path}')

    # Gerar HTML
    html = generate_html(data)
    html_path = os.path.join(BASE_DIR, 'index.html')
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'index.html salvo em {html_path}')

    print()
    print('=' * 60)
    print('CONCLUIDO')
    print('=' * 60)


if __name__ == '__main__':
    main()
