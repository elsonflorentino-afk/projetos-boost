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

# ── Stage IDs — TODOS os funis do CRM ──
# Funil Padrao
FP_SEM_CONTATO     = '69a1cc698603df001711f703'
FP_CONTATO         = '69a1cc698603df001711f704'
FP_FUP_POS         = '69d3fe91adb03e0014314e14'
FP_INTERESSE       = '69a1cc698603df001711f705'
FP_REUNIAO_MARCADA = '69a1cc698603df001711f706'
FP_REUNIAO_REALIZ  = '69a1cc698603df001711f707'
FP_FUP             = '69c07d74a56e7c0016468e4f'
FP_NEGOCIACAO      = '69c07dc7f9379f0013c95a4b'
FP_PERDIDO         = '69c52c04eea19c0019a5ee00'

# Prospeccao Ativa
PA_SEM_CONTATO  = '69c52ba2beeb7600166918d6'
PA_CONTATO      = '69c52ba2beeb7600166918d7'
PA_INTERESSE    = '69c52ba2beeb7600166918d8'
PA_REUNIAO_FUP  = '69c52ba3beeb7600166918d9'
PA_PROPOSTA     = '69c52ba3beeb7600166918da'
PA_NEGOCIACAO   = '69c6d204fb863000132cc68f'

# Clientes Renovacao
CR_SEM_CONTATO  = '69c690bd795a9b00168be39e'
CR_CONTATO      = '69c690bd795a9b00168be39f'
CR_INTERESSE    = '69c690bd795a9b00168be3a0'
CR_APRESENTACAO = '69c690bd795a9b00168be3a1'
CR_PROPOSTA     = '69c690bd795a9b00168be3a2'
CR_NEGOCIACAO   = '69cd2cf1d3062c00161ab229'
CR_PERDIDO      = '69d92716729c3f00173ce848'

# Cryptostart Upsell
CU_SEM_CONTATO  = '69de8e03eb0b02001791249a'
CU_CONTATO      = '69de8e03eb0b02001791249b'
CU_INTERESSE    = '69de8e03eb0b02001791249c'
CU_APRESENTACAO = '69de8e03eb0b02001791249d'
CU_PROPOSTA     = '69de8e03eb0b02001791249e'

# ── Stages "Sem contato" e "Perdido" (excluir de contatos) ──
STAGES_EXCLUIR = {
    FP_SEM_CONTATO, FP_PERDIDO,
    PA_SEM_CONTATO,
    CR_SEM_CONTATO, CR_PERDIDO,
    CU_SEM_CONTATO,
}

# ── Stages que contam como "contato" (todos EXCETO sem contato e perdido) ──
STAGES_CONTATO = {
    FP_CONTATO, FP_FUP_POS, FP_INTERESSE, FP_REUNIAO_MARCADA,
    FP_REUNIAO_REALIZ, FP_FUP, FP_NEGOCIACAO,
    PA_CONTATO, PA_INTERESSE, PA_REUNIAO_FUP, PA_PROPOSTA, PA_NEGOCIACAO,
    CR_CONTATO, CR_INTERESSE, CR_APRESENTACAO, CR_PROPOSTA, CR_NEGOCIACAO,
    CU_CONTATO, CU_INTERESSE, CU_APRESENTACAO, CU_PROPOSTA,
}

# ── Stages que contam como "reuniao" ──
# Inclui: Reuniao Marcada, Reuniao Realizada, Apresentacao, Proposta Enviada
STAGES_REUNIAO = {
    FP_REUNIAO_MARCADA, FP_REUNIAO_REALIZ,
    PA_REUNIAO_FUP, PA_PROPOSTA,
    CR_APRESENTACAO, CR_PROPOSTA,
    CU_APRESENTACAO, CU_PROPOSTA,
}

# ── Stages que contam como "pedido/negociacao" ──
STAGES_PEDIDO = {FP_NEGOCIACAO, PA_NEGOCIACAO, CR_NEGOCIACAO}

# Pipeline IDs (para buscar deals de todos os funis)
PIPELINE_IDS = [
    '69a1cc698603df001711f701',  # Funil Padrao
    '69c52ba2beeb7600166918d4',  # Prospeccao Ativa
    '69c690bd795a9b00168be39c',  # Clientes Renovacao
    '69de8e03eb0b020017912498',  # Cryptostart Upsell
]

# Backward compat — variaveis usadas em fetch_crm_data_from_supabase
STAGE_REUNIAO_MARCADA = FP_REUNIAO_MARCADA
STAGE_REUNIAO_REALIZADA = FP_REUNIAO_REALIZ
STAGE_NEGOCIACAO = FP_NEGOCIACAO

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
# ── Dados historicos do Report Midia & Vendas Marco 2026 (PDF) ──
# TODOS OS FUNIS COMBINADOS por mes
#
# Fev: Funil Padrao (pag 4): Contatos 224, Reunioes 4, Vendas 0
#      Renovacao (Resultados Consolidados pag 8): Pedidos Renov 1, Receita R$20k
#      TOTAL Fev: 224 contatos, 4 reunioes, 1 pedido, 1 venda
#
# Mar: Funil Padrao (Comercial Marco pag 6): Contato 214, Reunioes 11, Vendas 3
#      Renovacao Q1 (pag 5): 27 contatos, 9 reunioes, 7 renovacoes no Q1
#      Renovacao Mar (Resultados Consolidados pag 8): Pedidos Renov 3, Receita R$24k
#      TOTAL Mar: 214+27=241 contatos, 11+9=20 reunioes, 3+3=6 pedidos, 3+3=6 vendas
#
HISTORICAL_COMMERCIAL = {
    'Fev': {'contatos': 224, 'reunioes': 4, 'pedidos': 1, 'vendas': 1},
    'Mar': {'contatos': 241, 'reunioes': 20, 'pedidos': 6, 'vendas': 6},
}

# Renovacao historico (para funil acumulado com breakdown)
HISTORICAL_RENOVACAO = {
    'Jan': {'contatos': 0, 'reunioes': 0, 'pedidos': 2, 'vendas': 2, 'receita': 40000},
    'Fev': {'contatos': 0, 'reunioes': 0, 'pedidos': 1, 'vendas': 1, 'receita': 20000},
    'Mar': {'contatos': 27, 'reunioes': 9, 'pedidos': 3, 'vendas': 3, 'receita': 24000},
}

HISTORICAL_QUALIFIED = {
    # Fevereiro: SQL 124 do funil (report pagina 4)
    'Fev': 124,
    # Marco: ~76 leads qualificados (63 form nativo + 13 LP C4) — report pagina 9
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

    # Stages por funil
    funnel_config = {
        'fp': {
            'contato': {FP_CONTATO, FP_FUP_POS, FP_INTERESSE, FP_REUNIAO_MARCADA, FP_REUNIAO_REALIZ, FP_FUP, FP_NEGOCIACAO},
            'reuniao': {FP_REUNIAO_MARCADA, FP_REUNIAO_REALIZ},
            'pedido': {FP_NEGOCIACAO},
            'excluir': {FP_SEM_CONTATO, FP_PERDIDO},
        },
        'pa': {
            'contato': {PA_CONTATO, PA_INTERESSE, PA_REUNIAO_FUP, PA_PROPOSTA, PA_NEGOCIACAO},
            'reuniao': {PA_REUNIAO_FUP, PA_PROPOSTA},
            'pedido': {PA_NEGOCIACAO},
            'excluir': {PA_SEM_CONTATO},
        },
        'cr': {
            'contato': {CR_CONTATO, CR_INTERESSE, CR_APRESENTACAO, CR_PROPOSTA, CR_NEGOCIACAO},
            'reuniao': {CR_APRESENTACAO, CR_PROPOSTA},
            'pedido': {CR_NEGOCIACAO},
            'excluir': {CR_SEM_CONTATO, CR_PERDIDO},
        },
    }

    # Inicializar weeks por funil
    weeks_by_funnel = {}
    for fn in ['fp', 'pa', 'cr']:
        weeks_by_funnel[fn] = {}
        for label in all_week_labels():
            weeks_by_funnel[fn][label] = {'contatos': 0, 'reunioes': 0, 'pedidos': 0, 'vendas': 0}

    try:
        all_deals = sb_fetch('')
        print(f'  Supabase: {len(all_deals)} deals total')

        for d in all_deals:
            p = parse_date(d.get('crm_created_at'))
            if not p or (p[0], p[1]) not in valid_months:
                continue

            lbl = week_label(*p)
            sid = d.get('stage_id', '')
            is_won = d.get('deal_status') == 'won'

            for fn, cfg in funnel_config.items():
                all_fn_stages = cfg['contato'] | cfg['excluir']
                if sid not in all_fn_stages:
                    continue

                if lbl not in weeks_by_funnel[fn]:
                    continue

                if sid in cfg['contato']:
                    weeks_by_funnel[fn][lbl]['contatos'] += 1
                if sid in cfg['reuniao']:
                    weeks_by_funnel[fn][lbl]['reunioes'] += 1
                if sid in cfg['pedido']:
                    weeks_by_funnel[fn][lbl]['pedidos'] += 1
                if is_won:
                    weeks_by_funnel[fn][lbl]['vendas'] += 1

        # Log por funil
        for fn in ['fp', 'pa', 'cr']:
            tc = sum(w['contatos'] for w in weeks_by_funnel[fn].values())
            tr = sum(w['reunioes'] for w in weeks_by_funnel[fn].values())
            tv = sum(w['vendas'] for w in weeks_by_funnel[fn].values())
            print(f'  {fn.upper()} (Abr+): contatos={tc}, reunioes={tr}, vendas={tv}')

    except Exception as e:
        print(f'  Supabase CRM err: {e}', file=sys.stderr)

    # weeks principal = Funil Padrao (para compatibilidade)
    weeks = weeks_by_funnel['fp']

    # Guardar weeks_by_funnel no modulo para uso posterior
    global _weeks_by_funnel
    _weeks_by_funnel = weeks_by_funnel

    return weeks

_weeks_by_funnel = {}


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
    avg_cpl = round(total_spend / total_leads, 2) if total_leads else 0

    # Totais comerciais = todos os funis combinados
    # crm ja tem Fev/Mar com dados do PDF (todos os funis), Abr+ sera somado com PA/CR abaixo
    # Os totais finais serao recalculados depois que contatos_data for montado

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

    def build_row(label, values, formatter, css_class='', total_formatter=None):
        cells = f'<td class="metric-name {css_class}">{label}</td>'
        total = 0
        for w in week_labels:
            val = values.get(w, 0)
            cells += f'<td class="{css_class}">{formatter(val)}</td>'
            total += val
        # Coluna total
        tfmt = total_formatter or formatter
        cells += f'<td class="total-col {css_class}">{tfmt(total)}</td>'
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

    # Dados comerciais combinados (todos os funis)
    # Para Fev/Mar: historico ja inclui todos os funis (PDF)
    # Para Abr+: somar FP + PA + CR do Supabase
    contatos_data = {}
    reunioes_data = {}
    pedidos_data = {}
    vendas_data = {}

    for w in week_labels:
        # Historico (Fev/Mar) ja vem combinado no crm dict
        fp = crm.get(w, {})
        pa = _weeks_by_funnel.get('pa', {}).get(w, {})
        cr = _weeks_by_funnel.get('cr', {}).get(w, {})

        contatos_data[w] = fp.get('contatos', 0) + pa.get('contatos', 0) + cr.get('contatos', 0)
        reunioes_data[w] = fp.get('reunioes', 0) + pa.get('reunioes', 0) + cr.get('reunioes', 0)
        pedidos_data[w] = fp.get('pedidos', 0) + pa.get('pedidos', 0) + cr.get('pedidos', 0)
        vendas_data[w] = fp.get('vendas', 0) + pa.get('vendas', 0) + cr.get('vendas', 0)

    # Recalcular totais comerciais (todos os funis combinados)
    total_contatos = sum(contatos_data.values())
    total_reunioes = sum(reunioes_data.values())
    total_pedidos_all = sum(pedidos_data.values())
    total_vendas = sum(vendas_data.values())

    # CTR e CPL totais (media ponderada, nao soma)
    avg_ctr = round(sum(clicks_data.values()) / sum(impressions_data.values()) * 100, 2) if sum(impressions_data.values()) else 0

    def build_row_custom_total(label, values, formatter, total_val, total_fmt, css_class=''):
        """Row com total customizado (para CTR e CPL que nao sao soma)."""
        cells = f'<td class="metric-name {css_class}">{label}</td>'
        for w in week_labels:
            val = values.get(w, 0)
            cells += f'<td class="{css_class}">{formatter(val)}</td>'
        cells += f'<td class="total-col {css_class}">{total_fmt(total_val)}</td>'
        return f'<tr>{cells}</tr>'

    table_rows = '\n'.join([
        build_row('Investimento', spend_data, fmt_brl),
        build_row('Impressoes', impressions_data, fmt_int),
        build_row('Cliques', clicks_data, fmt_int),
        build_row('Leads', leads_data, fmt_int),
        build_row('Leads Qualificados', qual_data, fmt_int),
        build_row_custom_total('CTR', ctr_data, fmt_pct, avg_ctr, fmt_pct),
        build_row_custom_total('CPL', cpl_data, fmt_brl, avg_cpl, fmt_brl),
        build_row('Contatos', contatos_data, fmt_int, 'commercial'),
        build_row('Reunioes', reunioes_data, fmt_int, 'commercial'),
        build_row('Pedidos', pedidos_data, fmt_int, 'commercial'),
        build_row('Vendas', vendas_data, fmt_int, 'commercial'),
    ])

    week_headers = ''.join(f'<th>{w}</th>' for w in week_labels) + '<th class="total-col">TOTAL</th>'

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
        m_contatos = sum(contatos_data.get(w, 0) for w in m_weeks)
        m_reunioes = sum(reunioes_data.get(w, 0) for w in m_weeks)
        m_pedidos = sum(pedidos_data.get(w, 0) for w in m_weeks)
        m_vendas = sum(vendas_data.get(w, 0) for w in m_weeks)

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
                <div class="month-stat"><span class="month-label">Pedidos</span><span class="month-value">{fmt_int(m_pedidos)}</span></div>
                <div class="month-stat"><span class="month-label">Vendas</span><span class="month-value">{fmt_int(m_vendas)}</span></div>
            </div>
        </div>'''

    # ── Funil de conversao acumulado ──
    # Totais = mesmos da tabela semanal (todos os funis combinados)
    all_contatos = total_contatos
    all_reunioes = total_reunioes
    all_pedidos = total_pedidos_all
    all_vendas = total_vendas

    # Breakdown por funil (historico separado + Supabase Abr+)
    # FP historico: Fev=224c/4r/0p/0v, Mar=214c/11r/3p/3v
    fp_hist_c = 224 + 214  # Fev + Mar FP only
    fp_hist_r = 4 + 11
    fp_hist_p = 0 + 3
    fp_hist_v = 0 + 3
    # FP Supabase Abr+
    fp_abr_c = sum(_weeks_by_funnel.get('fp', {}).get(w, {}).get('contatos', 0) for w in week_labels if w.startswith('Abr'))
    fp_abr_r = sum(_weeks_by_funnel.get('fp', {}).get(w, {}).get('reunioes', 0) for w in week_labels if w.startswith('Abr'))
    fp_abr_p = sum(_weeks_by_funnel.get('fp', {}).get(w, {}).get('pedidos', 0) for w in week_labels if w.startswith('Abr'))
    fp_abr_v = sum(_weeks_by_funnel.get('fp', {}).get(w, {}).get('vendas', 0) for w in week_labels if w.startswith('Abr'))
    fp_contatos = fp_hist_c + fp_abr_c
    fp_reunioes = fp_hist_r + fp_abr_r
    fp_pedidos_t = fp_hist_p + fp_abr_p
    fp_vendas = fp_hist_v + fp_abr_v

    # PA (sem historico, tudo do Supabase)
    pa_contatos = sum(_weeks_by_funnel.get('pa', {}).get(w, {}).get('contatos', 0) for w in week_labels)
    pa_reunioes = sum(_weeks_by_funnel.get('pa', {}).get(w, {}).get('reunioes', 0) for w in week_labels)
    pa_vendas = sum(_weeks_by_funnel.get('pa', {}).get(w, {}).get('vendas', 0) for w in week_labels)

    # CR historico: Fev=0c/0r/1p/1v, Mar=27c/9r/3p/3v (do report PDF)
    cr_hist_c = 0 + 27
    cr_hist_r = 0 + 9
    cr_hist_v = 1 + 3  # Fev=1 renov + Mar=3 renov
    # CR Supabase Abr+
    cr_abr_c = sum(_weeks_by_funnel.get('cr', {}).get(w, {}).get('contatos', 0) for w in week_labels if w.startswith('Abr'))
    cr_abr_r = sum(_weeks_by_funnel.get('cr', {}).get(w, {}).get('reunioes', 0) for w in week_labels if w.startswith('Abr'))
    cr_abr_v = sum(_weeks_by_funnel.get('cr', {}).get(w, {}).get('vendas', 0) for w in week_labels if w.startswith('Abr'))
    cr_contatos = cr_hist_c + cr_abr_c
    cr_reunioes = cr_hist_r + cr_abr_r
    cr_vendas = cr_hist_v + cr_abr_v

    print(f'  Funil acumulado — FP: {fp_contatos}c/{fp_reunioes}r/{fp_vendas}v | PA: {pa_contatos}c/{pa_reunioes}r/{pa_vendas}v | CR: {cr_contatos}c/{cr_reunioes}r/{cr_vendas}v')
    print(f'  Totais: {all_contatos}c/{all_reunioes}r/{all_vendas}v (check: FP+PA+CR = {fp_contatos+pa_contatos+cr_contatos}c/{fp_reunioes+pa_reunioes+cr_reunioes}r/{fp_vendas+pa_vendas+cr_vendas}v)')

    conv_lead_cont = round(all_contatos / total_leads * 100, 1) if total_leads else 0
    conv_cont_qual = round(total_qual / all_contatos * 100, 1) if all_contatos else 0
    conv_qual_reun = round(all_reunioes / total_qual * 100, 1) if total_qual else 0
    conv_reun_vend = round(all_vendas / all_reunioes * 100, 1) if all_reunioes else 0
    conv_total = round(all_vendas / total_leads * 100, 2) if total_leads else 0

    # Breakdown HTML por origem
    def breakdown_badge(fp, pa, cr):
        parts = []
        if fp: parts.append(f'<span class="fb-fp">{fmt_int(fp)} Funil Padrao</span>')
        if pa: parts.append(f'<span class="fb-pa">{fmt_int(pa)} Prosp. Ativa</span>')
        if cr: parts.append(f'<span class="fb-cr">{fmt_int(cr)} Renovacao</span>')
        return ' '.join(parts) if parts else ''

    bd_contatos = breakdown_badge(fp_contatos, pa_contatos, cr_contatos)
    bd_reunioes = breakdown_badge(fp_reunioes, pa_reunioes, cr_reunioes)
    bd_vendas = breakdown_badge(fp_vendas, pa_vendas, cr_vendas)

    funnel_html = f'''
<div class="funnel-title">Funil de Conversao Acumulado — Todos os Funis</div>
<div class="funnel-section">
    <div class="funnel-container">
        <div class="vfunnel">
            <div class="vfunnel-stage">
                <div class="vfunnel-bar" style="width: 100%; background: linear-gradient(90deg, #00B37E, #00D195);">
                    <span class="vfunnel-label">Leads (Meta Ads)</span>
                    <span class="vfunnel-value">{fmt_int(total_leads)}</span>
                </div>
            </div>
            <div class="vfunnel-conv-row"><span class="vfunnel-pct">&#8595; {conv_lead_cont}% foram contatados</span></div>
            <div class="vfunnel-stage">
                <div class="vfunnel-bar" style="width: 75%; background: linear-gradient(90deg, #9B59B6, #C084FC);">
                    <span class="vfunnel-label">Contatos</span>
                    <span class="vfunnel-value">{fmt_int(all_contatos)}</span>
                </div>
            </div>
            <div class="vfunnel-breakdown">{bd_contatos}</div>
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
                    <span class="vfunnel-value">{fmt_int(all_reunioes)}</span>
                </div>
            </div>
            <div class="vfunnel-breakdown">{bd_reunioes}</div>
            <div class="vfunnel-conv-row"><span class="vfunnel-pct">&#8595; {conv_reun_vend}% fecharam</span></div>
            <div class="vfunnel-stage">
                <div class="vfunnel-bar" style="width: 18%; background: linear-gradient(90deg, #D94E7A, #FF6B9D);">
                    <span class="vfunnel-label">Vendas</span>
                    <span class="vfunnel-value">{fmt_int(all_vendas)}</span>
                </div>
            </div>
            <div class="vfunnel-breakdown">{bd_vendas}</div>
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

/* Total column */
td.total-col {{
    background: rgba(0, 209, 149, 0.08);
    font-weight: 700;
    border-left: 2px solid rgba(0, 209, 149, 0.3);
}}
th.total-col {{
    background: rgba(0, 209, 149, 0.12);
    color: #00D195;
    font-weight: 700;
    border-left: 2px solid rgba(0, 209, 149, 0.3);
}}

/* Prospect rows */
td.prospect {{ background: rgba(0, 209, 149, 0.06); }}
td.metric-name.prospect {{ background: rgba(0, 209, 149, 0.06); color: #00D195; }}
tr:hover td.prospect {{ background: rgba(0, 209, 149, 0.12); }}

/* Renovation rows */
td.renov {{ background: rgba(255, 165, 0, 0.06); }}
td.metric-name.renov {{ background: rgba(255, 165, 0, 0.06); color: #FFA500; }}
tr:hover td.renov {{ background: rgba(255, 165, 0, 0.12); }}

/* Total rows */
td.total-row {{ background: rgba(255, 255, 255, 0.04); font-weight: 700; }}
td.metric-name.total-row {{ background: rgba(255, 255, 255, 0.04); color: #FFF; font-weight: 700; }}

/* Section header in table */
.section-header-row td {{
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #A0A0C0;
    padding: 14px 10px 6px 10px;
    border-bottom: 1px solid #1a1a2e;
    background: transparent;
}}

/* Separator row */
.separator-row td {{
    padding: 0;
    height: 2px;
    border: none;
    background: transparent;
}}

/* Section labels */
.section-label {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #A0A0C0;
    margin: 24px 0 8px 0;
    padding-bottom: 6px;
    border-bottom: 1px solid #1a1a2e;
}}

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
.vfunnel-breakdown {{
    display: flex; justify-content: center; gap: 8px; padding: 4px 0 2px;
    flex-wrap: wrap;
}}
.vfunnel-breakdown span {{
    font-size: 10px; font-weight: 600; padding: 2px 8px; border-radius: 3px;
}}
.fb-fp {{ background: rgba(155, 89, 182, 0.15); color: #C084FC; }}
.fb-pa {{ background: rgba(0, 179, 126, 0.15); color: #00D195; }}
.fb-cr {{ background: rgba(224, 138, 0, 0.15); color: #FFA500; }}
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

<div class="section-label">Midia Paga (Meta Ads)</div>
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
</div>

<div class="section-label">Comercial — Todos os Funis</div>
<div class="summary-row">
    <div class="summary-card">
        <div class="label">Contatos</div>
        <div class="value pink">{fmt_int(total_contatos)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Reunioes</div>
        <div class="value pink">{fmt_int(total_reunioes)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Pedidos</div>
        <div class="value pink">{fmt_int(total_pedidos_all)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Vendas</div>
        <div class="value pink">{fmt_int(total_vendas)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Taxa Contato</div>
        <div class="value" style="color:#A0A0C0">{fmt_pct(round(total_contatos / total_leads * 100, 1) if total_leads else 0)}</div>
    </div>
    <div class="summary-card">
        <div class="label">Taxa Reuniao</div>
        <div class="value" style="color:#A0A0C0">{fmt_pct(round(total_reunioes / total_contatos * 100, 1) if total_contatos else 0)}</div>
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
