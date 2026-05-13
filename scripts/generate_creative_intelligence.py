#!/usr/bin/env python3
"""
generate_creative_intelligence.py
Dashboard "Creative Intelligence" — @kai + @cris

Cruza Meta Ads (por anuncio) + RD Station (segmentacoes de qualificados)
para mostrar qual criativo traz os melhores leads por perfil.

Principios de design aplicados:
  - Storytelling with Data (Knaflic): grey+accent, declutter, action titles
  - Data Story (Duarte): SBI structure, "So What?" test, executive summary 3-box

Fontes:
  - Meta Ads API: spend, leads, impressions, clicks, thumbnail por ad
  - RD Station Marketing: segmentacoes 19356678 (50k-500k) e 19356688 (>500k)
  - RD Station Marketing: contatos individuais com cf_utm_content para cruzar

Env vars (obrigatorias):
  META_ACCESS_TOKEN ou META_TOKEN
  RD_CLIENT_ID, RD_CLIENT_SECRET, RD_REFRESH_TOKEN

Saida:
  projetos-boost/painel/creative-intelligence/index.html
  projetos-boost/painel/creative-intelligence/data.json
"""
import json, urllib.request, urllib.parse, ssl, sys, os, time, hashlib
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ──────────────────────────────────────────────────────────────
# ENV VARS
# ──────────────────────────────────────────────────────────────
META_TOKEN = os.environ.get('META_ACCESS_TOKEN') or os.environ.get('META_TOKEN')
if not META_TOKEN:
    raise SystemExit('ERRO: defina META_ACCESS_TOKEN (ou META_TOKEN) no ambiente')

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit('ERRO: defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY')

RD_CLIENT_ID = os.environ.get('RD_CLIENT_ID')
RD_CLIENT_SECRET = os.environ.get('RD_CLIENT_SECRET')
RD_REFRESH_TOKEN = os.environ.get('RD_REFRESH_TOKEN')
if not all([RD_CLIENT_ID, RD_CLIENT_SECRET, RD_REFRESH_TOKEN]):
    raise SystemExit('ERRO: defina RD_CLIENT_ID, RD_CLIENT_SECRET e RD_REFRESH_TOKEN')

ACCOUNT = 'act_844208497068966'
META_BASE = 'https://graph.facebook.com/v19.0'
RD_MKT_BASE = 'https://api.rd.services'

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), 'painel', 'creative-intelligence')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Segmentacoes RD de leads qualificados
SEG_50K_500K = '19356678'
SEG_ABOVE_500K = '19356688'

# Periodo de analise (ultimos 30 dias)
TODAY = datetime.now()
SINCE = (TODAY - timedelta(days=30)).strftime('%Y-%m-%d')
UNTIL = TODAY.strftime('%Y-%m-%d')


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def api_get(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f'  API err {url[:80]}: {e}', file=sys.stderr)
        return {}


def meta_api(path, params=None):
    params = params or {}
    params['access_token'] = META_TOKEN
    url = f'{META_BASE}{path}?{urllib.parse.urlencode(params)}'
    return api_get(url)


def get_actions(actions, key):
    for a in (actions or []):
        if a.get('action_type') == key:
            return int(float(a.get('value', 0)))
    return 0


# ──────────────────────────────────────────────────────────────
# RD STATION AUTH
# ──────────────────────────────────────────────────────────────
RD_ACCESS_TOKEN = None

def rd_auth():
    global RD_ACCESS_TOKEN
    print('  RD: autenticando...')
    data = json.dumps({
        'client_id': RD_CLIENT_ID,
        'client_secret': RD_CLIENT_SECRET,
        'refresh_token': RD_REFRESH_TOKEN
    }).encode()
    req = urllib.request.Request(
        f'{RD_MKT_BASE}/auth/token',
        data=data,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            resp = json.loads(r.read())
            RD_ACCESS_TOKEN = resp.get('access_token')
            print(f'  RD: token OK')
    except Exception as e:
        print(f'  RD auth err: {e}', file=sys.stderr)


def rd_api(path, params=None):
    if not RD_ACCESS_TOKEN:
        rd_auth()
    params = params or {}
    qs = urllib.parse.urlencode(params)
    url = f'{RD_MKT_BASE}{path}{"?" + qs if qs else ""}'
    return api_get(url, headers={
        'Authorization': f'Bearer {RD_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    })


# ──────────────────────────────────────────────────────────────
# META ADS — DADOS POR ANUNCIO
# ──────────────────────────────────────────────────────────────
def fetch_ads_performance():
    """Busca performance por anuncio nos ultimos 30 dias."""
    print('  Meta: buscando ads...')
    ads = []
    url_params = {
        'level': 'ad',
        'fields': 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,actions',
        'time_range': json.dumps({'since': SINCE, 'until': UNTIL}),
        'limit': 100,
        'filtering': json.dumps([{'field': 'spend', 'operator': 'GREATER_THAN', 'value': '0'}])
    }
    r = meta_api(f'/{ACCOUNT}/insights', url_params)
    for row in r.get('data', []):
        actions = row.get('actions', [])
        leads = get_actions(actions, 'onsite_conversion.lead_grouped') or get_actions(actions, 'lead')
        spend = float(row.get('spend', 0))
        impressions = int(row.get('impressions', 0))
        clicks = int(row.get('clicks', 0))
        ctr = float(row.get('ctr', 0))
        cpl = spend / leads if leads > 0 else 0

        ads.append({
            'ad_id': row.get('ad_id'),
            'ad_name': row.get('ad_name', ''),
            'campaign_name': row.get('campaign_name', ''),
            'adset_name': row.get('adset_name', ''),
            'spend': spend,
            'impressions': impressions,
            'clicks': clicks,
            'ctr': ctr,
            'leads': leads,
            'cpl': cpl,
        })

    # Paginacao
    paging = r.get('paging', {})
    while paging.get('next'):
        r = api_get(paging['next'])
        for row in r.get('data', []):
            actions = row.get('actions', [])
            leads = get_actions(actions, 'onsite_conversion.lead_grouped') or get_actions(actions, 'lead')
            spend = float(row.get('spend', 0))
            impressions = int(row.get('impressions', 0))
            clicks = int(row.get('clicks', 0))
            ctr = float(row.get('ctr', 0))
            cpl = spend / leads if leads > 0 else 0
            ads.append({
                'ad_id': row.get('ad_id'),
                'ad_name': row.get('ad_name', ''),
                'campaign_name': row.get('campaign_name', ''),
                'adset_name': row.get('adset_name', ''),
                'spend': spend,
                'impressions': impressions,
                'clicks': clicks,
                'ctr': ctr,
                'leads': leads,
                'cpl': cpl,
            })
        paging = r.get('paging', {})

    print(f'  Meta: {len(ads)} anuncios com gasto')
    return ads


def fetch_ad_thumbnails(ad_ids):
    """Busca thumbnails dos anuncios via creative."""
    print(f'  Meta: buscando thumbnails de {len(ad_ids)} ads...')
    thumbs = {}
    for ad_id in ad_ids:
        try:
            # Step 1: get creative id
            r = meta_api(f'/{ad_id}', {'fields': 'creative{id}'})
            creative_id = r.get('creative', {}).get('id')
            if not creative_id:
                continue
            # Step 2: get image
            r2 = meta_api(f'/{creative_id}', {
                'fields': 'image_url,thumbnail_url,object_story_spec'
            })
            img = r2.get('image_url') or ''
            if not img:
                oss = r2.get('object_story_spec', {})
                link_data = oss.get('link_data', {})
                video_data = oss.get('video_data', {})
                photo_data = oss.get('photo_data', {})
                img = (link_data.get('picture') or
                       video_data.get('image_url') or
                       photo_data.get('url') or
                       r2.get('thumbnail_url') or '')
            if img:
                thumbs[ad_id] = img
            time.sleep(0.3)  # rate limit
        except Exception as e:
            print(f'    thumb err {ad_id}: {e}', file=sys.stderr)
    print(f'  Meta: {len(thumbs)} thumbnails obtidas')
    return thumbs


# ──────────────────────────────────────────────────────────────
# RD STATION — LEADS QUALIFICADOS VIA SEGMENTACOES + EVENT LOGS
# Metodo: buscar UUID de cada contato qualificado, depois
# /contacts/{uuid}/events?event_type=CONVERSION para pegar
# event_identifier (formulario) e traffic_source (campanha).
# ──────────────────────────────────────────────────────────────
def fetch_rd_segmentation_contacts(seg_id):
    """Busca todos os contatos de uma segmentacao RD."""
    contacts = []
    page = 1
    while True:
        r = rd_api(f'/platform/segmentations/{seg_id}/contacts', {
            'page_size': 125, 'page': page
        })
        batch = r.get('contacts', [])
        if not batch:
            break
        contacts.extend(batch)
        if len(batch) < 125:
            break
        page += 1
        time.sleep(0.5)
    return contacts


def rd_fetch_contact_full(uuid):
    """Busca dados completos de um contato (cf_*, telefone)."""
    return rd_api(f'/platform/contacts/{uuid}')


def rd_fetch_contact_conversions(uuid):
    """Busca eventos de conversao do contato (formulario de origem)."""
    r = rd_api(f'/platform/contacts/{uuid}/events', {
        'event_type': 'CONVERSION', 'page': 1, 'page_size': 5
    })
    if isinstance(r, dict) and 'events' in r:
        return r['events']
    if isinstance(r, list):
        return r
    return []


def fetch_qualified_leads_with_utm():
    """Busca leads qualificados via RD Segmentacoes + event logs para UTMs."""
    print('  RD: buscando leads qualificados via segmentacoes + event logs...')
    rd_auth()

    # 1. Buscar contatos das segmentacoes de qualificados
    all_contacts = []
    for seg_id, label in [(SEG_50K_500K, '50k-500k'), (SEG_ABOVE_500K, '>500k')]:
        contacts = fetch_rd_segmentation_contacts(seg_id)
        print(f'    Seg {label}: {len(contacts)} contatos')
        for c in contacts:
            c['_faixa'] = '>500k' if seg_id == SEG_ABOVE_500K else '50k-500k'
        all_contacts.extend(contacts)

    # Deduplicar por email
    seen = set()
    unique = []
    for c in all_contacts:
        email = (c.get('email') or '').lower()
        if email and email not in seen:
            seen.add(email)
            unique.append(c)

    rd_total = len(unique)
    print(f'  RD: {rd_total} leads qualificados unicos')

    # 2. Enriquecer cada contato com cf_* e event logs
    print(f'  RD: enriquecendo {rd_total} contatos (cf_* + events)...')
    enriched = []
    for i, c in enumerate(unique):
        uuid = c.get('uuid')
        if not uuid:
            continue

        # Buscar dados completos
        full = rd_fetch_contact_full(uuid)
        cf = {}
        if isinstance(full, dict):
            cf = full.get('custom_fields', {}) or {}

        # Buscar evento de conversao (utm_source/campaign/content)
        events = rd_fetch_contact_conversions(uuid)
        conversion_event = ''
        utm_source = ''
        utm_campaign = ''
        utm_content = ''

        if events and isinstance(events, list) and len(events) > 0:
            # Pegar primeiro evento de conversao
            evt = events[0] if isinstance(events[0], dict) else {}
            conversion_event = evt.get('event_identifier', '')
            # Extrair UTMs do content (se disponivel)
            content = evt.get('content', {}) or {}
            if isinstance(content, dict):
                utm_source = content.get('traffic_source', '') or ''
                utm_campaign = content.get('traffic_campaign', '') or ''
                utm_content = content.get('traffic_content', '') or ''

        # Fallback para cf_* se event logs nao tiver
        if not utm_campaign:
            utm_campaign = cf.get('cf_utm_campaign', '') or ''
        if not utm_content:
            utm_content = cf.get('cf_utm_content', '') or ''
        if not utm_source:
            utm_source = cf.get('cf_utm_source', '') or ''

        # Dados de investimento
        investe_cripto = cf.get('cf_voce_ja_possui_investimentos_em_bitcoin_ou_criptoativos', '') or \
                         cf.get('cf_voce_ja_possui_investimentos_em_bitcoin_cripto', '') or ''
        investe_trad = cf.get('cf_e_voce_possui_investimentos_no_mercado_tradicional_teso', '') or ''

        enriched.append({
            'email': c.get('email', ''),
            'name': c.get('name', ''),
            'faixa': c.get('_faixa', '50k-500k'),
            'conversion_event': conversion_event,
            'utm_campaign': utm_campaign,
            'utm_content': utm_content,
            'utm_source': utm_source,
            'investe_cripto': 'sim' if 'sim' in investe_cripto.lower() else 'nao',
            'investe_trad': 'sim' if 'sim' in investe_trad.lower() else 'nao',
        })

        if (i + 1) % 10 == 0:
            print(f'    ... {i + 1}/{rd_total} contatos enriquecidos')
        time.sleep(0.15)  # rate limit RD

    with_utm = len([l for l in enriched if l['utm_content'] or l['utm_campaign'] or l['conversion_event']])
    print(f'  RD: {with_utm}/{len(enriched)} com UTM ou conversion event')
    return enriched, rd_total


# ──────────────────────────────────────────────────────────────
# SUPABASE — ENRIQUECER PERFIL (investe_cripto, investe_trad)
# ──────────────────────────────────────────────────────────────
def supabase_api(path):
    url = f'{SUPABASE_URL}/rest/v1/{path}'
    return api_get(url, headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json'
    })


def enrich_with_supabase_profile(qualified_leads):
    """Enriquece leads qualificados com dados de perfil do Supabase."""
    print('  Supabase: buscando perfil investidor...')
    sb_leads = supabase_api(
        'leads?select=email,investe_cripto,investe_tradicional,patrimonio_cripto_min_k'
        '&patrimonio_cripto_min_k=gte.50&limit=500'
    )
    if not isinstance(sb_leads, list):
        sb_leads = []

    # Indexar por email
    sb_by_email = {}
    for l in sb_leads:
        email = (l.get('email') or '').lower()
        if email:
            sb_by_email[email] = l

    matched = 0
    for lead in qualified_leads:
        email = (lead.get('email') or '').lower()
        sb = sb_by_email.get(email)
        if sb:
            lead['investe_cripto'] = 'sim' if sb.get('investe_cripto') else 'nao'
            lead['investe_trad'] = 'sim' if sb.get('investe_tradicional') else 'nao'
            matched += 1

    print(f'  Supabase: {matched}/{len(qualified_leads)} enriquecidos com perfil')


# ──────────────────────────────────────────────────────────────
# CRUZAMENTO: ADS x LEADS QUALIFICADOS
# ──────────────────────────────────────────────────────────────
def cross_ads_with_qualified(ads, qualified_leads):
    """Cruza anuncios Meta com leads qualificados do Supabase.
    Usa first_content (utm_content) para match ad-level e
    first_campaign para match campaign-level."""

    # Mapeamento: event_identifier (formulario RD) → nome parcial da campanha Meta
    # Isso permite cruzar leads que vieram via Lead Form (sem UTM)
    # Mapeamento completo: event_identifier → parte do nome da campanha Meta
    # Baseado em analise de 79 leads qualificados (24/abr/2026)
    EVENT_TO_CAMPAIGN = {
        # LP Analise (analise.boostresearch.com.br) → campanhas C4
        'lp_mentoria_boost': 'C4',
        'formulario-analise': 'C4',
        'formulario-lp-analise': 'C4',
        # E-book IR → campanha IR Cripto
        'E-book IR 03/2026': 'IR',
        # Lead Form nativo Meta → C4 CONVERSAO-FORM
        'Campanha Inicial 03/2026': 'CONVERSAO-FORM',
        'Campanha Inicial 03/2026 - Versão — Maior intenção-copy': 'CONVERSAO-FORM',
        'Campanha Inicial 02/2026 - Versão — Menos pergunta V1': 'CONVERSAO-FORM',
        # Quiz
        'formulario-quiz': 'QUIZ',
        'quiz-patrimonio': 'QUIZ',
        # E-book Portfolio
        'lp_ebook_portfolio': 'EBOOK',
        # Webinar
        'pos_webinar_1milhao_ebook': 'WEBINAR',
        # Teste
        'teste-form-v3-valores': 'C4',
        # Ignorar (clientes existentes, nao campanha)
        'Consultoria Crypto Inteligence Essencial': '',
        'Consultoria Crypto Inteligence Pro': '',
        '/crypto-inteligence-access/': '',
        '/crypto-inteligence-essencial/': '',
        'New Form': '',
        'Newsletter': '',
    }

    # Indexar leads por utm_content, utm_campaign e conversion_event
    leads_by_content = defaultdict(list)
    leads_by_campaign = defaultdict(list)
    leads_by_event = defaultdict(list)
    leads_no_utm = []

    for lead in qualified_leads:
        utm_c = lead['utm_content'].strip()
        utm_camp = lead['utm_campaign'].strip()
        conv_evt = lead.get('conversion_event', '').strip()

        # Tentar mapear conversion_event para campaign parcial
        if conv_evt and conv_evt in EVENT_TO_CAMPAIGN and EVENT_TO_CAMPAIGN[conv_evt]:
            mapped_camp = EVENT_TO_CAMPAIGN[conv_evt]
            if not utm_camp:
                utm_camp = mapped_camp
                lead['utm_campaign'] = mapped_camp

        has_any = False
        if utm_c:
            leads_by_content[utm_c].append(lead)
            has_any = True
        if utm_camp:
            leads_by_campaign[utm_camp].append(lead)
            has_any = True
        if conv_evt:
            leads_by_event[conv_evt].append(lead)
            has_any = True
        if not has_any:
            leads_no_utm.append(lead)

    # PASSO 1: Match direto por utm_content (ad-level, alta confianca)
    for ad in ads:
        ad_name = ad['ad_name']
        # Match exato por utm_content
        matched_direct = leads_by_content.get(ad_name, [])
        # Match parcial (utm_content contem parte do ad_name ou vice-versa)
        matched_partial = []
        for utm_key, leads_list in leads_by_content.items():
            if utm_key and len(utm_key) > 5:
                if utm_key in ad_name or ad_name in utm_key:
                    matched_partial.extend(leads_list)

        seen = set()
        direct_matched = []
        for lead in matched_direct + matched_partial:
            if lead['email'] not in seen:
                seen.add(lead['email'])
                direct_matched.append(lead)

        ad['_direct_leads'] = direct_matched

    # PASSO 2: Match por campaign (exato e parcial) + distribuicao proporcional
    ads_by_campaign = defaultdict(list)
    for ad in ads:
        if ad['leads'] > 0:
            ads_by_campaign[ad['campaign_name']].append(ad)

    for ad in ads:
        camp_name = ad['campaign_name']
        direct = ad.pop('_direct_leads')

        if direct:
            all_matched = direct
        elif ad['leads'] > 0:
            # Buscar leads por match exato OU parcial de campaign name
            camp_leads = leads_by_campaign.get(camp_name, [])

            # Match parcial: campaign name contem o utm_campaign ou vice-versa
            if not camp_leads:
                for utm_camp, leads_list in leads_by_campaign.items():
                    if utm_camp and len(utm_camp) > 2:
                        if utm_camp.upper() in camp_name.upper() or camp_name.upper() in utm_camp.upper():
                            camp_leads = leads_list
                            break

            # Tambem tentar match por event_identifier → campanha
            if not camp_leads:
                for evt_name, leads_list in leads_by_event.items():
                    mapped = EVENT_TO_CAMPAIGN.get(evt_name, '')
                    if mapped and mapped.upper() in camp_name.upper():
                        # Dedup antes de adicionar
                        existing_emails = {l['email'] for l in camp_leads}
                        for l in leads_list:
                            if l['email'] not in existing_emails:
                                camp_leads.append(l)
                                existing_emails.add(l['email'])

            if camp_leads:
                # Distribuir proporcionalmente pelos ads da mesma campanha
                camp_ads = ads_by_campaign.get(camp_name, [])
                total_camp_leads_meta = sum(a['leads'] for a in camp_ads)
                if total_camp_leads_meta > 0:
                    ratio = ad['leads'] / total_camp_leads_meta
                    n_alloc = max(round(len(camp_leads) * ratio), 0)
                    # Deduplicar
                    seen_emails = set()
                    all_matched = []
                    for l in camp_leads:
                        if l['email'] not in seen_emails and len(all_matched) < n_alloc:
                            seen_emails.add(l['email'])
                            all_matched.append(l)
                else:
                    all_matched = []
            else:
                all_matched = []
        else:
            all_matched = []

        ad['qualified_leads'] = len(all_matched)
        ad['qualified_50k_500k'] = sum(1 for l in all_matched if l['faixa'] == '50k-500k')
        ad['qualified_above_500k'] = sum(1 for l in all_matched if l['faixa'] == '>500k')
        ad['pct_cripto'] = sum(1 for l in all_matched if 'sim' in l['investe_cripto']) / max(len(all_matched), 1) * 100
        ad['pct_trad'] = sum(1 for l in all_matched if 'sim' in l['investe_trad']) / max(len(all_matched), 1) * 100
        ad['cpl_qualificado'] = ad['spend'] / ad['qualified_leads'] if ad['qualified_leads'] > 0 else 0
        ad['taxa_qualificacao'] = ad['qualified_leads'] / ad['leads'] * 100 if ad['leads'] > 0 else 0

    return ads, len(leads_no_utm)


# ──────────────────────────────────────────────────────────────
# MAIN — Gera data.json (Python) + index.html (template estatico com JS)
# ──────────────────────────────────────────────────────────────
def main():
    print('=== Creative Intelligence Dashboard ===')
    print(f'  Periodo: {SINCE} a {UNTIL}')

    # 1. Buscar performance por anuncio
    ads = fetch_ads_performance()
    if not ads:
        print('ERRO: nenhum anuncio encontrado', file=sys.stderr)
        sys.exit(1)

    # 2. Buscar thumbnails — todos com leads + top 20 por spend
    ads_with_leads = [a for a in ads if a['leads'] > 0]
    top_by_spend = sorted(ads, key=lambda x: -x['spend'])[:20]
    seen_ids = set()
    ads_for_thumbs = []
    for a in ads_with_leads + top_by_spend:
        if a['ad_id'] not in seen_ids:
            seen_ids.add(a['ad_id'])
            ads_for_thumbs.append(a)
    ad_ids = [a['ad_id'] for a in ads_for_thumbs[:50]]
    thumbnails = fetch_ad_thumbnails(ad_ids)

    # 3. Buscar leads qualificados via RD event logs
    qualified_leads, rd_total_qualified = fetch_qualified_leads_with_utm()

    # 3b. Enriquecer com dados de perfil do Supabase
    enrich_with_supabase_profile(qualified_leads)

    # 4. Cruzar ads com leads qualificados
    ads, leads_no_utm = cross_ads_with_qualified(ads, qualified_leads)

    print(f'  Cruzamento: {sum(1 for a in ads if a["qualified_leads"] > 0)} ads com qualificados')

    # 5. Adicionar thumbnails aos ads
    for ad in ads:
        ad['thumbnail'] = thumbnails.get(ad['ad_id'], '')

    # 6. Calcular perfil geral
    total_qualified = rd_total_qualified
    pct_cripto = sum(1 for l in qualified_leads if 'sim' in l.get('investe_cripto', '')) / max(len(qualified_leads), 1) * 100
    pct_trad = sum(1 for l in qualified_leads if 'sim' in l.get('investe_trad', '')) / max(len(qualified_leads), 1) * 100

    # 7. Salvar data.json
    os.makedirs(BASE_DIR, exist_ok=True)
    data_path = os.path.join(BASE_DIR, 'data.json')
    export = {
        'generated_at': datetime.now().isoformat(),
        'period': {'since': SINCE, 'until': UNTIL},
        'ads': ads,
        'summary': {
            'total_spend': sum(a['spend'] for a in ads),
            'total_leads': sum(a['leads'] for a in ads),
            'total_qualified': total_qualified,
            'total_50k_500k': sum(1 for l in qualified_leads if l['faixa'] == '50k-500k'),
            'total_above_500k': sum(1 for l in qualified_leads if l['faixa'] == '>500k'),
            'pct_cripto': round(pct_cripto, 1),
            'pct_trad': round(pct_trad, 1),
            'leads_no_utm': leads_no_utm,
        },
    }
    with open(data_path, 'w', encoding='utf-8') as f:
        json.dump(export, f, ensure_ascii=False, indent=2)
    print(f'  JSON salvo: {data_path}')

    # 8. Gerar HTML: ler template e injetar data.json inline
    html_path = os.path.join(BASE_DIR, 'index.html')
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'creative_intelligence_template.html')
    if os.path.exists(template_path):
        with open(template_path, 'r', encoding='utf-8') as f:
            template = f.read()
        # Injetar JSON inline para funcionar sem servidor (file://)
        data_json_str = json.dumps(export, ensure_ascii=False)
        # Substituir o fetch por dados inline
        template = template.replace(
            "fetch('data.json')\n  .then(r => r.json())\n  .then(d => { DATA = d; render(); })\n  .catch(e => {\n    document.getElementById('top-section').innerHTML = `<div style=\"color:var(--red);padding:40px;text-align:center\">Erro ao carregar dados: ${e.message}<br>Execute o script generate_creative_intelligence.py primeiro.</div>`;\n  });",
            f"DATA = {data_json_str};\nrender();"
        )
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(template)
        print(f'  HTML gerado com dados inline')
    else:
        print(f'  AVISO: template {template_path} nao encontrado')

    print('=== Concluido ===')


if __name__ == '__main__':
    main()
