#!/usr/bin/env python3
"""
dashboard_performance.py — Dashboard Performance Pós-Reestruturação (auto-atualizado)

Puxa dados de Meta Ads API + RD Station Marketing e gera HTML estático.
Roda a cada 2h via GitHub Actions.

Fontes:
  - Meta Ads API: campanhas, ad sets, ads (spend, leads, CPL, impressions)
  - RD Station Marketing: leads qualificados (patrimônio >= R$50k)

Env vars (obrigatórias):
  META_ACCESS_TOKEN
  RD_CLIENT_ID
  RD_CLIENT_SECRET
  RD_REFRESH_TOKEN

Saída:
  painel/performance-pos-reestruturacao/index.html
"""
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

# ──────────────────────────────────────────────────────────────
# ENV VARS — nunca hardcodar tokens
# ──────────────────────────────────────────────────────────────
META_TOKEN = os.environ.get('META_ACCESS_TOKEN') or os.environ.get('META_TOKEN')
if not META_TOKEN:
    raise SystemExit('ERRO: defina META_ACCESS_TOKEN no ambiente')

RD_CLIENT_ID = os.environ.get('RD_CLIENT_ID')
RD_CLIENT_SECRET = os.environ.get('RD_CLIENT_SECRET')
RD_REFRESH_TOKEN = os.environ.get('RD_REFRESH_TOKEN')
if not all([RD_CLIENT_ID, RD_CLIENT_SECRET, RD_REFRESH_TOKEN]):
    print('AVISO: RD_CLIENT_ID/SECRET/REFRESH_TOKEN não definidos — seção de leads qualificados ficará vazia', file=sys.stderr)
    RD_AVAILABLE = False
else:
    RD_AVAILABLE = True

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────
ACCOUNT = 'act_844208497068966'
META_BASE = 'https://graph.facebook.com/v21.0'
RD_MKT_BASE = 'https://api.rd.services'

# Data da reestruturação (referência para comparativos)
REESTRUTURACAO_DATE = '2026-04-14'

# Budget mensal meta
BUDGET_META_MENSAL = 40000

# Campanhas a rastrear (todas as ativas pós-reestruturação)
# Buscamos TODAS as campanhas ativas da conta automaticamente

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), 'painel', 'performance-pos-reestruturacao')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def meta_get(path, params=None):
    """GET na Meta API."""
    p = {'access_token': META_TOKEN}
    if params:
        p.update(params)
    url = f"{META_BASE}/{path}?{urllib.parse.urlencode(p)}"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"  Meta API erro {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
        return {}


def get_leads_from_actions(actions):
    """Extrai leads de actions da Meta API."""
    if not actions:
        return 0
    for a in actions:
        if a.get('action_type') in ('lead', 'onsite_conversion.lead_grouped'):
            return int(a.get('value', 0))
    return 0


def fmt_brl(val):
    """Formata valor em BRL."""
    if val is None:
        return '-'
    return f"R${val:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def fmt_brl_int(val):
    """Formata valor inteiro em BRL."""
    if val is None:
        return '-'
    return f"R${val:,.0f}".replace(',', '.')


# ──────────────────────────────────────────────────────────────
# META ADS — busca dados
# ──────────────────────────────────────────────────────────────
def fetch_all_campaigns():
    """Busca todas as campanhas da conta."""
    campaigns = []
    url_path = f"{ACCOUNT}/campaigns"
    params = {
        'fields': 'id,name,status,objective,daily_budget,lifetime_budget',
        'limit': 100,
    }
    data = meta_get(url_path, params)
    campaigns.extend(data.get('data', []))
    return campaigns


def fetch_campaign_insights(campaign_id, since, until):
    """Busca insights de uma campanha no período."""
    params = {
        'fields': 'spend,impressions,clicks,ctr,actions,cost_per_action_type,reach,frequency',
        'time_range': json.dumps({'since': since, 'until': until}),
    }
    data = meta_get(f"{campaign_id}/insights", params)
    return data.get('data', [{}])[0] if data.get('data') else {}


def fetch_ads_insights(campaign_id, since, until):
    """Busca insights por anúncio de uma campanha."""
    params = {
        'fields': 'ad_name,spend,impressions,clicks,actions',
        'time_range': json.dumps({'since': since, 'until': until}),
        'level': 'ad',
        'limit': 50,
    }
    data = meta_get(f"{campaign_id}/insights", params)
    return data.get('data', [])


def fetch_account_insights(since, until):
    """Busca insights totais da conta no período."""
    params = {
        'fields': 'spend,impressions,clicks,actions',
        'time_range': json.dumps({'since': since, 'until': until}),
    }
    data = meta_get(f"{ACCOUNT}/insights", params)
    return data.get('data', [{}])[0] if data.get('data') else {}


# ──────────────────────────────────────────────────────────────
# RD STATION MARKETING — leads qualificados
# ──────────────────────────────────────────────────────────────
def rd_get_access_token():
    """Renova access token do RD Station Marketing."""
    body = json.dumps({
        'client_id': RD_CLIENT_ID,
        'client_secret': RD_CLIENT_SECRET,
        'refresh_token': RD_REFRESH_TOKEN,
    }).encode()
    req = urllib.request.Request(
        f"{RD_MKT_BASE}/auth/token",
        data=body,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
            return json.loads(r.read()).get('access_token')
    except Exception as e:
        print(f"  RD auth erro: {e}", file=sys.stderr)
        return None


def rd_fetch_segmentation(token, seg_id):
    """Busca contatos de uma segmentação do RD (retorna dados resumidos)."""
    contacts = []
    page = 1
    while True:
        url = f"{RD_MKT_BASE}/platform/segmentations/{seg_id}/contacts?page={page}&page_size=125"
        req = urllib.request.Request(url, headers={
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        })
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
                data = json.loads(r.read())
            batch = data.get('contacts', [])
            if not batch:
                break
            contacts.extend(batch)
            if len(batch) < 125:
                break
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"  RD seg {seg_id} page {page} erro: {e}", file=sys.stderr)
            break
    return contacts


def rd_fetch_contact_full(token, uuid):
    """Busca dados completos de um contato (cf_*, telefone)."""
    url = f"{RD_MKT_BASE}/platform/contacts/{uuid}"
    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"    RD contact {uuid[:12]}... erro: {e}", file=sys.stderr)
        return {}


def rd_fetch_contact_conversions(token, uuid):
    """Busca eventos de conversão do contato (para saber onde converteu)."""
    url = f"{RD_MKT_BASE}/platform/contacts/{uuid}/events?event_type=CONVERSION&page=1&page_size=5"
    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
            return json.loads(r.read())
    except Exception:
        return []


def fetch_qualified_leads():
    """Busca leads qualificados (>=R$50k) via RD Station segmentações + dados completos."""
    if not RD_AVAILABLE:
        return []
    token = rd_get_access_token()
    if not token:
        return []
    # Segmentações de leads qualificados (>=R$50k cripto OU tradicional)
    SEG_CRIPTO = '19356678'
    SEG_TRAD = '19356688'
    all_contacts = []
    for seg_id in [SEG_CRIPTO, SEG_TRAD]:
        contacts = rd_fetch_segmentation(token, seg_id)
        all_contacts.extend(contacts)
    # Dedup por email
    seen = set()
    unique = []
    for c in all_contacts:
        email = (c.get('email') or '').lower()
        if email and email not in seen:
            seen.add(email)
            unique.append(c)

    # Buscar dados completos (cf_*, telefone, conversão) de cada contato
    print(f"    Buscando dados completos de {len(unique)} contatos...")
    enriched = []
    for i, c in enumerate(unique):
        uuid = c.get('uuid')
        if not uuid:
            continue
        full = rd_fetch_contact_full(token, uuid)
        if full:
            full['created_at'] = c.get('created_at', full.get('created_at', ''))
            # Buscar evento de conversão (onde o lead converteu)
            events = rd_fetch_contact_conversions(token, uuid)
            if events:
                # Primeira conversão = origem do lead
                full['_conversion_event'] = events[0].get('event_identifier', '')
                full['_conversion_date'] = events[0].get('event_timestamp', '')
            else:
                full['_conversion_event'] = ''
                full['_conversion_date'] = ''
            enriched.append(full)
        if (i + 1) % 10 == 0:
            print(f"    ... {i + 1}/{len(unique)} contatos enriquecidos")
        time.sleep(0.15)  # rate limit RD

    return enriched


# ──────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ──────────────────────────────────────────────────────────────
def main():
    now = datetime.now(timezone.utc)
    today = now.strftime('%Y-%m-%d')

    # Período pós-reestruturação: 01/abr → hoje
    since_pos = '2026-04-01'
    until_pos = today

    # Período pré-reestruturação: 01/abr → 14/abr
    since_pre = '2026-04-01'
    until_pre = '2026-04-14'

    # Abril inteiro (para projeção)
    since_abr = '2026-04-01'
    until_abr = today

    days_pos = max((now - datetime(2026, 4, 1, tzinfo=timezone.utc)).days, 1)
    days_pre = 14
    days_abr = max((now - datetime(2026, 4, 1, tzinfo=timezone.utc)).days, 1)
    days_remaining = max(30 - days_abr, 0)

    print(f"{'='*60}")
    print(f"  dashboard_performance.py")
    print(f"  Período pós: {since_pos} → {until_pos} ({days_pos} dias)")
    print(f"  {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    # 1. Buscar campanhas
    print("  Buscando campanhas...")
    all_campaigns = fetch_all_campaigns()
    print(f"  {len(all_campaigns)} campanhas encontradas")

    # 2. Buscar insights por campanha (pós-reestruturação)
    print(f"\n  Buscando insights pós-reestruturação ({since_pos} → {until_pos})...")
    camp_data_pos = []
    all_ads = []

    for camp in all_campaigns:
        ins = fetch_campaign_insights(camp['id'], since_pos, until_pos)
        if not ins:
            continue
        spend = float(ins.get('spend', 0))
        leads = get_leads_from_actions(ins.get('actions'))
        cpl = round(spend / leads, 2) if leads > 0 else None

        # Determinar status
        status = camp.get('status', 'UNKNOWN')
        if status == 'ACTIVE':
            badge = 'green'
            status_label = 'Ativa'
        elif status == 'PAUSED':
            badge = 'red'
            status_label = 'Pausada'
        else:
            badge = 'gray'
            status_label = status

        # Ajustar CPL alto
        if cpl and cpl > 100:
            badge = 'orange'
            status_label = 'CPL alto'

        camp_data_pos.append({
            'id': camp['id'],
            'nome': camp.get('name', '?'),
            'spend': round(spend, 2),
            'leads': leads,
            'cpl': cpl,
            'status': status_label,
            'badge': badge,
            'impressions': int(ins.get('impressions', 0)),
            'clicks': int(ins.get('clicks', 0)),
        })

        # Buscar anúncios individuais
        ads = fetch_ads_insights(camp['id'], since_pos, until_pos)
        for ad in ads:
            ad_spend = float(ad.get('spend', 0))
            ad_leads = get_leads_from_actions(ad.get('actions'))
            if ad_spend > 0:
                all_ads.append({
                    'nome': ad.get('ad_name', '?'),
                    'campanha': camp.get('name', '?')[:30],
                    'spend': round(ad_spend, 2),
                    'leads': ad_leads,
                    'cpl': round(ad_spend / ad_leads, 2) if ad_leads > 0 else None,
                })
        time.sleep(0.2)

    # Ordenar campanhas por leads desc
    camp_data_pos.sort(key=lambda x: x['leads'], reverse=True)

    # Top 10 anúncios por leads
    all_ads.sort(key=lambda x: x['leads'], reverse=True)
    top_ads = all_ads[:10]

    # 3. Totais pós
    total_spend_pos = sum(c['spend'] for c in camp_data_pos)
    total_leads_pos = sum(c['leads'] for c in camp_data_pos)
    cpl_pos = round(total_spend_pos / total_leads_pos, 2) if total_leads_pos > 0 else 0
    spend_dia_pos = round(total_spend_pos / days_pos, 2)
    leads_dia_pos = round(total_leads_pos / days_pos, 1)

    # 4. Buscar insights pré-reestruturação (comparativo)
    print(f"\n  Buscando insights pré-reestruturação ({since_pre} → {until_pre})...")
    ins_pre = fetch_account_insights(since_pre, until_pre)
    total_spend_pre = float(ins_pre.get('spend', 0))
    total_leads_pre = get_leads_from_actions(ins_pre.get('actions'))
    cpl_pre = round(total_spend_pre / total_leads_pre, 2) if total_leads_pre > 0 else 0
    spend_dia_pre = round(total_spend_pre / days_pre, 2)
    leads_dia_pre = round(total_leads_pre / days_pre, 1)

    # Variações
    var_cpl = round((cpl_pos - cpl_pre) / cpl_pre * 100) if cpl_pre > 0 else 0
    var_spend = round((spend_dia_pos - spend_dia_pre) / spend_dia_pre * 100) if spend_dia_pre > 0 else 0
    var_leads = round((leads_dia_pos - leads_dia_pre) / leads_dia_pre * 100) if leads_dia_pre > 0 else 0

    # 5. Projeção abril
    ins_abr = fetch_account_insights(since_abr, until_abr)
    total_spend_abr = float(ins_abr.get('spend', 0))
    budget_restante = max(BUDGET_META_MENSAL - total_spend_abr, 0)
    budget_dia_necessario = round(budget_restante / days_remaining) if days_remaining > 0 else 0

    # 6. Leads qualificados (RD Station)
    print("\n  Buscando leads qualificados (RD Marketing)...")
    qualified = fetch_qualified_leads()
    # Filtrar por data (pós 15/abr)
    qualified_pos = []
    for c in qualified:
        created = c.get('created_at', '')
        if created >= since_pos:
            # Nomes canônicos dos cf_* no RD (confirmados via API)
            pat_cripto = c.get('cf_que_otimo_agora_preciso_entender_qual_seu_patrimonio_ho') or '-'
            pat_trad = c.get('cf_qual_seu_patrimonio_investido_no_mercado_tradicional') or '-'
            nome = c.get('name', '?')
            tel = c.get('personal_phone') or c.get('mobile_phone') or '-'
            # Fonte: evento de conversão (onde o lead converteu)
            fonte = c.get('_conversion_event') or c.get('cf_utm_campaign') or 'Desconhecido'
            # Classificação de qualificação
            faixas_altas = ['Entre R$ 50 mil a R$ 200 mil', 'Entre R$ 200 mil e R$500 mil', 'Acima de R$500 mil']
            pat_class = 'qual-high' if (pat_cripto in faixas_altas or pat_trad in faixas_altas) else 'qual-mid'
            # Data de conversão
            conv_date_raw = c.get('_conversion_date') or created
            try:
                dt = datetime.fromisoformat(conv_date_raw.replace('Z', '+00:00'))
                conv_date = dt.strftime('%d/%m/%Y %H:%M')
            except Exception:
                conv_date = conv_date_raw[:16] if conv_date_raw else '-'
            qualified_pos.append({
                'nome': nome,
                'tel': tel,
                'patCripto': pat_cripto,
                'patTrad': pat_trad,
                'fonte': str(fonte)[:30],
                'patClass': pat_class,
                'convDate': conv_date,
            })
    print(f"  {len(qualified_pos)} leads qualificados no período pós")

    # 7. Gerar HTML
    print("\n  Gerando HTML...")
    html = generate_html(
        camp_data=camp_data_pos,
        top_ads=top_ads,
        total_spend_pos=total_spend_pos,
        total_leads_pos=total_leads_pos,
        cpl_pos=cpl_pos,
        spend_dia_pos=spend_dia_pos,
        leads_dia_pos=leads_dia_pos,
        spend_dia_pre=spend_dia_pre,
        leads_dia_pre=leads_dia_pre,
        cpl_pre=cpl_pre,
        var_cpl=var_cpl,
        var_spend=var_spend,
        var_leads=var_leads,
        total_spend_abr=total_spend_abr,
        budget_restante=budget_restante,
        days_remaining=days_remaining,
        budget_dia_necessario=budget_dia_necessario,
        qualified=qualified_pos,
        since_pos=since_pos,
        until_pos=until_pos,
        days_pos=days_pos,
        now=now,
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'index.html')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n  Dashboard salvo em: {output_path}")
    print(f"  Campanhas: {len(camp_data_pos)}")
    print(f"  Spend pós: {fmt_brl(total_spend_pos)}")
    print(f"  Leads pós: {total_leads_pos}")
    print(f"  CPL pós: {fmt_brl(cpl_pos)}")
    print(f"  Variação CPL: {var_cpl:+d}%")


# ──────────────────────────────────────────────────────────────
# GERADOR HTML
# ──────────────────────────────────────────────────────────────
def generate_html(**d):
    """Gera o HTML completo do dashboard."""

    def color_var(val):
        return 'var(--green)' if val < 0 else 'var(--red)' if val > 0 else 'var(--gold)'

    def cpl_color(cpl):
        if cpl is None:
            return 'var(--dim)'
        if cpl <= 30:
            return 'var(--green)'
        if cpl <= 50:
            return 'var(--gold)'
        if cpl <= 80:
            return 'var(--orange)'
        return 'var(--red)'

    # Campanhas rows
    camp_rows = ''
    for c in d['camp_data']:
        cpl_display = fmt_brl(c['cpl']) if c['cpl'] else '-'
        camp_rows += f"""<tr>
<td style="font-weight:600">{c['nome'][:50]}</td>
<td style="text-align:right">{fmt_brl(c['spend'])}</td>
<td style="text-align:right;font-weight:700;color:{'var(--green)' if c['leads'] > 0 else 'var(--dim)'}">{c['leads']}</td>
<td style="text-align:right;font-weight:700;color:{cpl_color(c['cpl'])}">{cpl_display}</td>
<td><span class="badge badge-{c['badge']}">{c['status']}</span></td>
</tr>"""

    camp_rows += f"""<tr style="border-top:2px solid #444">
<td><strong style="color:var(--white)">TOTAL</strong></td>
<td style="text-align:right;color:var(--white)"><strong>{fmt_brl(d['total_spend_pos'])}</strong></td>
<td style="text-align:right;color:var(--green)"><strong>{d['total_leads_pos']}</strong></td>
<td style="text-align:right;color:var(--green)"><strong>{fmt_brl(d['cpl_pos'])}</strong></td>
<td></td>
</tr>"""

    # Anúncios rows
    ads_rows = ''
    for a in d['top_ads']:
        cpl_display = fmt_brl(a['cpl']) if a['cpl'] else '-'
        ads_rows += f"""<tr>
<td style="font-weight:600">{a['nome'][:40]}</td>
<td><span class="badge badge-blue">{a['campanha'][:25]}</span></td>
<td style="text-align:right">{fmt_brl(a['spend'])}</td>
<td style="text-align:right;font-weight:700;color:var(--green)">{a['leads']}</td>
<td style="text-align:right;font-weight:700;color:{cpl_color(a['cpl'])}">{cpl_display}</td>
</tr>"""

    # Leads qualificados rows
    qual_rows = ''
    for q in d['qualified']:
        # Formatar telefone
        tel = q['tel']
        digits = ''.join(c for c in tel if c.isdigit())
        if len(digits) == 13 and digits.startswith('55'):
            tel_fmt = f"({digits[2:4]}) {digits[4:9]}-{digits[9:]}"
        elif len(digits) == 12 and digits.startswith('55'):
            tel_fmt = f"({digits[2:4]}) {digits[4:8]}-{digits[8:]}"
        elif len(digits) == 11:
            tel_fmt = f"({digits[0:2]}) {digits[2:7]}-{digits[7:]}"
        else:
            tel_fmt = tel

        pat_class = q.get('patClass', 'qual-mid')
        pat_trad_style = f'class="qual-badge {pat_class}"' if q['patTrad'] != '-' else 'style="color:var(--dim)"'
        qual_rows += f"""<tr>
<td style="font-weight:600">{q['nome']}</td>
<td style="font-size:11px">{tel_fmt}</td>
<td><span class="qual-badge {pat_class}">{q['patCripto']}</span></td>
<td><span {pat_trad_style}>{q['patTrad']}</span></td>
<td><span class="badge badge-blue">{q['fonte']}</span></td>
<td style="font-size:11px;color:var(--dim)">{q['convDate']}</td>
</tr>"""

    if not qual_rows:
        qual_rows = '<tr><td colspan="6" style="text-align:center;color:var(--dim)">Nenhum lead qualificado no período</td></tr>'

    updated = d['now'].strftime('%d/%m/%Y %H:%M UTC')
    since_fmt = datetime.strptime(d['since_pos'], '%Y-%m-%d').strftime('%d/%m')
    until_fmt = datetime.strptime(d['until_pos'], '%Y-%m-%d').strftime('%d/%m')

    return f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Performance Pos-Reestruturacao | {since_fmt}-{until_fmt}/2026 | Boost Research</title>
<style>
:root{{--bg:#0a0a0a;--card:#141414;--border:#222;--green:#00C853;--gold:#FFD600;--blue:#448AFF;--purple:#B388FF;--orange:#FF9100;--text:#E0E0E0;--dim:#666;--white:#fff;--red:#FF5252}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--text);padding:24px}}
h1{{font-size:24px;color:var(--white);margin-bottom:4px}}
.sub{{color:var(--dim);font-size:13px;margin-bottom:24px}}
.stats{{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:24px}}
.stat{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px;text-align:center}}
.sv{{font-size:26px;font-weight:800}}.sl{{font-size:10px;color:var(--dim);margin-top:4px;text-transform:uppercase}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}}
.panel{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:24px}}
.panel h2{{font-size:14px;color:var(--white);margin-bottom:16px;text-transform:uppercase;letter-spacing:1px}}
.info{{background:#1a1a00;border:1px solid #333300;border-radius:8px;padding:14px 18px;margin-bottom:24px;font-size:13px;line-height:1.6}}
.info strong{{color:var(--gold)}}
.badge{{display:inline-block;padding:3px 8px;border-radius:4px;font-size:10px;font-weight:600}}
.badge-green{{background:rgba(0,200,83,.15);color:var(--green)}}
.badge-blue{{background:rgba(68,138,255,.15);color:var(--blue)}}
.badge-gold{{background:rgba(255,214,0,.15);color:var(--gold)}}
.badge-purple{{background:rgba(179,136,255,.15);color:var(--purple)}}
.badge-orange{{background:rgba(255,145,0,.15);color:var(--orange)}}
.badge-gray{{background:rgba(255,255,255,.08);color:var(--dim)}}
.badge-red{{background:rgba(255,82,82,.15);color:var(--red)}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
thead th{{background:#1a1a1a;color:var(--dim);padding:8px 10px;text-align:left;font-size:10px;text-transform:uppercase;position:sticky;top:0}}
tbody td{{padding:8px 10px;border-bottom:1px solid var(--border)}}tbody tr:hover{{background:#1a1a1a}}
.comp-grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}}
.comp-card{{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:16px;text-align:center}}
.comp-card .label{{font-size:10px;color:var(--dim);text-transform:uppercase;margin-bottom:8px}}
.comp-card .values{{display:flex;justify-content:center;gap:24px;margin-bottom:6px}}
.comp-card .val{{font-size:20px;font-weight:800}}
.comp-card .tag{{font-size:9px;color:var(--dim);text-transform:uppercase}}
.comp-card .delta{{font-size:14px;font-weight:700;margin-top:4px}}
.proj-grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}}
.proj-item{{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center}}
.proj-item .pv{{font-size:20px;font-weight:800;color:var(--white)}}
.proj-item .pl{{font-size:9px;color:var(--dim);text-transform:uppercase;margin-top:4px}}
.qual-badge{{display:inline-block;padding:2px 6px;border-radius:3px;font-size:10px;font-weight:600;background:rgba(0,200,83,.15);color:var(--green)}}
.bar-row{{display:flex;align-items:center;gap:8px;margin-bottom:8px}}
.bar-label{{font-size:11px;color:var(--text);min-width:200px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.bar-track{{flex:1;height:22px;background:#1a1a1a;border-radius:4px;overflow:hidden}}
.bar-fill{{height:100%;border-radius:4px;display:flex;align-items:center;padding-left:8px;font-size:10px;font-weight:700;color:#000;min-width:25px}}
@media(max-width:900px){{.stats{{grid-template-columns:repeat(3,1fr)}}.grid2{{grid-template-columns:1fr}}.comp-grid{{grid-template-columns:1fr}}.proj-grid{{grid-template-columns:repeat(3,1fr)}}.creative-grid{{grid-template-columns:1fr}}}}
@media(max-width:600px){{.stats{{grid-template-columns:repeat(2,1fr)}}.proj-grid{{grid-template-columns:repeat(2,1fr)}}}}
</style></head><body>

<h1>Performance Pos-Reestruturacao | {since_fmt}-{until_fmt}/2026</h1>
<p class="sub">{d['days_pos']} dias apos ajustes de 14/abr | Meta R$40k abril | Boost Research | Atualizado: {updated}</p>

<!-- Stats Bar -->
<div class="stats">
<div class="stat"><div class="sv" style="color:var(--white)">{fmt_brl_int(d['total_spend_pos'])}</div><div class="sl">Spend Periodo</div></div>
<div class="stat"><div class="sv" style="color:var(--green)">{d['total_leads_pos']}</div><div class="sl">Leads</div></div>
<div class="stat"><div class="sv" style="color:var(--green)">{fmt_brl(d['cpl_pos'])}</div><div class="sl">CPL</div></div>
<div class="stat"><div class="sv" style="color:var(--white)">{fmt_brl_int(d['spend_dia_pos'])}</div><div class="sl">Spend/dia</div></div>
<div class="stat"><div class="sv" style="color:{color_var(d['var_cpl'])}">{d['var_cpl']:+d}%</div><div class="sl">Variacao CPL</div></div>
<div class="stat"><div class="sv" style="color:var(--blue)">{d['leads_dia_pos']:.0f}</div><div class="sl">Leads/dia</div></div>
</div>

<!-- Comparativo -->
<div class="panel">
<h2>Comparativo Antes vs Depois da Reestruturacao</h2>
<div class="comp-grid">
<div class="comp-card">
<div class="label">Spend/dia</div>
<div class="values">
<div><div class="val" style="color:var(--dim)">{fmt_brl_int(d['spend_dia_pre'])}</div><div class="tag">Antes (01-14/abr)</div></div>
<div><div class="val" style="color:var(--white)">{fmt_brl_int(d['spend_dia_pos'])}</div><div class="tag">Depois ({since_fmt}+)</div></div>
</div>
<div class="delta" style="color:{color_var(d['var_spend'])}">{d['var_spend']:+d}%</div>
</div>
<div class="comp-card">
<div class="label">Leads/dia</div>
<div class="values">
<div><div class="val" style="color:var(--dim)">{d['leads_dia_pre']:.0f}</div><div class="tag">Antes (01-14/abr)</div></div>
<div><div class="val" style="color:var(--white)">{d['leads_dia_pos']:.0f}</div><div class="tag">Depois ({since_fmt}+)</div></div>
</div>
<div class="delta" style="color:{color_var(d['var_leads'])}">{d['var_leads']:+d}%</div>
</div>
<div class="comp-card">
<div class="label">CPL</div>
<div class="values">
<div><div class="val" style="color:var(--dim)">{fmt_brl(d['cpl_pre'])}</div><div class="tag">Antes (01-14/abr)</div></div>
<div><div class="val" style="color:var(--white)">{fmt_brl(d['cpl_pos'])}</div><div class="tag">Depois ({since_fmt}+)</div></div>
</div>
<div class="delta" style="color:{color_var(d['var_cpl'])}">{d['var_cpl']:+d}%</div>
</div>
</div>
</div>

<!-- Campanhas Table -->
<div class="panel">
<h2>Campanhas — {since_fmt} a {until_fmt}/2026</h2>
<table>
<thead><tr><th>Campanha</th><th style="text-align:right">Spend</th><th style="text-align:right">Leads</th><th style="text-align:right">CPL</th><th>Status</th></tr></thead>
<tbody>{camp_rows}</tbody>
</table>
</div>

<!-- Top Anuncios Table -->
<div class="panel">
<h2>Top 10 Anuncios — {since_fmt} a {until_fmt}/2026</h2>
<table>
<thead><tr><th>Anuncio</th><th>Campanha</th><th style="text-align:right">Spend</th><th style="text-align:right">Leads</th><th style="text-align:right">CPL</th></tr></thead>
<tbody>{ads_rows}</tbody>
</table>
</div>

<!-- Projecao -->
<div class="panel">
<h2>Projecao Abril 2026</h2>
<div class="proj-grid">
<div class="proj-item"><div class="pv" style="color:var(--white)">{fmt_brl_int(d['total_spend_abr'])}</div><div class="pl">Gasto ate agora</div></div>
<div class="proj-item"><div class="pv" style="color:var(--gold)">R$40.000</div><div class="pl">Meta Abril</div></div>
<div class="proj-item"><div class="pv" style="color:var(--green)">{fmt_brl_int(d['budget_restante'])}</div><div class="pl">Restante</div></div>
<div class="proj-item"><div class="pv" style="color:var(--blue)">{d['days_remaining']}</div><div class="pl">Dias restantes</div></div>
<div class="proj-item"><div class="pv" style="color:var(--orange)">{fmt_brl_int(d['budget_dia_necessario'])}</div><div class="pl">Budget/dia necessario</div></div>
</div>
</div>

<!-- Leads Qualificados -->
<div class="panel">
<h2>Leads Qualificados >= 50k ({len(d['qualified'])} leads)</h2>
<table>
<thead><tr><th>Nome</th><th>Telefone</th><th>Pat. Cripto</th><th>Pat. Tradicional</th><th>Fonte</th><th>Data Conversao</th></tr></thead>
<tbody>{qual_rows}</tbody>
</table>
</div>

<div class="info">
<strong>Atualizado automaticamente a cada 2 horas via GitHub Actions.</strong><br>
Dados: Meta Ads API + RD Station Marketing (segmentacoes qualificados >= R$50k).<br>
Referencia de reestruturacao: 14/abr/2026 — C4 CONV e IR-LP pausadas, budget redistribuido.
</div>

</body></html>"""


if __name__ == '__main__':
    main()
