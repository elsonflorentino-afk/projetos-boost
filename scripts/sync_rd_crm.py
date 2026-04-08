#!/usr/bin/env python3
"""
sync_rd_crm.py — Sincroniza deals do RD Station CRM para Supabase.

Modos:
  --all          Backfill completo (todos os 875+ deals)
  --since-hours N  Sincroniza deals atualizados nas últimas N horas (default: 48)

Env vars (obrigatórias):
  RD_CRM_TOKEN                 Token de instância do RD CRM
  SUPABASE_URL                 URL do projeto Supabase
  SUPABASE_SERVICE_ROLE_KEY    Service role key (bypassa RLS)

Uso local:
  export RD_CRM_TOKEN=...
  export SUPABASE_URL=...
  export SUPABASE_SERVICE_ROLE_KEY=...
  python3 scripts/sync_rd_crm.py --since-hours 48

Uso no GitHub Actions:
  Configurado em .github/workflows/sync-rd-crm.yml
"""
import os
import sys
import time
import argparse
import requests
from datetime import datetime, timedelta, timezone

RD_TOKEN = os.environ.get("RD_CRM_TOKEN")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not all([RD_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    print("ERROR: env vars missing (RD_CRM_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)")
    sys.exit(1)

RD_BASE = "https://crm.rdstation.com/api/v1"

HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}


def status_from_win(win, closed_at):
    if win is True:
        return "won"
    if win is False and closed_at:
        return "lost"
    return "open"


def safe_get(d, *keys):
    for k in keys:
        if d is None:
            return None
        d = d.get(k) if isinstance(d, dict) else None
    return d


def build_row(deal, contact_info):
    closed = deal.get("closed_at")
    status = status_from_win(deal.get("win"), closed)

    email = None
    name = None
    if contact_info and contact_info.get("contacts"):
        c0 = contact_info["contacts"][0]
        name = c0.get("name")
        if c0.get("emails"):
            em = c0["emails"][0].get("email", "")
            email = em.strip().lower() or None

    return {
        "id": deal.get("_id") or deal.get("id"),
        "name": deal.get("name"),
        "pipeline_id": safe_get(deal, "deal_pipeline", "id"),
        "pipeline_name": safe_get(deal, "deal_pipeline", "name"),
        "stage_id": safe_get(deal, "deal_stage", "_id") or safe_get(deal, "deal_stage", "id"),
        "stage_name": safe_get(deal, "deal_stage", "name"),
        "deal_status": status,
        "win": deal.get("win"),
        "deal_lost_note": deal.get("deal_lost_note"),
        "amount_total": float(deal.get("amount_total") or 0),
        "amount_unique": float(deal.get("amount_unique") or 0),
        "amount_monthly": float(deal.get("amount_montly") or 0),  # typo do RD
        "interactions": int(deal.get("interactions") or 0),
        "rating": deal.get("rating"),
        "owner_id": safe_get(deal, "user", "_id") or safe_get(deal, "user", "id"),
        "owner_name": safe_get(deal, "user", "name"),
        "crm_created_at": deal.get("created_at"),
        "crm_updated_at": deal.get("updated_at"),
        "closed_at": closed,
        "contact_email": email,
        "contact_name": name,
    }


def fetch_deals(since_hours=None, fetch_all=False):
    """Pagina /deals e retorna apenas os atualizados no período ou todos."""
    cutoff = None
    if not fetch_all and since_hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    all_deals = []
    page = 1
    while True:
        params = {"token": RD_TOKEN, "limit": 100, "page": page}
        r = requests.get(f"{RD_BASE}/deals", params=params, timeout=30)
        if r.status_code != 200:
            print(f"ERROR listing page {page}: {r.status_code} {r.text[:200]}")
            break
        d = r.json()
        deals = d.get("deals", [])
        if not deals:
            break

        if cutoff is None:
            all_deals.extend(deals)
        else:
            # Filtra por updated_at (listagem vem ordenada desc por update)
            stop = False
            for deal in deals:
                updated = deal.get("updated_at")
                if not updated:
                    continue
                try:
                    dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    if dt >= cutoff:
                        all_deals.append(deal)
                    else:
                        stop = True
                        break
                except Exception:
                    continue
            if stop:
                break

        print(f"  Page {page}: +{len(deals)} (total processados {page*100})")
        if not d.get("has_more"):
            break
        page += 1
        time.sleep(0.1)

    return all_deals


def fetch_contact(deal_id):
    try:
        r = requests.get(
            f"{RD_BASE}/deals/{deal_id}/contacts",
            params={"token": RD_TOKEN},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"  contact fetch error for {deal_id}: {e}")
    return None


def upsert_batch(rows):
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/deals",
            headers=HEADERS_SB,
            json=rows,
            timeout=30,
        )
        if r.status_code in (200, 201):
            return len(rows), 0
        print(f"  BATCH ERROR {r.status_code}: {r.text[:200]}")
        return 0, len(rows)
    except Exception as e:
        print(f"  BATCH EXCEPTION: {e}")
        return 0, len(rows)


def link_deals_to_leads():
    """Roda o UPDATE SQL pra linkar deals.lead_id via email match."""
    sql = """
    UPDATE deals d
    SET lead_id = l.id
    FROM leads l
    WHERE lower(d.contact_email) = lower(l.email)
      AND d.lead_id IS NULL;
    """
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/exec_sql",
        headers=HEADERS_SB,
        json={"query": sql},
        timeout=30,
    )
    # exec_sql may not exist; fallback to PATCH via filter
    if r.status_code == 404:
        print("  (link via RPC não disponível, use UPDATE manual no SQL Editor)")
        return
    print(f"  Link deals→leads: {r.status_code}")


def refresh_leads_crm_columns():
    """Atualiza leads.crm_* baseado no deal mais recente de cada lead."""
    # Sem SQL custom via REST, isso precisa ser feito via trigger ou SQL direto.
    # Por enquanto apenas imprimir lembrete.
    print("  (refresh leads.crm_* — rodar SQL manual pela primeira vez, depois cron)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Backfill completo")
    parser.add_argument("--since-hours", type=int, default=48, help="Últimas N horas (default: 48)")
    parser.add_argument("--batch-size", type=int, default=25)
    args = parser.parse_args()

    mode = "BACKFILL (all)" if args.all else f"INCREMENTAL (last {args.since_hours}h)"
    print(f"=== sync_rd_crm.py | mode={mode} ===")
    print(f"[{datetime.now(timezone.utc).isoformat()}] Start")

    deals = fetch_deals(
        since_hours=None if args.all else args.since_hours,
        fetch_all=args.all,
    )
    print(f"Deals to sync: {len(deals)}")
    print()

    synced = 0
    failed = 0
    no_email = 0
    batch = []

    for i, deal in enumerate(deals, 1):
        deal_id = deal.get("_id") or deal.get("id")
        contact = fetch_contact(deal_id)
        row = build_row(deal, contact)
        if not row["contact_email"]:
            no_email += 1
        batch.append(row)

        if len(batch) >= args.batch_size or i == len(deals):
            s, f = upsert_batch(batch)
            synced += s
            failed += f
            print(f"  Progress: {i}/{len(deals)} | synced={synced} failed={failed} sem_email={no_email}")
            batch = []

        time.sleep(0.1)

    print()
    print("=" * 60)
    print(f"RESULT: total={len(deals)} synced={synced} failed={failed} sem_email={no_email}")
    print(f"[{datetime.now(timezone.utc).isoformat()}] Done")
    print("=" * 60)

    # Exit code não-zero se houve falhas
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
