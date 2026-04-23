#!/usr/bin/env python3
"""
sync_linkedin_capi.py — Sincroniza leads do RD Station CRM para LinkedIn Conversions API.

Lê deals recentes do RD CRM, identifica leads novos (não enviados anteriormente)
e envia eventos de conversão para o LinkedIn CAPI.

Modos:
  --since-hours N   Deals atualizados nas últimas N horas (default: 72)
  --dry-run         Mostra o que seria enviado sem enviar
  --force           Reenvia todos, ignorando cache
  --backfill-days N Envia deals dos últimos N dias (para primeiro sync)

Env vars obrigatórias:
  RD_CRM_TOKEN
  LINKEDIN_ACCESS_TOKEN
  LINKEDIN_AD_ACCOUNT_ID
  LINKEDIN_CONVERSION_RULE_ID

Uso:
  python3 scripts/sync_linkedin_capi.py --since-hours 72
  python3 scripts/sync_linkedin_capi.py --dry-run
  python3 scripts/sync_linkedin_capi.py --backfill-days 30
"""
import argparse
import hashlib
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Env vars — nunca hardcodar tokens
# ---------------------------------------------------------------------------
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN")
LINKEDIN_TOKEN = os.environ.get("LINKEDIN_ACCESS_TOKEN")
LINKEDIN_ACCOUNT = os.environ.get("LINKEDIN_AD_ACCOUNT_ID")
LINKEDIN_RULE_ID = os.environ.get("LINKEDIN_CONVERSION_RULE_ID")

REQUIRED = {
    "RD_CRM_TOKEN": RD_CRM_TOKEN,
    "LINKEDIN_ACCESS_TOKEN": LINKEDIN_TOKEN,
    "LINKEDIN_AD_ACCOUNT_ID": LINKEDIN_ACCOUNT,
    "LINKEDIN_CONVERSION_RULE_ID": LINKEDIN_RULE_ID,
}
missing = [k for k, v in REQUIRED.items() if not v]
if missing:
    raise SystemExit(f"ERRO: defina no ambiente: {', '.join(missing)}")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
RD_CRM_BASE = "https://crm.rdstation.com/api/v1"
LINKEDIN_BASE = "https://api.linkedin.com/rest"
LINKEDIN_VERSION = "202604"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
CACHE_FILE = os.path.join(REPO_ROOT, "painel", "metas-kpis", "linkedin_capi_cache.json")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


# ---------------------------------------------------------------------------
# Cache — rastreia quais deals já foram enviados
# ---------------------------------------------------------------------------
def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {"sent_deals": {}, "stats": {"total_sent": 0, "last_sync": None}}


def save_cache(cache: dict):
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    cache["stats"]["last_sync"] = datetime.now(timezone.utc).isoformat()
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# RD CRM — busca deals recentes
# ---------------------------------------------------------------------------
def fetch_deals(since_hours: int = None, backfill_days: int = None) -> list:
    """Pagina /deals e retorna deals com email."""
    cutoff = None
    if backfill_days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=backfill_days)
    elif since_hours:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    all_deals = []
    page = 1
    while True:
        params = {"token": RD_CRM_TOKEN, "limit": 100, "page": page}
        url = f"{RD_CRM_BASE}/deals?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
                data = json.loads(r.read())
        except Exception as e:
            print(f"  ERRO page {page}: {e}", file=sys.stderr)
            break

        deals = data.get("deals", [])
        if not deals:
            break

        if cutoff is None:
            all_deals.extend(deals)
        else:
            stop = False
            for deal in deals:
                updated = deal.get("updated_at") or deal.get("created_at")
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

        if not data.get("has_more"):
            break
        page += 1
        time.sleep(0.2)

    return all_deals


def fetch_deal_contact(deal_id: str) -> dict:
    """Busca contato associado ao deal."""
    url = f"{RD_CRM_BASE}/deals/{deal_id}/contacts?token={RD_CRM_TOKEN}"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
            return json.loads(r.read())
    except Exception:
        return {}


def extract_contact_info(deal: dict, contact_data: dict) -> dict:
    """Extrai email, nome do contato."""
    info = {"email": None, "first_name": None, "last_name": None}
    contacts = contact_data.get("contacts", [])
    if not contacts:
        return info

    c = contacts[0]
    if c.get("emails"):
        info["email"] = c["emails"][0].get("email", "").strip().lower() or None

    name = c.get("name", "")
    if name:
        parts = name.strip().split(" ", 1)
        info["first_name"] = parts[0]
        info["last_name"] = parts[1] if len(parts) > 1 else parts[0]

    return info


# ---------------------------------------------------------------------------
# LinkedIn CAPI — envio de eventos
# ---------------------------------------------------------------------------
def sha256_email(email: str) -> str:
    return hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()


def _li_headers(extra: dict = None) -> dict:
    h = {
        "Authorization": f"Bearer {LINKEDIN_TOKEN}",
        "Content-Type": "application/json",
        "LinkedIn-Version": LINKEDIN_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
    }
    if extra:
        h.update(extra)
    return h


def send_single_event(event: dict) -> tuple:
    """Envia evento e retorna (ok: bool, status_code: int)."""
    url = f"{LINKEDIN_BASE}/conversionEvents"
    body = json.dumps(event).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=_li_headers(), method="POST")
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
            return True, r.status
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")[:200]
        print(f"    ERRO {e.code}: {error_body}", file=sys.stderr)
        return False, e.code


def send_batch_events(events: list) -> tuple:
    """Envia batch (max 5000) e retorna (ok, status_code)."""
    url = f"{LINKEDIN_BASE}/conversionEvents"
    body = json.dumps({"elements": events}).encode("utf-8")
    headers = _li_headers({"X-RestLi-Method": "BATCH_CREATE"})
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
            return True, r.status
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")[:200]
        print(f"    BATCH ERRO {e.code}: {error_body}", file=sys.stderr)
        return False, e.code


def build_conversion_event(email: str, event_id: str,
                           first_name: str = None, last_name: str = None,
                           happened_at_ms: int = None) -> dict:
    """Monta payload de evento de conversão."""
    if happened_at_ms is None:
        happened_at_ms = int(time.time() * 1000)

    event = {
        "conversion": f"urn:lla:llaPartnerConversion:{LINKEDIN_RULE_ID}",
        "conversionHappenedAt": happened_at_ms,
        "user": {
            "userIds": [{"idType": "SHA256_EMAIL", "idValue": sha256_email(email)}]
        },
        "eventId": event_id,
    }

    if first_name and last_name:
        event["user"]["userInfo"] = {
            "firstName": first_name,
            "lastName": last_name,
            "countryCode": "BR",
        }

    return event


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Sync RD CRM deals → LinkedIn CAPI")
    parser.add_argument("--since-hours", type=int, default=72, help="Deals das últimas N horas (default: 72)")
    parser.add_argument("--backfill-days", type=int, help="Backfill dos últimos N dias")
    parser.add_argument("--dry-run", action="store_true", help="Mostra sem enviar")
    parser.add_argument("--force", action="store_true", help="Ignora cache, reenvia tudo")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size para LinkedIn (max 5000)")
    args = parser.parse_args()

    mode = "DRY-RUN" if args.dry_run else "LIVE"
    if args.backfill_days:
        period = f"backfill {args.backfill_days} dias"
    else:
        period = f"últimas {args.since_hours}h"

    print(f"{'='*60}")
    print(f"  sync_linkedin_capi.py | {mode} | {period}")
    print(f"  Regra: {LINKEDIN_RULE_ID} | Conta: {LINKEDIN_ACCOUNT}")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    # 1. Carregar cache
    cache = load_cache()
    sent_deals = cache["sent_deals"]
    print(f"  Cache: {len(sent_deals)} deals já enviados")

    # 2. Buscar deals do RD CRM
    print(f"\n  Buscando deals RD CRM ({period})...")
    deals = fetch_deals(
        since_hours=args.since_hours if not args.backfill_days else None,
        backfill_days=args.backfill_days,
    )
    print(f"  {len(deals)} deals encontrados")

    # 3. Filtrar novos (não no cache)
    new_deals = []
    skipped_no_email = 0
    skipped_cached = 0

    for deal in deals:
        deal_id = deal.get("_id") or deal.get("id")
        if not deal_id:
            continue

        if not args.force and deal_id in sent_deals:
            skipped_cached += 1
            continue

        # Buscar contato
        contact_data = fetch_deal_contact(deal_id)
        info = extract_contact_info(deal, contact_data)

        if not info["email"]:
            skipped_no_email += 1
            continue

        # Filtrar emails de teste
        email_lower = info["email"].lower()
        if any(t in email_lower for t in ["teste@", "test@", "@teste.", "@test.", "example.com"]):
            skipped_no_email += 1
            continue

        # Timestamp do deal
        created = deal.get("created_at")
        happened_ms = None
        if created:
            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                happened_ms = int(dt.timestamp() * 1000)
            except Exception:
                pass

        new_deals.append({
            "deal_id": deal_id,
            "email": info["email"],
            "first_name": info["first_name"],
            "last_name": info["last_name"],
            "happened_at_ms": happened_ms,
            "deal_name": deal.get("name", ""),
        })
        time.sleep(0.1)  # rate limit RD

    print(f"\n  Resultado filtragem:")
    print(f"    Novos para enviar: {len(new_deals)}")
    print(f"    Já enviados (cache): {skipped_cached}")
    print(f"    Sem email: {skipped_no_email}")

    if not new_deals:
        print("\n  Nada para enviar.")
        save_cache(cache)
        return

    # 4. Enviar para LinkedIn CAPI
    if args.dry_run:
        print(f"\n  [DRY-RUN] {len(new_deals)} eventos que seriam enviados:")
        for d in new_deals[:10]:
            print(f"    - {d['email']} (deal: {d['deal_id'][:12]}... | {d['deal_name'][:30]})")
        if len(new_deals) > 10:
            print(f"    ... e mais {len(new_deals) - 10}")
        return

    sent = 0
    failed = 0

    # Usar batch se > 5 eventos
    if len(new_deals) > 5:
        print(f"\n  Enviando batch de {len(new_deals)} eventos...")
        for i in range(0, len(new_deals), args.batch_size):
            chunk = new_deals[i:i + args.batch_size]
            events = []
            for d in chunk:
                ev = build_conversion_event(
                    email=d["email"],
                    event_id=f"rd-deal-{d['deal_id']}",
                    first_name=d["first_name"],
                    last_name=d["last_name"],
                    happened_at_ms=d["happened_at_ms"],
                )
                events.append(ev)

            ok, status = send_batch_events(events)
            if ok:
                sent += len(chunk)
                for d in chunk:
                    cache["sent_deals"][d["deal_id"]] = {
                        "email_hash": sha256_email(d["email"]),
                        "sent_at": datetime.now(timezone.utc).isoformat(),
                    }
                print(f"    Batch {i//args.batch_size + 1}: {len(chunk)} enviados (HTTP {status})")
            else:
                failed += len(chunk)
                print(f"    Batch {i//args.batch_size + 1}: FALHOU ({len(chunk)} eventos)")

            time.sleep(0.5)  # rate limit LinkedIn
    else:
        print(f"\n  Enviando {len(new_deals)} eventos individuais...")
        for d in new_deals:
            ev = build_conversion_event(
                email=d["email"],
                event_id=f"rd-deal-{d['deal_id']}",
                first_name=d["first_name"],
                last_name=d["last_name"],
                happened_at_ms=d["happened_at_ms"],
            )
            ok, status = send_single_event(ev)
            if ok:
                sent += 1
                cache["sent_deals"][d["deal_id"]] = {
                    "email_hash": sha256_email(d["email"]),
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                }
                print(f"    {d['email']}: OK ({status})")
            else:
                failed += 1
                print(f"    {d['email']}: FALHOU ({status})")
            time.sleep(0.3)

    # 5. Salvar cache
    cache["stats"]["total_sent"] = len(cache["sent_deals"])
    save_cache(cache)

    # 6. Resumo
    print(f"\n{'='*60}")
    print(f"  RESULTADO: enviados={sent} falhas={failed} total_cache={len(cache['sent_deals'])}")
    print(f"{'='*60}\n")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
