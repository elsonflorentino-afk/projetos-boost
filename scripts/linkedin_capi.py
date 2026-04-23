"""
linkedin_capi.py
LinkedIn Conversions API (CAPI) — envio de eventos de conversão server-side.

Uso:
  # Enviar evento único (lead)
  python linkedin_capi.py send --email "lead@email.com" --event-id "rd-12345"

  # Enviar evento com valor
  python linkedin_capi.py send --email "lead@email.com" --event-id "rd-12345" --value 50.0

  # Enviar batch de eventos (JSON file)
  python linkedin_capi.py batch --file events.json

  # Listar regras de conversão da conta
  python linkedin_capi.py list-rules

  # Criar regra de conversão
  python linkedin_capi.py create-rule --name "Boost Lead" --type LEAD

  # Associar regra a todas as campanhas
  python linkedin_capi.py associate --rule-id 123456 --all-campaigns

  # Testar conexão
  python linkedin_capi.py test

Variáveis de ambiente obrigatórias:
  LINKEDIN_ACCESS_TOKEN     — OAuth2 token (scope: rw_conversions, r_ads)
  LINKEDIN_AD_ACCOUNT_ID    — ID da conta (ex: 518857520)

Variáveis opcionais:
  LINKEDIN_CONVERSION_RULE_ID — ID da regra de conversão (para send/batch)
"""
import hashlib
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LINKEDIN_VERSION = "202604"
BASE_URL = "https://api.linkedin.com/rest"

ACCESS_TOKEN = os.environ.get("LINKEDIN_ACCESS_TOKEN", "")
AD_ACCOUNT_ID = os.environ.get("LINKEDIN_AD_ACCOUNT_ID", "")
CONVERSION_RULE_ID = os.environ.get("LINKEDIN_CONVERSION_RULE_ID", "")

if not ACCESS_TOKEN:
    raise SystemExit("ERRO: defina LINKEDIN_ACCESS_TOKEN no ambiente")
if not AD_ACCOUNT_ID:
    raise SystemExit("ERRO: defina LINKEDIN_AD_ACCOUNT_ID no ambiente")

# SSL context (mesmo padrão dos outros scripts Boost)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sha256_email(email: str) -> str:
    """Normaliza (lowercase + strip) e gera SHA-256 hex do email."""
    return hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()


def _headers(extra: dict = None) -> dict:
    h = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "LinkedIn-Version": LINKEDIN_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
    }
    if extra:
        h.update(extra)
    return h


def _request(method: str, path: str, data: dict = None, extra_headers: dict = None) -> dict:
    """Faz request à API do LinkedIn."""
    url = f"{BASE_URL}{path}"
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=_headers(extra_headers), method=method)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            raw = resp.read()
            if not raw:
                return {"status": resp.status, "ok": True}
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        print(f"  ERRO HTTP {e.code}: {error_body}", file=sys.stderr)
        raise


def _account_urn() -> str:
    return f"urn:li:sponsoredAccount:{AD_ACCOUNT_ID}"


def _conversion_urn(rule_id: str = None) -> str:
    rid = rule_id or CONVERSION_RULE_ID
    if not rid:
        raise SystemExit("ERRO: defina LINKEDIN_CONVERSION_RULE_ID ou passe --rule-id")
    return f"urn:lla:llaPartnerConversion:{rid}"


# ---------------------------------------------------------------------------
# Conversion Event — Single
# ---------------------------------------------------------------------------
def build_event(
    email: str,
    event_id: str,
    rule_id: str = None,
    li_fat_id: str = None,
    value: float = None,
    currency: str = "BRL",
    first_name: str = None,
    last_name: str = None,
    country_code: str = "BR",
    happened_at_ms: int = None,
) -> dict:
    """Constrói payload de um evento de conversão."""
    if happened_at_ms is None:
        happened_at_ms = int(time.time() * 1000)

    user_ids = [{"idType": "SHA256_EMAIL", "idValue": sha256_email(email)}]
    if li_fat_id:
        user_ids.append({
            "idType": "LINKEDIN_FIRST_PARTY_ADS_TRACKING_UUID",
            "idValue": li_fat_id,
        })

    event = {
        "conversion": _conversion_urn(rule_id),
        "conversionHappenedAt": happened_at_ms,
        "user": {"userIds": user_ids},
        "eventId": event_id,
    }

    if value is not None:
        event["conversionValue"] = {
            "currencyCode": currency,
            "amount": str(value),
        }

    if first_name and last_name:
        event["user"]["userInfo"] = {
            "firstName": first_name,
            "lastName": last_name,
            "countryCode": country_code,
        }

    return event


def send_event(event: dict) -> dict:
    """Envia um único evento de conversão."""
    return _request("POST", "/conversionEvents", data=event)


def send_batch(events: list) -> dict:
    """Envia batch de eventos (max 5000)."""
    if len(events) > 5000:
        raise ValueError(f"Batch máximo é 5000, recebeu {len(events)}")
    return _request(
        "POST",
        "/conversionEvents",
        data={"elements": events},
        extra_headers={"X-RestLi-Method": "BATCH_CREATE"},
    )


# ---------------------------------------------------------------------------
# Conversion Rules — CRUD
# ---------------------------------------------------------------------------
def list_rules() -> list:
    """Lista regras de conversão da conta."""
    account_urn = urllib.parse.quote(_account_urn(), safe="")
    path = f"/conversions?q=account&account={account_urn}"
    result = _request("GET", path)
    return result.get("elements", [])


def create_rule(
    name: str,
    conv_type: str = "LEAD",
    post_click_window: int = 90,
    view_through_window: int = 90,
    attribution: str = "LAST_TOUCH_BY_CAMPAIGN",
) -> dict:
    """Cria regra de conversão via API."""
    payload = {
        "name": name,
        "account": _account_urn(),
        "conversionMethod": "CONVERSIONS_API",
        "type": conv_type,
        "postClickAttributionWindowSize": post_click_window,
        "viewThroughAttributionWindowSize": view_through_window,
        "attributionType": attribution,
    }
    return _request("POST", "/conversions", data=payload)


def associate_all_campaigns(rule_id: str) -> dict:
    """Associa regra a todas as campanhas da conta."""
    urn = urllib.parse.quote(_conversion_urn(rule_id), safe="")
    path = f"/conversions/{urn}?autoAssociationType=ALL_CAMPAIGNS"
    return _request("POST", path)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------
def test_connection() -> bool:
    """Testa conexão listando regras."""
    try:
        rules = list_rules()
        print(f"  Conexão OK — {len(rules)} regra(s) de conversão encontrada(s)")
        for r in rules:
            status = "ativo" if r.get("enabled", True) else "inativo"
            print(f"    - [{status}] {r.get('name', '?')} (id: {r.get('id', '?')}, tipo: {r.get('type', '?')})")
        return True
    except Exception as e:
        print(f"  ERRO: {e}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(args: list) -> dict:
    """Parser simples de argumentos CLI."""
    parsed = {}
    i = 0
    while i < len(args):
        if args[i].startswith("--"):
            key = args[i][2:].replace("-", "_")
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                parsed[key] = args[i + 1]
                i += 2
            else:
                parsed[key] = True
                i += 1
        else:
            i += 1
    return parsed


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    command = sys.argv[1]
    opts = _parse_args(sys.argv[2:])

    if command == "test":
        ok = test_connection()
        sys.exit(0 if ok else 1)

    elif command == "list-rules":
        rules = list_rules()
        print(json.dumps(rules, indent=2, ensure_ascii=False))

    elif command == "create-rule":
        name = opts.get("name", f"Conversion - {datetime.now().strftime('%b %d, %Y')}")
        conv_type = opts.get("type", "LEAD")
        result = create_rule(name=name, conv_type=conv_type)
        print(f"  Regra criada: {json.dumps(result, indent=2)}")

    elif command == "associate":
        rule_id = opts.get("rule_id")
        if not rule_id:
            raise SystemExit("ERRO: --rule-id obrigatório")
        if opts.get("all_campaigns"):
            result = associate_all_campaigns(rule_id)
            print(f"  Associação feita: {result}")
        else:
            print("  Use --all-campaigns para associar a todas as campanhas")

    elif command == "send":
        email = opts.get("email")
        event_id = opts.get("event_id")
        if not email:
            raise SystemExit("ERRO: --email obrigatório")
        if not event_id:
            event_id = f"manual-{int(time.time())}"
            print(f"  event_id gerado: {event_id}")

        event = build_event(
            email=email,
            event_id=event_id,
            rule_id=opts.get("rule_id"),
            li_fat_id=opts.get("li_fat_id"),
            value=float(opts["value"]) if opts.get("value") else None,
            first_name=opts.get("first_name"),
            last_name=opts.get("last_name"),
        )
        result = send_event(event)
        print(f"  Evento enviado: {result}")

    elif command == "batch":
        filepath = opts.get("file")
        if not filepath:
            raise SystemExit("ERRO: --file obrigatório (JSON com array de eventos)")
        with open(filepath) as f:
            raw_events = json.load(f)

        events = []
        for item in raw_events:
            ev = build_event(
                email=item["email"],
                event_id=item.get("event_id", f"batch-{int(time.time())}-{len(events)}"),
                rule_id=item.get("rule_id"),
                li_fat_id=item.get("li_fat_id"),
                value=item.get("value"),
                first_name=item.get("first_name"),
                last_name=item.get("last_name"),
                happened_at_ms=item.get("happened_at_ms"),
            )
            events.append(ev)

        print(f"  Enviando batch com {len(events)} evento(s)...")
        result = send_batch(events)
        print(f"  Batch enviado: {json.dumps(result, indent=2)}")

    else:
        print(f"Comando desconhecido: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
