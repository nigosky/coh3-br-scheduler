#!/usr/bin/env python3
"""
Pré-checagem inteligente (stdlib-only, zero segredos, TLS 1.2+) para `nigosky/coh3-br-scheduler`.

Consulta diretamente a API pública da Relic (`getRecentMatchHistoryByProfileId`) das 33 contas Steam
monitoradas (cobrindo tanto Automatch ranqueadas quanto partidas Customizadas!) e decide se vale a pena
disparar (`workflow_dispatch`) o repositório privado `nigosky/coh3-br-tracker`.

Resultado:
- Checagens rápidas no agendador público (onde minutos do GitHub Actions são 100% gratuitos e ilimitados).
- Redução de ~70-85% no consumo mensal de minutos do repositório privado, mantendo detecção quase em tempo real!
"""

from __future__ import annotations

import concurrent.futures
import gzip
import json
import os
import ssl
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

RELIC_HOST = "coh3-api.reliclink.com"
BASE_URL = f"https://{RELIC_HOST}/community/leaderboard"
MAX_HTTP_BYTES = 5 * 1024 * 1024  # 5 MB limite estrito anti-Gzip-bomb
BRT_TZ = timezone(timedelta(hours=-3))
HEARTBEAT_INTERVAL_SEC = 3 * 3600  # Garante 1 disparo de segurança a cada 3h (limpeza 72h, Open Data, retries)

# As 33 contas vinculadas (32 jogadores únicos) monitoradas no CoH3 Brasil Tracker
MONITORED_PROFILES: tuple[tuple[int, str], ...] = (
    (79327, "Chacineiro"),
    (991764, "SaNgar [BR]"),
    (1088455, "luiz_fmh"),
    (735940, "Jimmy Duelista"),
    (559962, "É o Gabe"),
    (648514, "Halokoo"),
    (35952, "Sauronzinho"),
    (900358, "Margheriti"),
    (691, "Vítor"),
    (5768, "The Dog"),
    (13450, "7 DIVISAO PHANTOM"),
    (19747, "twitch.tv/luck22ai"),
    (944488, "gbytes™"),
    (21206, "hjax"),
    (509690, "Alekel"),
    (1114404, "Vikitor"),
    (366826, "Lem22"),
    (1282592, "SapoBoi_BR"),
    (639157, "Itaperuna"),
    (110078, "Bineto"),
    (1143262, "Mazzaroppee"),
    (55693, "BinhoBiceps"),
    (337051, "Maiti"),
    (1022333, "gaules"),
    (32922, "Showtaro"),
    (153969, "Cunha"),
    (557890, "É o Gabe (sec)"),
    (26910, "nigo_BR ᵀᴹ"),
    (6557, "Dragoness"),
    (1226910, "Lupus Alba"),
    (3895, "Liquid"),
    (1005730, "arc"),
    (31145, "vonMises"),
)

_TLS_LOCALS = threading.local()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        raise urllib.error.HTTPError(req.full_url, code, f"Redirecionamento bloqueado ({code})", headers, fp)


def _get_opener() -> urllib.request.OpenerDirector:
    opener = getattr(_TLS_LOCALS, "opener", None)
    if opener is None:
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx), _NoRedirectHandler())
        _TLS_LOCALS.opener = opener
    return opener


def _read_bounded_json(resp: Any) -> dict[str, Any]:
    raw_chunks: list[bytes] = []
    total = 0
    while True:
        chunk = resp.read(65536)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_HTTP_BYTES:
            raise ValueError("Resposta HTTP excedeu limite seguro de 5 MB.")
        raw_chunks.append(chunk)
    raw = b"".join(raw_chunks)

    enc = str(resp.headers.get("Content-Encoding") or "").lower()
    if "gzip" in enc or raw[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
            out_chunks: list[bytes] = []
            uz_total = 0
            while True:
                piece = gz.read(65536)
                if not piece:
                    break
                uz_total += len(piece)
                if uz_total > MAX_HTTP_BYTES:
                    raise ValueError("Payload descompactado excedeu limite seguro de 5 MB.")
                out_chunks.append(piece)
            raw = b"".join(out_chunks)
    return json.loads(raw.decode("utf-8"))


import io  # noqa: E402


def fetch_latest_player_marker(profile_id: int) -> tuple[int, int, int, bool]:
    """Retorna (profile_id, max_match_id, max_completion_time, ok) para uma conta monitorada."""
    pid = int(profile_id)
    if pid <= 0:
        return pid, 0, 0, False
    qs = urllib.parse.urlencode({"title": "coh3", "profile_id": str(pid)})
    url = f"{BASE_URL}/getRecentMatchHistoryByProfileId?{qs}"
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != RELIC_HOST:
        raise ValueError("Host fora da allowlist da Relic.")

    headers = {
        "User-Agent": "CoH3-BR-SmartScheduler/1.0",
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
    }
    for attempt in range(2):
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with _get_opener().open(req, timeout=15) as resp:
                data = _read_bounded_json(resp)
            stats = data.get("matchHistoryStats") or []
            max_mid = 0
            max_ct = 0
            for item in stats:
                if not isinstance(item, dict):
                    continue
                mid = int(item.get("id", 0) or 0)
                ct = int(item.get("completiontime", 0) or 0)
                if mid > max_mid:
                    max_mid = mid
                if ct > max_ct:
                    max_ct = ct
            return pid, max_mid, max_ct, True
        except Exception:
            if attempt == 0:
                time.sleep(0.8)
    return pid, 0, 0, False


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.is_file():
        return {}
    try:
        parsed = json.loads(state_path.read_text(encoding="utf-8"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    payload = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    fd, tmp_name = tempfile.mkstemp(prefix=".state_", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def write_github_outputs(outputs: dict[str, str]) -> None:
    gh_out = os.environ.get("GITHUB_OUTPUT", "").strip()
    if not gh_out:
        return
    with open(gh_out, "a", encoding="utf-8") as fh:
        for k, v in outputs.items():
            clean_v = str(v).replace("\r", " ").replace("\n", " ").strip()
            fh.write(f"{k}={clean_v}\n")


def evaluate_precheck(state_path: Path, force_dispatch: bool = False, dry_run: bool = False) -> tuple[bool, str, dict[str, Any]]:
    now_ts = int(time.time())
    now_brt = datetime.fromtimestamp(now_ts, tz=BRT_TZ)
    today_brt = now_brt.strftime("%Y-%m-%d")
    iso_year, iso_week, _ = now_brt.isocalendar()
    current_iso_week = f"{iso_year}-W{iso_week:02d}"

    prev = load_state(state_path)
    prev_players: dict[str, dict[str, int]] = dict(prev.get("players") or {})
    last_dispatch_ts = int(prev.get("last_dispatch_ts", 0) or 0)
    last_daily_rank_date = str(prev.get("last_daily_rank_date_brt") or "")
    last_weekly_week = str(prev.get("last_weekly_summary_iso_week") or "")

    alias_by_pid = {pid: alias for pid, alias in MONITORED_PROFILES}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(fetch_latest_player_marker, [pid for pid, _ in MONITORED_PROFILES]))

    ok_count = 0
    new_activity_players: list[str] = []
    updated_players: dict[str, dict[str, int]] = dict(prev_players)

    for pid, max_mid, max_ct, ok in results:
        key = str(pid)
        if not ok:
            continue
        ok_count += 1
        old_entry = prev_players.get(key) or {}
        old_mid = int(old_entry.get("max_match_id", 0) or 0)
        old_ct = int(old_entry.get("max_completion_time", 0) or 0)
        if prev_players and (max_mid > old_mid or max_ct > old_ct):
            new_activity_players.append(f"{alias_by_pid.get(pid, str(pid))} (#{max_mid})")
        updated_players[key] = {
            "max_match_id": max(max_mid, old_mid),
            "max_completion_time": max(max_ct, old_ct),
        }

    reasons: list[str] = []
    mark_daily_done = False
    mark_weekly_done = False

    if force_dispatch:
        reasons.append("disparo manual forcado (workflow_dispatch)")
    if not prev_players:
        reasons.append("inicializacao do baseline de estado (1a execucao)")
    if new_activity_players:
        sample = ", ".join(new_activity_players[:4])
        extra = f" +{len(new_activity_players) - 4}" if len(new_activity_players) > 4 else ""
        reasons.append(f"nova partida detectada: {sample}{extra}")
    if (now_brt.hour, now_brt.minute) >= (0, 1) and last_daily_rank_date != today_brt:
        reasons.append(f"janela do Ranking Diario ({today_brt} >= 00:01 BRT)")
        mark_daily_done = True
    if now_brt.weekday() == 0 and (now_brt.hour, now_brt.minute) >= (0, 5) and last_weekly_week != current_iso_week:
        reasons.append(f"janela do Resumo Semanal ({current_iso_week} segunda >= 00:05 BRT)")
        mark_weekly_done = True
    if ok_count == 0:
        reasons.append("fallback de seguranca (Relic API indisponivel na pre-checagem)")
    elif last_dispatch_ts <= 0 or (now_ts - last_dispatch_ts) >= HEARTBEAT_INTERVAL_SEC:
        elapsed_h = round((now_ts - last_dispatch_ts) / 3600.0, 1) if last_dispatch_ts > 0 else 0.0
        reasons.append(f"heartbeat periodico de seguranca ({elapsed_h}h desde ultimo ciclo)")

    should_dispatch = len(reasons) > 0
    reason_str = " | ".join(reasons) if reasons else f"sem novas partidas ({ok_count}/{len(MONITORED_PROFILES)} contas verificadas)"

    next_state = {
        "schema_version": 1,
        "last_checked_ts": now_ts,
        "last_dispatch_ts": now_ts if should_dispatch else last_dispatch_ts,
        "last_daily_rank_date_brt": today_brt if (should_dispatch and mark_daily_done) else (last_daily_rank_date or today_brt if not prev_players and (now_brt.hour, now_brt.minute) >= (0, 1) else last_daily_rank_date),
        "last_weekly_summary_iso_week": current_iso_week if (should_dispatch and mark_weekly_done) else last_weekly_week,
        "players": updated_players,
    }

    if should_dispatch and not dry_run:
        atomic_write_json(state_path, next_state)

    return should_dispatch, reason_str, {"ok_accounts": ok_count, "total_accounts": len(MONITORED_PROFILES)}


def main() -> int:
    repo_dir = Path(__file__).resolve().parent
    state_path = repo_dir / "state.json"
    force_env = os.environ.get("FORCE_DISPATCH", "false").strip().lower() == "true"
    dry_run = "--dry-run" in sys.argv

    should_dispatch, reason, meta = evaluate_precheck(
        state_path=state_path,
        force_dispatch=force_env or ("--force" in sys.argv),
        dry_run=dry_run,
    )
    status_word = "DISPARAR" if should_dispatch else "ECONOMIZAR (SKIP)"
    print(
        f"[PRECHECK] Decisao={status_word} | Contas Relic OK={meta['ok_accounts']}/{meta['total_accounts']} | Motivo: {reason}"
    )
    write_github_outputs(
        {
            "should_dispatch": "true" if should_dispatch else "false",
            "reason": reason,
            "ok_accounts": f"{meta['ok_accounts']}/{meta['total_accounts']}",
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
