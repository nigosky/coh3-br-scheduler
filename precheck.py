#!/usr/bin/env python3
"""
Pré-checagem inteligente v1.2 (stdlib-only, zero segredos, TLS 1.2+) para `nigosky/coh3-br-scheduler`.

1. Consulta diretamente a API pública da Relic (`getRecentMatchHistoryByProfileId`) das 33 contas Steam
   monitoradas (cobrindo tanto Automatch ranqueadas quanto partidas Customizadas!).
2. Consulta o XML público da Comunidade Steam (`?xml=1`) das mesmas 33 contas (~1,3s em paralelo) para
   detectar em tempo real quando veteranos abrem ou fecham o Company of Heroes 3 (`AppID 1677280`),
   atualizando o 2º canal (Radar de Lobby Ao Vivo) imediatamente sem exigir chave da Steam!
"""

from __future__ import annotations

import concurrent.futures
import gzip
import html
import io
import json
import os
import re
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
STEAM_HOST = "steamcommunity.com"
BASE_URL = f"https://{RELIC_HOST}/community/leaderboard"
MAX_HTTP_BYTES = 5 * 1024 * 1024  # 5 MB limite estrito anti-Gzip-bomb
MAX_STEAM_XML_BYTES = 65_536      # 64 KB limite para perfil XML da Steam
BRT_TZ = timezone(timedelta(hours=-3))
HEARTBEAT_INTERVAL_SEC = 3 * 3600       # Disparo de segurança a cada 3h
STEAM_ONLINE_DEBOUNCE_SEC = 20 * 60     # Debounce de 20 min para trocas apenas de Online na Steam (fora do CoH3)
COH3_APP_ID = "1677280"

STEAM_REDIRECT_PATH_RE = re.compile(r"^/(?:profiles/7656119\d{10}|id/[A-Za-z0-9_-]{1,64})/?$")
_ONLINE_STATE_RE = re.compile(r"<onlineState>\s*([^<\s]+)\s*</onlineState>", re.IGNORECASE)
_GAME_NAME_RE = re.compile(
    r"<gameName>\s*(?:<!\[CDATA\[(.*?)\]\]>|([^<]*))\s*</gameName>",
    re.IGNORECASE | re.DOTALL,
)
_GAME_LINK_RE = re.compile(
    r"<gameLink>\s*(?:<!\[CDATA\[(.*?)\]\]>|([^<]*))\s*</gameLink>",
    re.IGNORECASE | re.DOTALL,
)
_STATE_MSG_RE = re.compile(
    r"<stateMessage>\s*(?:<!\[CDATA\[(.*?)\]\]>|([^<]*))\s*</stateMessage>",
    re.IGNORECASE | re.DOTALL,
)
_HTML_STRIP_RE = re.compile(r"<[^>]+>")

# As 33 contas vinculadas (32 jogadores únicos): (profile_id, steam_id64, alias)
MONITORED_ACCOUNTS: tuple[tuple[int, str, str], ...] = (
    (79327, "76561197963172584", "Chacineiro"),
    (991764, "76561198115923932", "SaNgar [BR]"),
    (1088455, "76561198108568598", "luiz_fmh"),
    (735940, "76561198034510845", "Jimmy Duelista"),
    (559962, "76561198062871244", "É o Gabe"),
    (648514, "76561198372757032", "Halokoo"),
    (35952, "76561198121484554", "Sauronzinho"),
    (900358, "76561198070545853", "Margheriti"),
    (691, "76561198001604015", "Vítor"),
    (5768, "76561198205052385", "The Dog"),
    (13450, "76561198284234077", "7 DIVISAO PHANTOM"),
    (19747, "76561198058548575", "twitch.tv/luck22ai"),
    (944488, "76561198063124280", "gbytes™"),
    (21206, "76561198061434984", "hjax"),
    (509690, "76561198053226112", "Alekel"),
    (1114404, "76561198024118058", "Vikitor"),
    (366826, "76561198014985380", "Lem22"),
    (1282592, "76561198028644178", "SapoBoi_BR"),
    (639157, "76561197979883434", "Itaperuna"),
    (110078, "76561198024839791", "Bineto"),
    (1143262, "76561198105073246", "Mazzaroppee"),
    (55693, "76561197996817129", "BinhoBiceps"),
    (337051, "76561197981615184", "Maiti"),
    (1022333, "76561198264285149", "gaules"),
    (32922, "76561197986271242", "Showtaro"),
    (153969, "76561198005412401", "Cunha"),
    (557890, "76561199538846112", "É o Gabe (sec)"),
    (26910, "76561197963741792", "nigo_BR ᵀᴹ"),
    (6557, "76561198135142137", "Dragoness"),
    (1226910, "76561198140782132", "Lupus Alba"),
    (3895, "76561198054318813", "Liquid"),
    (1005730, "76561199791811246", "arc"),
    (31145, "76561197985268629", "vonMises"),
)

_TLS_LOCALS = threading.local()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        raise urllib.error.HTTPError(req.full_url, code, f"Redirecionamento bloqueado ({code})", headers, fp)


class _SteamIntraRedirectHandler(urllib.request.HTTPRedirectHandler):
    max_redirections = 3

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Any:
        parsed = urllib.parse.urlparse(newurl)
        if parsed.scheme != "https" or parsed.hostname != STEAM_HOST or not STEAM_REDIRECT_PATH_RE.fullmatch(parsed.path or ""):
            raise urllib.error.HTTPError(req.full_url, code, "Redirect Steam fora da allowlist", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _get_relic_opener() -> urllib.request.OpenerDirector:
    opener = getattr(_TLS_LOCALS, "relic_opener", None)
    if opener is None:
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx), _NoRedirectHandler())
        _TLS_LOCALS.relic_opener = opener
    return opener


def _get_steam_opener() -> urllib.request.OpenerDirector:
    opener = getattr(_TLS_LOCALS, "steam_opener", None)
    if opener is None:
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx), _SteamIntraRedirectHandler())
        _TLS_LOCALS.steam_opener = opener
    return opener


def _read_bounded_bytes(resp: Any, max_bytes: int) -> bytes:
    raw_chunks: list[bytes] = []
    total = 0
    while True:
        chunk = resp.read(65536)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ValueError("Resposta HTTP excedeu limite seguro.")
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
                if uz_total > max_bytes:
                    raise ValueError("Payload descompactado excedeu limite seguro.")
                out_chunks.append(piece)
            raw = b"".join(out_chunks)
    return raw


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
        "User-Agent": "CoH3-BR-SmartScheduler/1.2",
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
    }
    for attempt in range(2):
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with _get_relic_opener().open(req, timeout=15) as resp:
                data = json.loads(_read_bounded_bytes(resp, MAX_HTTP_BYTES).decode("utf-8"))
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


def _extract_xml_val(pattern: re.Pattern[str], text: str) -> str:
    m = pattern.search(text)
    if not m:
        return ""
    raw = (m.group(1) if m.group(1) is not None else (m.group(2) or "")) if (m.lastindex and m.lastindex >= 2) else (m.group(1) or "")
    return html.unescape(_HTML_STRIP_RE.sub(" ", raw)).strip()[:80]


def fetch_steam_presence_marker(steam_id: str) -> tuple[str, bool, bool, bool]:
    """Retorna (steam_id, is_in_coh3, is_online, ok) lendo o XML público da Comunidade Steam."""
    sid = str(steam_id).strip()
    url = f"https://{STEAM_HOST}/profiles/{sid}/?xml=1"
    headers = {
        "User-Agent": "CoH3-BR-SmartScheduler/1.2",
        "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.1",
        "Accept-Encoding": "gzip",
    }
    for attempt in range(2):
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with _get_steam_opener().open(req, timeout=8) as resp:
                xml_text = _read_bounded_bytes(resp, MAX_STEAM_XML_BYTES).decode("utf-8", errors="ignore")
            online_state = _extract_xml_val(_ONLINE_STATE_RE, xml_text).lower()
            if not online_state:
                continue
            g_name = _extract_xml_val(_GAME_NAME_RE, xml_text).lower()
            g_link = _extract_xml_val(_GAME_LINK_RE, xml_text).lower()
            s_msg = _extract_xml_val(_STATE_MSG_RE, xml_text).lower()
            in_coh3 = (
                f"/app/{COH3_APP_ID}" in g_link
                or "company of heroes 3" in g_name
                or ("in-game" in online_state and "company of heroes 3" in s_msg)
            )
            is_online = online_state in ("online", "in-game")
            return sid, bool(in_coh3), bool(is_online), True
        except Exception:
            if attempt == 0:
                time.sleep(0.4)
    return sid, False, False, False


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
    prev_in_coh3_sids: list[str] = sorted(str(x) for x in (prev.get("steam_in_coh3_sids") or []))
    prev_online_sids: list[str] = sorted(str(x) for x in (prev.get("steam_online_sids") or []))
    had_steam_baseline = "steam_in_coh3_sids" in prev
    last_dispatch_ts = int(prev.get("last_dispatch_ts", 0) or 0)
    last_daily_rank_date = str(prev.get("last_daily_rank_date_brt") or "")
    last_weekly_week = str(prev.get("last_weekly_summary_iso_week") or "")

    alias_by_pid = {pid: alias for pid, _, alias in MONITORED_ACCOUNTS}
    alias_by_sid = {sid: alias for _, sid, alias in MONITORED_ACCOUNTS}

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        relic_futs = [pool.submit(fetch_latest_player_marker, pid) for pid, _, _ in MONITORED_ACCOUNTS]
        steam_futs = [pool.submit(fetch_steam_presence_marker, sid) for _, sid, _ in MONITORED_ACCOUNTS]
        results = [f.result() for f in relic_futs]
        steam_results = [f.result() for f in steam_futs]

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

    steam_ok_count = sum(1 for _, _, _, ok in steam_results if ok)
    curr_in_coh3_sids = sorted(sid for sid, in_coh3, _, ok in steam_results if ok and in_coh3)
    curr_online_sids = sorted(sid for sid, in_coh3, is_on, ok in steam_results if ok and is_on and not in_coh3)

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

    # Gatilho de Lobby Ao Vivo: mudança imediata quando alguém abre/fecha o CoH3!
    if steam_ok_count >= 15:
        if not had_steam_baseline:
            coh3_names = ", ".join(alias_by_sid.get(s, s) for s in curr_in_coh3_sids[:4]) or "0"
            reasons.append(f"sincronizacao inicial do Radar de Lobby (no CoH3: {coh3_names})")
        elif curr_in_coh3_sids != prev_in_coh3_sids:
            entered = [alias_by_sid.get(s, s) for s in curr_in_coh3_sids if s not in prev_in_coh3_sids]
            left = [alias_by_sid.get(s, s) for s in prev_in_coh3_sids if s not in curr_in_coh3_sids]
            parts: list[str] = []
            if entered:
                parts.append("entrou no CoH3: " + ", ".join(entered[:4]))
            if left:
                parts.append("saiu do CoH3: " + ", ".join(left[:4]))
            reasons.append("Radar de Lobby (" + "; ".join(parts) + ")")
        elif curr_online_sids != prev_online_sids and (now_ts - last_dispatch_ts) >= STEAM_ONLINE_DEBOUNCE_SEC:
            reasons.append(f"atualizacao de presença Steam Online ({len(curr_online_sids)} online)")

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
    reason_str = (
        " | ".join(reasons)
        if reasons
        else f"sem mudancas (Relic {ok_count}/{len(MONITORED_ACCOUNTS)}, CoH3 agora={len(curr_in_coh3_sids)}, Online={len(curr_online_sids)})"
    )

    next_state = {
        "schema_version": 2,
        "last_checked_ts": now_ts,
        "last_dispatch_ts": now_ts if should_dispatch else last_dispatch_ts,
        "last_daily_rank_date_brt": (
            today_brt
            if (should_dispatch and mark_daily_done)
            else (last_daily_rank_date or today_brt if not prev_players and (now_brt.hour, now_brt.minute) >= (0, 1) else last_daily_rank_date)
        ),
        "last_weekly_summary_iso_week": current_iso_week if (should_dispatch and mark_weekly_done) else last_weekly_week,
        "steam_in_coh3_sids": curr_in_coh3_sids if steam_ok_count >= 15 else prev_in_coh3_sids,
        "steam_online_sids": curr_online_sids if steam_ok_count >= 15 else prev_online_sids,
        "players": updated_players,
    }

    if should_dispatch and not dry_run:
        atomic_write_json(state_path, next_state)

    return should_dispatch, reason_str, {
        "ok_accounts": ok_count,
        "steam_ok_accounts": steam_ok_count,
        "in_coh3_count": len(curr_in_coh3_sids),
        "online_count": len(curr_online_sids),
        "total_accounts": len(MONITORED_ACCOUNTS),
    }


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
        f"[PRECHECK] Decisao={status_word} | Relic={meta['ok_accounts']}/{meta['total_accounts']} | "
        f"Steam={meta['steam_ok_accounts']}/{meta['total_accounts']} (CoH3={meta['in_coh3_count']}, Online={meta['online_count']}) | "
        f"Motivo: {reason}"
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
