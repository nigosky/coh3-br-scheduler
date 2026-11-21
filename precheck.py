#!/usr/bin/env python3
"""
Pré-checagem inteligente (stdlib-only, TLS 1.2+) com loop contínuo de 5 min para `nigosky/coh3-br-scheduler`.

Consulta diretamente a API pública da Relic (`getRecentMatchHistoryByProfileId`) das contas Steam
monitoradas (sincronizadas da base compartilhada `players_roster.json`, com fallback embutido de 47 contas,
cobrindo tanto Automatch ranqueadas quanto partidas Customizadas!) a cada 5 minutos durante uma sessão
autônoma de 60 minutos (12 ciclos) e dispara (`workflow_dispatch`) o repositório privado
`nigosky/coh3-br-tracker` imediatamente assim que qualquer partida nova termina!

Resultado:
- Detecção pontual a cada 5 minutos 24h/dia (sem sofrer com atrasos ou descartes do cron do GitHub Actions).
- Minutos 100% gratuitos no agendador público, economizando ~80-90% da cota do repositório privado.
"""

from __future__ import annotations

import argparse
import ast
import concurrent.futures
import gzip
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

USER_AGENT = "CoH3-BR-SmartScheduler/2.4"
RELIC_HOST = "coh3-api.reliclink.com"
GITHUB_API_HOST = "api.github.com"
RADAR_HOST = "raw.githubusercontent.com"
RADAR_STATE_PATH = "/nigosky/coh3-br-lobby-radar/main/state.json"
RADAR_STATE_URL = f"https://{RADAR_HOST}{RADAR_STATE_PATH}"
SHARED_ROSTER_PATH = "/nigosky/coh3-br-lobby-radar/main/players_roster.json"
SHARED_ROSTER_URL = f"https://{RADAR_HOST}{SHARED_ROSTER_PATH}"
BASE_URL = f"https://{RELIC_HOST}/community/leaderboard"
MAX_HTTP_BYTES = 5 * 1024 * 1024  # 5 MB limite estrito anti-Gzip-bomb
MAX_RADAR_BYTES = 256 * 1024  # 256 KB limite estrito para state.json e players_roster.json
RADAR_MAX_AGE_SEC = 7200  # Ignora estado do Radar se estiver há >2h sem sincronizar
BRT_TZ = timezone(timedelta(hours=-3))
HEARTBEAT_INTERVAL_SEC = 3 * 3600  # Garante 1 disparo de segurança a cada 3h (auditoria profunda, limpeza 72h, Open Data)
QUIET_PERSONAL_STAT_AUDIT_SEC = 3600  # Após 1h sem partidas, varre getPersonalStat em 100% do elenco a cada 1h
QUIET_STAT_TOLERANCE_SEC = 60  # Margem segura acima do drift normal (+1s) do lastmatchdate da Relic
QUIET_STAT_RECENT_WINDOW_SEC = 72 * 3600  # Considera apenas partidas recentes (<=72h) na auditoria silenciosa
AUTOMATCH_TYPE_IDS: frozenset[int] = frozenset({1, 2, 3, 4, 20, 21, 22, 23})  # 1v1..4v4 Automatch CoH3 (exclui Custom=0)
BG_FOLLOWUP_FRESH_MATCH_SEC = 300  # Partida Automatch encerrada há <5 min pode ainda estar sendo parseada no cohdb.com
BG_FOLLOWUP_DELAY_SEC = 240  # Agenda 1 disparo único de confirmação no próximo ciclo de 5 min (+4-5 min)
BG_FOLLOWUP_TTL_SEC = 2700  # Descarta agendamento se ficar obsoleto (>45 min)

URL_FORBIDDEN_CHARS_RE = re.compile(r"[\x00-\x20\x7f]")
URL_PATH_TRAVERSAL_RE = re.compile(r"(?:^|/)\.\.(?:/|$)|%2e%2e|%5c|\\", re.IGNORECASE)
SAFE_GH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
SAFE_GH_REF_RE = re.compile(r"^[A-Za-z0-9_./-]{1,128}$")
STEAM_ID64_VALID_RE = re.compile(r"^7656119\d{10}$")

# Fallback embutido caso players_roster.json (base única compartilhada) esteja temporariamente inacessível
FALLBACK_MONITORED_PROFILES: tuple[tuple[int, str], ...] = (
    (79327, "Chacineiro"),
    (1333418, "Chacineiro (sec)"),
    (991764, "SaNgar [BR]"),
    (1088455, "luiz_fmh"),
    (735940, "Jimmy Duelista"),
    (559962, "Gabe"),
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
    (55693, "Alface aka BinhoBiceps"),
    (337051, "Maiti"),
    (1022333, "gaules"),
    (32922, "Showtaro"),
    (153969, "Cunha"),
    (557890, "Gabe (sec)"),
    (26910, "nigo_BR ᵀᴹ"),
    (6557, "Dragoness"),
    (1226910, "Lupus Alba"),
    (3895, "Liquid"),
    (1005730, "arc"),
    (1423426, "arc (sec)"),
    (31145, "vonMises"),
    (58737, "INFIEL"),
    (431802, "Conflict.BR"),
    (59864, "Lion Heart"),
    (84821, "TJBR"),
    (99722, "MonteiroBR"),
    (31061, "ACB"),
    (81751, "Razi"),
    (28937, "Mjr Bruno"),
    (737235, "meuqsaco"),
    (23250, "Nargoth"),
    (1097608, "Winter Animal"),
    (975688, "Kefbr"),
)

# Mapa fallback SteamID64 -> profile_id (permite consumir `last_steam_fast_sig[0]` do Radar sem usar chave Steam)
FALLBACK_STEAM_ID_TO_PID: dict[str, int] = {
    "76561197963172584": 79327,    # Chacineiro
    "76561198698969876": 1333418,  # Chacineiro (sec)
    "76561198115923932": 991764,   # SaNgar [BR]
    "76561198108568598": 1088455,  # luiz_fmh
    "76561198034510845": 735940,   # Jimmy Duelista
    "76561198062871244": 559962,   # Gabe
    "76561198372757032": 648514,   # Halokoo
    "76561198121484554": 35952,    # Sauronzinho
    "76561198070545853": 900358,   # Margheriti
    "76561198001604015": 691,      # Vítor
    "76561198205052385": 5768,     # The Dog
    "76561198284234077": 13450,    # 7 DIVISAO PHANTOM
    "76561198058548575": 19747,    # twitch.tv/luck22ai
    "76561198063124280": 944488,   # gbytes™
    "76561198061434984": 21206,    # hjax
    "76561198053226112": 509690,   # Alekel
    "76561198024118058": 1114404,  # Vikitor
    "76561198014985380": 366826,   # Lem22
    "76561198028644178": 1282592,  # SapoBoi_BR
    "76561197979883434": 639157,   # Itaperuna
    "76561198024839791": 110078,   # Bineto
    "76561198105073246": 1143262,  # Mazzaroppee
    "76561197996817129": 55693,    # Alface aka BinhoBiceps
    "76561197981615184": 337051,   # Maiti
    "76561198264285149": 1022333,  # gaules
    "76561197986271242": 32922,    # Showtaro
    "76561198005412401": 153969,   # Cunha
    "76561199538846112": 557890,   # Gabe (sec)
    "76561197963741792": 26910,    # nigo_BR ᵀᴹ
    "76561198135142137": 6557,     # Dragoness
    "76561198140782132": 1226910,  # Lupus Alba
    "76561198054318813": 3895,     # Liquid
    "76561199791811246": 1005730,  # arc
    "76561199482388323": 1423426,  # arc (sec)
    "76561197985268629": 31145,    # vonMises
    "76561197993922195": 58737,    # INFIEL
    "76561198000099422": 431802,   # Conflict.BR
    "76561198123446420": 59864,    # Lion Heart
    "76561198076576055": 84821,    # TJBR
    "76561198021071716": 99722,    # MonteiroBR
    "76561198009056438": 31061,    # ACB
    "76561198080023560": 81751,    # Razi
    "76561198089948995": 28937,    # Mjr Bruno
    "76561198026394732": 737235,   # meuqsaco
    "76561198028378060": 23250,    # Nargoth
    "76561199498421192": 1097608,  # Winter Animal
    "76561197994245218": 975688,   # Kefbr
}

# Contas secundárias mescladas preservadas quando a conta principal está no players_roster.json remoto
LOCAL_MERGED_ACCOUNT_FALLBACKS: tuple[tuple[int, str, str, int], ...] = (
    (1333418, "76561198698969876", "Chacineiro (sec)", 79327),
)

MONITORED_PROFILES: tuple[tuple[int, str], ...] = FALLBACK_MONITORED_PROFILES
MONITORED_PID_SET: frozenset[int] = frozenset(pid for pid, _ in MONITORED_PROFILES)
STEAM_ID_TO_PID: dict[str, int] = dict(FALLBACK_STEAM_ID_TO_PID)
_LAST_FETCHED_STEAM_ID_TO_PID: dict[str, int] = {}

_TLS_LOCALS = threading.local()


class _HttpsOnlySchemeHandler(urllib.request.BaseHandler):
    """Bloqueia no nível do OpenerDirector qualquer esquema fora de HTTPS (defesa em profundidade anti-SSRF/LFI)."""

    handler_order = 100

    def default_open(self, req: Any) -> Any:
        scheme = str(getattr(req, "type", "") or "").lower()
        if scheme != "https":
            raise urllib.error.URLError(f"Esquema '{scheme or '<vazio>'}' bloqueado pela política HTTPS-only.")
        return None

    http_open = default_open
    ftp_open = default_open
    file_open = default_open
    data_open = default_open


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        raise urllib.error.HTTPError(req.full_url, code, f"Redirecionamento bloqueado ({code})", headers, fp)


def _get_opener() -> urllib.request.OpenerDirector:
    opener = getattr(_TLS_LOCALS, "opener", None)
    if opener is None:
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        if hasattr(ssl, "OP_NO_COMPRESSION"):
            ctx.options |= ssl.OP_NO_COMPRESSION
        opener = urllib.request.build_opener(
            _HttpsOnlySchemeHandler(),
            urllib.request.HTTPSHandler(context=ctx),
            _NoRedirectHandler(),
        )
        _TLS_LOCALS.opener = opener
    return opener


def _validate_outbound_https(
    url: str,
    allowed_host: str,
    path_prefix: str = "/",
    allow_query: bool = True,
) -> urllib.parse.ParseResult:
    """Valida esquema HTTPS, host exato, porta 443, ausência de credenciais/fragmentos/traversal e prefixo de caminho."""
    clean = (url or "").strip()
    if not clean or URL_FORBIDDEN_CHARS_RE.search(clean):
        raise ValueError("URL inválida ou com caracteres de controle proibidos.")
    parsed = urllib.parse.urlparse(clean)
    if parsed.scheme != "https" or (parsed.hostname or "").lower() != allowed_host.lower():
        raise ValueError(f"Host fora da allowlist ({allowed_host}).")
    if parsed.username or parsed.password:
        raise ValueError("Credenciais embutidas na URL são bloqueadas.")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Porta inválida na URL.") from exc
    if port not in (None, 443):
        raise ValueError("Porta fora do padrão HTTPS (443) bloqueada.")
    if parsed.params or parsed.fragment or "#" in clean:
        raise ValueError("Parâmetros de caminho ou fragmentos de URL são bloqueados.")
    path_val = parsed.path or ""
    if URL_PATH_TRAVERSAL_RE.search(path_val):
        raise ValueError("Tentativa de path traversal na URL bloqueada.")
    if path_prefix and not path_val.startswith(path_prefix):
        raise ValueError("Caminho de URL fora do escopo autorizado.")
    if not allow_query and (parsed.query or "?" in clean):
        raise ValueError("Query string não permitida nesta URL.")
    return parsed


def _read_bounded_json(resp: Any, max_bytes: int = MAX_HTTP_BYTES) -> dict[str, Any]:
    raw_chunks: list[bytes] = []
    total = 0
    while True:
        chunk = resp.read(65536)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(f"Resposta HTTP excedeu limite seguro ({max_bytes} bytes).")
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
                    raise ValueError(f"Payload descompactado excedeu limite seguro ({max_bytes} bytes).")
                out_chunks.append(piece)
            raw = b"".join(out_chunks)
    parsed = json.loads(raw.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("Formato JSON inesperado.")
    return parsed


def _extract_pids_from_seq(seq: Any) -> set[int]:
    out: set[int] = set()
    if not isinstance(seq, (list, tuple, set)):
        return out
    for item in seq:
        candidate = item[0] if isinstance(item, (list, tuple)) and item else item
        if isinstance(candidate, bool):
            continue
        if isinstance(candidate, int) and candidate > 0:
            out.add(candidate)
        elif isinstance(candidate, str) and candidate.isdigit() and int(candidate) > 0:
            out.add(int(candidate))
    return out


def _extract_steam_id(raw: Any) -> str:
    s = str(raw or "").strip()
    if s.startswith("/steam/"):
        s = s[7:].strip()
    return s if STEAM_ID64_VALID_RE.fullmatch(s) else ""


def _extract_steam_ids_from_seq(seq: Any) -> set[str]:
    out: set[str] = set()
    if not isinstance(seq, (list, tuple, set)):
        return out
    for item in seq:
        candidate = item[0] if isinstance(item, (list, tuple)) and item else item
        sid = _extract_steam_id(candidate)
        if sid:
            out.add(sid)
    return out


def _parse_radar_state_active_pids(
    data: dict[str, Any],
    monitored_pids: frozenset[int] | None = None,
    steam_id_to_pid: dict[str, int] | None = None,
) -> set[int]:
    """
    Extrai os `profile_id`s de Veteranos em atividade CoH3 recente do `state.json` público do Radar:
    1. `last_alert_coh3_pids`
    2. `last_digest` (`vet_in_coh3` + `vet_recent_relic`)
    3. `last_steam_fast_sig[0]` (lista de SteamID64s em CoH3 na passada rápida Web API/XML; ignora `fast[1]` de outros jogos)
    """
    allowed_pids = MONITORED_PID_SET if monitored_pids is None else monitored_pids
    sid_map = STEAM_ID_TO_PID if steam_id_to_pid is None else steam_id_to_pid

    active_pids: set[int] = set()
    active_pids.update(_extract_pids_from_seq(data.get("last_alert_coh3_pids")))

    raw_digest = data.get("last_digest")
    if isinstance(raw_digest, str) and 2 <= len(raw_digest) <= 65536:
        try:
            dig = ast.literal_eval(raw_digest)
            if isinstance(dig, tuple) and dig and isinstance(dig[0], tuple):
                vet_tuple = dig[0]
                if len(vet_tuple) >= 1:
                    active_pids.update(_extract_pids_from_seq(vet_tuple[0]))
                if len(vet_tuple) >= 3:
                    active_pids.update(_extract_pids_from_seq(vet_tuple[2]))
        except (ValueError, SyntaxError, TypeError, RecursionError, MemoryError):
            pass

    raw_fast_sig = data.get("last_steam_fast_sig")
    if isinstance(raw_fast_sig, str) and 2 <= len(raw_fast_sig) <= 65536:
        try:
            fast = ast.literal_eval(raw_fast_sig)
            if isinstance(fast, tuple) and fast:
                for sid in _extract_steam_ids_from_seq(fast[0]):
                    mapped_pid = sid_map.get(sid)
                    if mapped_pid is not None and mapped_pid > 0:
                        active_pids.add(int(mapped_pid))
        except (ValueError, SyntaxError, TypeError, RecursionError, MemoryError):
            pass

    return active_pids & allowed_pids


def fetch_veteranos_radar_active_pids(max_age_sec: int = RADAR_MAX_AGE_SEC) -> tuple[set[int], int, bool]:
    """
    Lê (estritamente somente-leitura) o `state.json` público do Radar dos Veteranos
    (`nigosky/coh3-br-lobby-radar`) e retorna `(active_pids, last_sync_ts, ok)`.
    Não exige nem manipula `STEAM_WEB_API_KEY`.
    """
    parsed = _validate_outbound_https(RADAR_STATE_URL, RADAR_HOST, path_prefix=RADAR_STATE_PATH, allow_query=False)
    if parsed.path != RADAR_STATE_PATH:
        raise ValueError("Caminho do Radar fora da allowlist.")

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "Cache-Control": "no-cache",
    }
    for attempt in range(2):
        req = urllib.request.Request(RADAR_STATE_URL, headers=headers, method="GET")
        try:
            with _get_opener().open(req, timeout=8) as resp:
                data = _read_bounded_json(resp, max_bytes=MAX_RADAR_BYTES)

            last_sync_ts = int(data.get("last_sync_ts", 0) or 0)
            now_ts = int(time.time())
            if max_age_sec > 0 and last_sync_ts > 0 and (now_ts - last_sync_ts) > max_age_sec:
                return set(), last_sync_ts, False

            return _parse_radar_state_active_pids(data), last_sync_ts, True
        except Exception:
            if attempt == 0:
                time.sleep(0.6)
    return set(), 0, False


def parse_shared_roster_details(data: dict[str, Any]) -> tuple[tuple[tuple[int, str], ...], dict[str, int]]:
    """Extrai `(profiles, steam_id_to_pid)` de todos os Veteranos da base compartilhada `players_roster.json`."""
    if not isinstance(data, dict) or int(data.get("schema_version", 0) or 0) < 1:
        raise ValueError("Formato inválido de players_roster.json.")
    raw_vets = data.get("veteranos")
    if not isinstance(raw_vets, list) or not raw_vets:
        raise ValueError("Lista 'veteranos' ausente ou vazia em players_roster.json.")

    profiles: list[tuple[int, str]] = []
    steam_to_pid: dict[str, int] = {}
    seen_pids: set[int] = set()
    for row in raw_vets:
        if not isinstance(row, dict):
            continue
        try:
            pid = int(row.get("profile_id", 0) or 0)
        except (TypeError, ValueError):
            continue
        if pid <= 0 or pid in seen_pids:
            continue
        seen_pids.add(pid)
        alias = str(row.get("original_alias") or row.get("alias") or f"Player {pid}").strip()[:40]
        if int(row.get("merged_into_profile_id", 0) or 0) > 0 and "(sec)" not in alias:
            alias = f"{alias} (sec)"
        profiles.append((pid, alias))
        sid = _extract_steam_id(row.get("steam_id"))
        if sid and sid not in steam_to_pid:
            steam_to_pid[sid] = pid

    if not profiles:
        raise ValueError("Nenhum profile_id válido encontrado em players_roster.json.")
    return tuple(profiles), steam_to_pid


def parse_shared_roster_profiles(data: dict[str, Any]) -> tuple[tuple[int, str], ...]:
    """Extrai `(profile_id, alias)` de todos os Veteranos da base compartilhada `players_roster.json`."""
    profiles, _ = parse_shared_roster_details(data)
    return profiles


def fetch_shared_monitored_profiles(url: str = SHARED_ROSTER_URL) -> tuple[tuple[int, str], ...] | None:
    """
    Lê a base compartilhada `players_roster.json` (`nigosky/coh3-br-lobby-radar`) via HTTPS validado.
    Retorna `None` em caso de falha transitória (mantendo fallback seguro).
    """
    global _LAST_FETCHED_STEAM_ID_TO_PID
    parsed = _validate_outbound_https(url, RADAR_HOST, path_prefix=SHARED_ROSTER_PATH, allow_query=False)
    if parsed.path != SHARED_ROSTER_PATH:
        raise ValueError("Caminho do roster compartilhado fora da allowlist.")

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "Cache-Control": "no-cache",
    }
    for attempt in range(2):
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with _get_opener().open(req, timeout=8) as resp:
                data = _read_bounded_json(resp, max_bytes=MAX_RADAR_BYTES)
            profiles, sid_map = parse_shared_roster_details(data)
            _LAST_FETCHED_STEAM_ID_TO_PID = dict(sid_map)
            return profiles
        except Exception:
            if attempt == 0:
                time.sleep(0.6)
    return None


def refresh_monitored_profiles(url: str = SHARED_ROSTER_URL) -> bool:
    """Atualiza `MONITORED_PROFILES`, `MONITORED_PID_SET` e `STEAM_ID_TO_PID` a partir da base compartilhada."""
    global MONITORED_PROFILES, MONITORED_PID_SET, STEAM_ID_TO_PID, _LAST_FETCHED_STEAM_ID_TO_PID
    _LAST_FETCHED_STEAM_ID_TO_PID = {}
    fetched = fetch_shared_monitored_profiles(url=url)
    if fetched is None:
        return False
    merged_list = list(fetched)
    present_pids = {pid for pid, _ in merged_list}
    sid_map: dict[str, int] = (
        {sid: pid for sid, pid in _LAST_FETCHED_STEAM_ID_TO_PID.items() if pid in present_pids}
        if _LAST_FETCHED_STEAM_ID_TO_PID
        else {sid: pid for sid, pid in STEAM_ID_TO_PID.items() if pid in present_pids}
    )
    for ext_pid, ext_sid, ext_alias, main_pid in LOCAL_MERGED_ACCOUNT_FALLBACKS:
        if ext_pid not in present_pids and main_pid in present_pids:
            insert_idx = len(merged_list)
            for idx, (pid, _) in enumerate(merged_list):
                if pid == main_pid:
                    insert_idx = idx + 1
                    break
            merged_list.insert(insert_idx, (ext_pid, ext_alias))
            present_pids.add(ext_pid)
            sid_map[ext_sid] = ext_pid
    MONITORED_PROFILES = tuple(merged_list)
    MONITORED_PID_SET = frozenset(present_pids)
    STEAM_ID_TO_PID = sid_map
    return True


def fetch_personal_stat_last_match(pids: list[int]) -> dict[int, int]:
    """
    Consulta `getPersonalStat` da Relic (lotes de até 10 contas) apenas para o subconjunto
    sinalizado pelo Radar dos Veteranos, retornando `{profile_id: max_lastmatchdate}`.
    """
    clean_pids = sorted({int(p) for p in pids if int(p) > 0})
    if not clean_pids:
        return {}

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
    }
    latest: dict[int, int] = {}
    for idx in range(0, len(clean_pids), 10):
        chunk = clean_pids[idx : idx + 10]
        target_set = set(chunk)
        encoded_ids = urllib.parse.quote("[" + ",".join(str(p) for p in chunk) + "]")
        url = f"{BASE_URL}/getPersonalStat?title=coh3&profile_ids={encoded_ids}"
        _validate_outbound_https(url, RELIC_HOST, path_prefix="/community/leaderboard/")

        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with _get_opener().open(req, timeout=12) as resp:
                data = _read_bounded_json(resp)
        except Exception:
            continue

        sg_to_pids: dict[int, list[int]] = {}
        for sg in data.get("statGroups") or []:
            if not isinstance(sg, dict) or "id" not in sg:
                continue
            matched = [
                int(m.get("profile_id", 0) or 0)
                for m in (sg.get("members") or [])
                if isinstance(m, dict) and int(m.get("profile_id", 0) or 0) in target_set
            ]
            if matched:
                sg_to_pids[int(sg["id"])] = matched

        for st in data.get("leaderboardStats") or []:
            if not isinstance(st, dict):
                continue
            sg_id = int(st.get("statgroup_id", -1))
            lmd = int(st.get("lastmatchdate", 0) or 0)
            if lmd <= 0 or sg_id not in sg_to_pids:
                continue
            for pid in sg_to_pids[sg_id]:
                if lmd > latest.get(pid, 0):
                    latest[pid] = lmd

    return latest


def fetch_latest_player_marker(profile_id: int) -> tuple[int, int, int, bool, int, int]:
    """Retorna (profile_id, max_match_id, max_completion_time, ok, max_automatch_id, max_automatch_ct) para uma conta monitorada."""
    pid = int(profile_id)
    if pid <= 0:
        return pid, 0, 0, False, 0, 0
    qs = urllib.parse.urlencode({"title": "coh3", "profile_id": str(pid)})
    url = f"{BASE_URL}/getRecentMatchHistoryByProfileId?{qs}"
    _validate_outbound_https(url, RELIC_HOST, path_prefix="/community/leaderboard/")

    headers = {
        "User-Agent": USER_AGENT,
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
            max_auto_mid = 0
            max_auto_ct = 0
            for item in stats:
                if not isinstance(item, dict):
                    continue
                mid = int(item.get("id", 0) or 0)
                ct = int(item.get("completiontime", 0) or 0)
                mt_id = int(item.get("matchtype_id", 0) or 0)
                if mid > max_mid:
                    max_mid = mid
                if ct > max_ct:
                    max_ct = ct
                if mt_id in AUTOMATCH_TYPE_IDS:
                    if mid > max_auto_mid:
                        max_auto_mid = mid
                    if ct > max_auto_ct:
                        max_auto_ct = ct
            return pid, max_mid, max_ct, True, max_auto_mid, max_auto_ct
        except Exception:
            if attempt == 0:
                time.sleep(0.8)
    return pid, 0, 0, False, 0, 0


def dispatch_private_tracker(
    token: str,
    owner: str = "nigosky",
    repo: str = "coh3-br-tracker",
    workflow: str = "coh3_monitor.yml",
    ref: str = "main",
) -> tuple[bool, int]:
    """Dispara workflow_dispatch no repositório privado via HTTPS TLS 1.2+ sem expor segredos."""
    clean_token = (token or "").strip()
    if not clean_token:
        return False, 0

    for label, seg in (("owner", owner), ("repo", repo), ("workflow", workflow)):
        val = (seg or "").strip()
        if not SAFE_GH_SEGMENT_RE.fullmatch(val) or ".." in val:
            raise ValueError(f"Parâmetro GitHub inválido ({label}).")
    clean_ref = (ref or "").strip()
    if not SAFE_GH_REF_RE.fullmatch(clean_ref) or ".." in clean_ref:
        raise ValueError("Parâmetro GitHub inválido (ref).")

    url = f"https://{GITHUB_API_HOST}/repos/{owner.strip()}/{repo.strip()}/actions/workflows/{workflow.strip()}/dispatches"
    _validate_outbound_https(url, GITHUB_API_HOST, path_prefix="/repos/", allow_query=False)

    body = json.dumps({"ref": clean_ref}).encode("utf-8")
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {clean_token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }

    last_code = 0
    for attempt in range(1, 4):
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with _get_opener().open(req, timeout=20) as resp:
                last_code = int(resp.getcode() or 0)
                if last_code == 204:
                    return True, 204
        except urllib.error.HTTPError as exc:
            last_code = int(exc.code or 0)
        except Exception:
            last_code = 0

        if attempt < 3:
            time.sleep(attempt * 3)

    return False, last_code


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.is_file() or state_path.is_symlink():
        return {}
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(state_path, flags)
        with os.fdopen(fd, "r", encoding="utf-8") as fh:
            parsed = json.loads(fh.read())
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    if path.is_symlink():
        raise RuntimeError(f"Escrita bloqueada em symlink não confiável: {path}")
    payload = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    fd, tmp_name = tempfile.mkstemp(prefix=".state_", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        if path.is_symlink():
            raise RuntimeError(f"Escrita bloqueada em symlink não confiável: {path}")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def write_github_outputs(outputs: dict[str, str]) -> None:
    gh_out = os.environ.get("GITHUB_OUTPUT", "").strip()
    if not gh_out:
        return
    out_path = Path(gh_out)
    if out_path.is_symlink():
        raise RuntimeError("GITHUB_OUTPUT não pode ser symlink.")
    with open(out_path, "a", encoding="utf-8") as fh:
        for k, v in outputs.items():
            clean_k = str(k).strip()
            if not clean_k or not re.fullmatch(r"[A-Za-z0-9_]+", clean_k):
                continue
            clean_v = str(v).replace("\r", " ").replace("\n", " ").strip()
            fh.write(f"{clean_k}={clean_v}\n")


def evaluate_precheck(
    state_path: Path,
    force_dispatch: bool = False,
    dry_run: bool = False,
    dispatch_token: str = "",
    target_owner: str = "nigosky",
    target_repo: str = "coh3-br-tracker",
    target_workflow: str = "coh3_monitor.yml",
    target_ref: str = "main",
    prev_radar_pids_override: set[int] | None = None,
    last_full_stat_audit_ts_override: int | None = None,
) -> tuple[bool, bool, str, dict[str, Any]]:
    """
    Executa 1 ciclo de pré-checagem (Relic History + Radar dos Veteranos + Relic PersonalStat + Auditoria de Silêncio 1h).
    Retorna `(should_dispatch, dispatched_ok, reason_str, meta)`.
    Importante: só avança `state.json` se `dispatched_ok` for True (evita perder partidas caso o dispatch falhe).
    """
    now_ts = int(time.time())
    now_brt = datetime.fromtimestamp(now_ts, tz=BRT_TZ)
    iso_year, iso_week, _ = now_brt.isocalendar()
    current_iso_week = f"{iso_year}-W{iso_week:02d}"

    prev = load_state(state_path)
    prev_players: dict[str, dict[str, int]] = dict(prev.get("players") or {})
    last_dispatch_ts = int(prev.get("last_dispatch_ts", 0) or 0)
    last_weekly_week = str(prev.get("last_weekly_summary_iso_week") or "")
    prev_bg_due_ts = int(prev.get("pending_bg_followup_due_ts", 0) or 0)
    prev_bg_match_id = int(prev.get("pending_bg_followup_match_id", 0) or 0)
    prev_full_audit_ts = max(
        int(prev.get("last_full_stat_audit_ts", 0) or 0),
        int(last_full_stat_audit_ts_override or 0),
    )
    prev_radar_pids = (
        set(prev_radar_pids_override)
        if prev_radar_pids_override is not None
        else _extract_pids_from_seq(prev.get("radar_coh3_pids"))
    )

    alias_by_pid = {pid: alias for pid, alias in MONITORED_PROFILES}
    all_monitored_pids = [pid for pid, _ in MONITORED_PROFILES]
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(fetch_latest_player_marker, all_monitored_pids))

    # Checagem cruzada somente-leitura com o Radar dos Veteranos (nigosky/coh3-br-lobby-radar)
    curr_radar_pids, radar_sync_ts, radar_ok = fetch_veteranos_radar_active_pids()
    exited_radar_pids = (prev_radar_pids - curr_radar_pids) if radar_ok else set()
    focus_radar_pids = (curr_radar_pids | exited_radar_pids) if radar_ok else set()

    results_by_pid: dict[int, tuple[int, int, int, bool, int, int]] = {
        pid: (pid, mid, ct, ok, a_mid, a_ct) for pid, mid, ct, ok, a_mid, a_ct in results
    }

    run_full_stat_audit = bool(
        prev_players
        and last_dispatch_ts > 0
        and (now_ts - last_dispatch_ts) >= QUIET_PERSONAL_STAT_AUDIT_SEC
        and (now_ts - prev_full_audit_ts) >= QUIET_PERSONAL_STAT_AUDIT_SEC
    )

    # Rechecagem prioritária imediata se um Veterano ativo/saindo do Radar (ou qualquer conta na auditoria de silêncio 1h) falhou na 1ª passada
    retry_candidate_pids = set(focus_radar_pids)
    if run_full_stat_audit:
        retry_candidate_pids.update(pid for pid, rec in results_by_pid.items() if not rec[3])

    radar_failed_aliases: list[str] = []
    for fpid in sorted(retry_candidate_pids):
        _, _, _, ok_first, _, _ = results_by_pid.get(fpid, (fpid, 0, 0, False, 0, 0))
        if not ok_first:
            retry_res = fetch_latest_player_marker(fpid)
            results_by_pid[fpid] = retry_res
            if not retry_res[3] and fpid in focus_radar_pids:
                radar_failed_aliases.append(alias_by_pid.get(fpid, str(fpid)))

    ok_count = 0
    new_activity_players: list[str] = []
    new_activity_pids: set[int] = set()
    fresh_automatch_mid = 0
    updated_players: dict[str, dict[str, int]] = {
        k: dict(v)
        for k, v in prev_players.items()
        if str(k).isdigit() and int(k) in MONITORED_PID_SET
    }

    for pid, _ in MONITORED_PROFILES:
        _, max_mid, max_ct, ok, max_auto_mid, max_auto_ct = results_by_pid[pid]
        key = str(pid)
        if not ok:
            continue
        ok_count += 1
        old_entry = prev_players.get(key) or {}
        old_mid = int(old_entry.get("max_match_id", 0) or 0)
        old_ct = int(old_entry.get("max_completion_time", 0) or 0)
        old_stat_lmd = int(old_entry.get("max_stat_lmd", 0) or 0)
        if prev_players and (max_mid > old_mid or max_ct > old_ct):
            new_activity_players.append(f"{alias_by_pid.get(pid, str(pid))} (#{max_mid})")
            new_activity_pids.add(pid)
            if (
                max_auto_ct > 0
                and (max_auto_mid > old_mid or max_auto_ct > old_ct)
                and (now_ts - max_auto_ct) < BG_FOLLOWUP_FRESH_MATCH_SEC
            ):
                if max_auto_mid > fresh_automatch_mid:
                    fresh_automatch_mid = max_auto_mid
        entry_next: dict[str, int] = {
            "max_match_id": max(max_mid, old_mid),
            "max_completion_time": max(max_ct, old_ct),
        }
        if old_stat_lmd > 0:
            entry_next["max_stat_lmd"] = max(old_stat_lmd, entry_next["max_completion_time"])
        updated_players[key] = entry_next

    # Cross-check Relic getPersonalStat:
    # - A cada 5 min para Veteranos sinalizados pelo Radar
    # - A cada 1h de silêncio, varredura completa em 100% do elenco (inclusive Steam privado!)
    radar_stat_triggers: list[str] = []
    quiet_stat_triggers: list[str] = []
    stat_target_pids = sorted(set(all_monitored_pids) if run_full_stat_audit else focus_radar_pids)
    if stat_target_pids:
        stat_lmd_map = fetch_personal_stat_last_match(stat_target_pids)
        for fpid in stat_target_pids:
            stat_lmd = int(stat_lmd_map.get(fpid, 0) or 0)
            if stat_lmd <= 0:
                continue
            key = str(fpid)
            old_entry = prev_players.get(key) or {}
            old_ct = int(old_entry.get("max_completion_time", 0) or 0)
            old_stat_lmd = int(old_entry.get("max_stat_lmd", 0) or 0)
            baseline_ts = max(old_ct, old_stat_lmd)

            cur_slot = updated_players.setdefault(
                key,
                {"max_match_id": int(old_entry.get("max_match_id", 0) or 0), "max_completion_time": old_ct},
            )
            cur_slot["max_stat_lmd"] = max(stat_lmd, baseline_ts, int(cur_slot.get("max_completion_time", 0) or 0))

            if not prev_players or fpid in new_activity_pids:
                continue
            if fpid in focus_radar_pids:
                if stat_lmd > (baseline_ts + 10):
                    radar_stat_triggers.append(alias_by_pid.get(fpid, str(fpid)))
            elif (
                (now_ts - stat_lmd) <= QUIET_STAT_RECENT_WINDOW_SEC
                and stat_lmd > (baseline_ts + QUIET_STAT_TOLERANCE_SEC)
            ):
                quiet_stat_triggers.append(alias_by_pid.get(fpid, str(fpid)))

    reasons: list[str] = []
    mark_weekly_done = False

    if force_dispatch:
        reasons.append("disparo manual forcado (workflow_dispatch)")
    if not prev_players:
        reasons.append("inicializacao do baseline de estado (1a execucao)")
    if new_activity_players:
        sample = ", ".join(new_activity_players[:4])
        extra = f" +{len(new_activity_players) - 4}" if len(new_activity_players) > 4 else ""
        reasons.append(f"nova partida detectada: {sample}{extra}")
    if radar_stat_triggers:
        sample_r = ", ".join(radar_stat_triggers[:4])
        extra_r = f" +{len(radar_stat_triggers) - 4}" if len(radar_stat_triggers) > 4 else ""
        reasons.append(f"Radar Veteranos + Relic PersonalStat detectou partida recente: {sample_r}{extra_r}")
    if quiet_stat_triggers:
        sample_q = ", ".join(quiet_stat_triggers[:4])
        extra_q = f" +{len(quiet_stat_triggers) - 4}" if len(quiet_stat_triggers) > 4 else ""
        reasons.append(f"Auditoria de silencio (1h) + Relic PersonalStat detectou partida recente: {sample_q}{extra_q}")
    if radar_failed_aliases:
        sample_f = ", ".join(radar_failed_aliases[:4])
        reasons.append(f"Radar Veteranos: fallback p/ rechecagem Relic ({sample_f})")
    bg_followup_due = (
        prev_bg_due_ts > 0
        and now_ts >= prev_bg_due_ts
        and (now_ts - prev_bg_due_ts) <= BG_FOLLOWUP_TTL_SEC
    )
    if bg_followup_due and not new_activity_players and not radar_stat_triggers and not quiet_stat_triggers:
        mid_hint = f" #{prev_bg_match_id}" if prev_bg_match_id > 0 else ""
        reasons.append(f"confirmacao de Battlegroups cohdb (+5 min apos partida{mid_hint})")
    if now_brt.weekday() == 0 and (now_brt.hour, now_brt.minute) >= (0, 5) and last_weekly_week != current_iso_week:
        reasons.append(f"janela do Resumo Semanal ({current_iso_week} segunda >= 00:05 BRT)")
        mark_weekly_done = True
    if ok_count == 0:
        reasons.append("fallback de seguranca (Relic API indisponivel na pre-checagem)")
    elif last_dispatch_ts <= 0 or (now_ts - last_dispatch_ts) >= HEARTBEAT_INTERVAL_SEC:
        elapsed_h = round((now_ts - last_dispatch_ts) / 3600.0, 1) if last_dispatch_ts > 0 else 0.0
        reasons.append(f"heartbeat periodico de auditoria profunda ({elapsed_h}h desde ultimo ciclo)")

    should_dispatch = len(reasons) > 0
    audit_suffix = " | auditoria silencio 1h OK (100% PersonalStat)" if run_full_stat_audit else ""
    reason_str = (
        " | ".join(reasons)
        if reasons
        else f"sem novas partidas ({ok_count}/{len(MONITORED_PROFILES)} contas verificadas{audit_suffix})"
    )

    dispatched_ok = False
    http_code = 0
    if should_dispatch:
        if dry_run:
            dispatched_ok = True
            http_code = 204
        elif dispatch_token.strip():
            dispatched_ok, http_code = dispatch_private_tracker(
                token=dispatch_token,
                owner=target_owner,
                repo=target_repo,
                workflow=target_workflow,
                ref=target_ref,
            )
        else:
            # Modo legado sem token no processo Python (ex.: teste local)
            dispatched_ok = False
            http_code = 0

    if should_dispatch and dispatched_ok and not dry_run:
        if fresh_automatch_mid > 0 or radar_stat_triggers or quiet_stat_triggers:
            next_bg_due_ts = now_ts + BG_FOLLOWUP_DELAY_SEC
            next_bg_mid = fresh_automatch_mid
        elif prev_bg_due_ts > 0 and now_ts < (prev_bg_due_ts - 60):
            next_bg_due_ts = prev_bg_due_ts
            next_bg_mid = prev_bg_match_id
        else:
            next_bg_due_ts = 0
            next_bg_mid = 0
        next_state = {
            "schema_version": 2,
            "last_checked_ts": now_ts,
            "last_dispatch_ts": now_ts,
            "last_full_stat_audit_ts": now_ts if run_full_stat_audit else prev_full_audit_ts,
            "last_weekly_summary_iso_week": current_iso_week if mark_weekly_done else last_weekly_week,
            "pending_bg_followup_due_ts": next_bg_due_ts,
            "pending_bg_followup_match_id": next_bg_mid,
            "radar_coh3_pids": sorted(curr_radar_pids) if radar_ok else sorted(prev_radar_pids),
            "radar_last_sync_ts": radar_sync_ts if radar_ok else int(prev.get("radar_last_sync_ts", 0) or 0),
            "players": updated_players,
        }
        atomic_write_json(state_path, next_state)

    meta = {
        "ok_accounts": ok_count,
        "total_accounts": len(MONITORED_PROFILES),
        "http_code": http_code,
        "radar_ok": radar_ok,
        "radar_active_pids": sorted(curr_radar_pids),
        "radar_exited_pids": sorted(exited_radar_pids),
        "full_stat_audit_ran": run_full_stat_audit,
        "full_stat_audit_ts": now_ts if run_full_stat_audit else prev_full_audit_ts,
    }
    return should_dispatch, dispatched_ok, reason_str, meta


def main() -> int:
    parser = argparse.ArgumentParser(description="CoH3 BR Smart Pre-Check Scheduler")
    parser.add_argument("--loop-cycles", type=int, default=1, help="Quantidade de ciclos por sessao (1..24)")
    parser.add_argument("--loop-interval-sec", type=int, default=300, help="Intervalo em segundos entre ciclos")
    parser.add_argument("--force", action="store_true", help="Forca disparo no 1o ciclo")
    parser.add_argument("--dry-run", action="store_true", help="Simula checagem sem disparar nem gravar estado")
    args = parser.parse_args()

    cycles = max(1, min(24, int(args.loop_cycles)))
    interval_sec = max(30, min(900, int(args.loop_interval_sec)))

    repo_dir = Path(__file__).resolve().parent
    state_path = repo_dir / "state.json"
    force_first = args.force or (os.environ.get("FORCE_DISPATCH", "false").strip().lower() == "true")

    dispatch_token = os.environ.get("TRACKER_DISPATCH_TOKEN", "").strip()
    target_owner = os.environ.get("TARGET_OWNER", "nigosky").strip() or "nigosky"
    target_repo = os.environ.get("TARGET_REPO", "coh3-br-tracker").strip() or "coh3-br-tracker"
    target_workflow = os.environ.get("TARGET_WORKFLOW", "coh3_monitor.yml").strip() or "coh3_monitor.yml"
    target_ref = os.environ.get("TARGET_REF", "main").strip() or "main"

    if not args.dry_run and not dispatch_token:
        print(
            "::error::O Secret TRACKER_DISPATCH_TOKEN nao esta configurado em Settings -> Secrets and variables -> Actions.",
            flush=True,
        )
        return 1

    total_dispatches = 0
    total_skips = 0
    last_ok_str = f"0/{len(MONITORED_PROFILES)}"
    dispatch_events: list[str] = []
    mem_radar_pids: set[int] | None = None
    mem_last_full_stat_audit_ts: int | None = None

    for idx in range(1, cycles + 1):
        cycle_start = time.monotonic()
        if idx == 1 or (idx - 1) % 3 == 0:
            synced = refresh_monitored_profiles()
            if idx == 1:
                src_lbl = "players_roster.json (compartilhado)" if synced else "fallback embutido"
                print(
                    f"[ROSTER] Base sincronizada ({src_lbl}): {len(MONITORED_PROFILES)} contas monitoradas.",
                    flush=True,
                )
        now_brt_str = datetime.now(tz=BRT_TZ).strftime("%H:%M:%S BRT")
        should_dispatch, dispatched_ok, reason, meta = evaluate_precheck(
            state_path=state_path,
            force_dispatch=(force_first and idx == 1),
            dry_run=args.dry_run,
            dispatch_token=dispatch_token,
            target_owner=target_owner,
            target_repo=target_repo,
            target_workflow=target_workflow,
            target_ref=target_ref,
            prev_radar_pids_override=mem_radar_pids,
            last_full_stat_audit_ts_override=mem_last_full_stat_audit_ts,
        )
        if meta.get("radar_ok"):
            mem_radar_pids = set(meta.get("radar_active_pids") or [])
        if meta.get("full_stat_audit_ran"):
            mem_last_full_stat_audit_ts = int(meta.get("full_stat_audit_ts") or int(time.time()))
        last_ok_str = f"{meta['ok_accounts']}/{meta['total_accounts']}"
        radar_tag = f"Radar={len(meta.get('radar_active_pids') or [])}" if meta.get("radar_ok") else "Radar=OFF"

        if should_dispatch:
            if dispatched_ok:
                total_dispatches += 1
                dispatch_events.append(f"[{now_brt_str}] {reason}")
                print(
                    f"[CICLO {idx}/{cycles} • {now_brt_str}] DISPARADO (HTTP {meta['http_code']}) -> "
                    f"{target_owner}/{target_repo}@{target_ref} | Contas OK={last_ok_str} | {radar_tag} | Motivo: {reason}",
                    flush=True,
                )
            else:
                print(
                    f"::warning::[CICLO {idx}/{cycles} • {now_brt_str}] Falha ao disparar {target_owner}/{target_repo} "
                    f"(HTTP {meta['http_code']}); estado preservado para retentativa no proximo ciclo! | {radar_tag} | Motivo: {reason}",
                    flush=True,
                )
        else:
            total_skips += 1
            print(
                f"[CICLO {idx}/{cycles} • {now_brt_str}] ECONOMIZADO (SKIP) | Contas OK={last_ok_str} | {radar_tag} | {reason}",
                flush=True,
            )

        if idx < cycles:
            elapsed = time.monotonic() - cycle_start
            sleep_for = max(1.0, float(interval_sec) - elapsed)
            time.sleep(sleep_for)

    summary_reason = (
        " ; ".join(dispatch_events[-3:])
        if dispatch_events
        else f"Nenhuma partida nova nos {cycles} ciclo(s) ({total_skips} skips)"
    )
    write_github_outputs(
        {
            "total_cycles": str(cycles),
            "dispatches_count": str(total_dispatches),
            "skips_count": str(total_skips),
            "ok_accounts": last_ok_str,
            "summary_reason": summary_reason,
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
