#!/usr/bin/env python3
"""
Pré-checagem inteligente (stdlib-only, TLS 1.2+) com loop contínuo de 5 min para `nigosky/coh3-br-scheduler`.

Consulta diretamente a API pública da Relic (`getRecentMatchHistoryByProfileId`) das contas Steam
monitoradas (sincronizadas da base compartilhada `players_roster.json`, com fallback embutido,
cobrindo tanto Automatch ranqueadas quanto partidas Customizadas!) a cada 5 minutos durante uma sessão
autônoma de 60 minutos (12 ciclos) e dispara (`workflow_dispatch`):
- `nigosky/coh3-br-tracker` (`coh3_monitor.yml`) imediatamente assim que qualquer partida nova termina,
  em janelas de confirmação de Battlegroups, Resumo Semanal ou heartbeat de 3h;
- `nigosky/coh3-br-ranking`:
  1. `boletim_diario.yml` (todo dia >= 08:03 BRT)
  2. `ranking_unificado.yml` (2x/dia >= 00:02 BRT e >= 12:02 BRT, `{"force_update": false}`)
  3. `campeoes_battlegroups.yml` (sábado >= 12:05 BRT, janela de recuperação até domingo 11:59 BRT)

Resultado:
- Detecção e agendamentos pontuais a cada 5 minutos 24h/dia (sem sofrer com atrasos ou descartes do cron do GitHub Actions).
- Minutos 100% gratuitos no agendador público, economizando ~80-90% da cota dos repositórios privados.
"""

from __future__ import annotations

import argparse
import ast
import concurrent.futures
import copy
import gzip
import http.client
import io
import json
import os
import re
import signal
import ssl
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.response
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

USER_AGENT = "CoH3-BR-SmartScheduler/2.6"
RELIC_HOST = "coh3-api.reliclink.com"
GITHUB_API_HOST = "api.github.com"
RADAR_HOST = "raw.githubusercontent.com"
RADAR_STATE_PATH = "/nigosky/coh3-br-lobby-radar/main/state.json"
RADAR_STATE_URL = f"https://{RADAR_HOST}{RADAR_STATE_PATH}"
SHARED_ROSTER_PATH = "/nigosky/coh3-br-lobby-radar/main/players_roster.json"
SHARED_ROSTER_URL = f"https://{RADAR_HOST}{SHARED_ROSTER_PATH}"
BASE_URL = f"https://{RELIC_HOST}/community/leaderboard"
RANKING_TARGET_OWNER = "nigosky"
RANKING_TARGET_REPO = "coh3-br-ranking"
RANKING_TARGET_REF = "main"
RANKING_DAILY_WORKFLOW = "boletim_diario.yml"
RANKING_UNIFIED_WORKFLOW = "ranking_unificado.yml"
RANKING_BG_WORKFLOW = "campeoes_battlegroups.yml"
RANKING_UNIFIED_INPUTS: dict[str, Any] = {"force_update": False}
MAX_HTTP_BYTES = 5 * 1024 * 1024  # 5 MB limite estrito anti-Gzip-bomb
MAX_RADAR_BYTES = 256 * 1024  # 256 KB limite estrito para state.json e players_roster.json
MAX_AST_NESTING_DEPTH = 16  # Limite estrito anti-DoS para ast.literal_eval em payloads externos
RADAR_MAX_AGE_SEC = 7200  # Ignora estado do Radar se estiver há >2h sem sincronizar
BRT_TZ = timezone(timedelta(hours=-3))
HEARTBEAT_INTERVAL_SEC = 3 * 3600  # Garante 1 disparo de segurança a cada 3h (auditoria profunda, limpeza 72h, Open Data)
RELIC_OUTAGE_FALLBACK_COOLDOWN_SEC = 1800  # Circuit breaker: durante queda total da Relic, limita fallback a 1x/30 min
QUIET_PERSONAL_STAT_AUDIT_SEC = 3600  # Após 1h sem partidas, varre getPersonalStat em 100% do elenco a cada 1h
QUIET_STAT_TOLERANCE_SEC = 60  # Margem segura acima do drift normal (+1s) do lastmatchdate da Relic
QUIET_STAT_RECENT_WINDOW_SEC = 72 * 3600  # Considera apenas partidas recentes (<=72h) na auditoria silenciosa e onboarding
DEFAULT_MAX_SESSION_SEC = 3660  # Limite seguro (<70 min timeout do Actions) p/ garantir persistência + Self-Relay
AUTOMATCH_TYPE_IDS: frozenset[int] = frozenset({1, 2, 3, 4, 20, 21, 22, 23})  # 1v1..4v4 Automatch CoH3
BG_ELIGIBLE_TYPE_IDS: frozenset[int] = AUTOMATCH_TYPE_IDS | frozenset({0})  # Automatch + Custom (mt=0); exclui Co-Op vs IA
BG_FOLLOWUP_FRESH_MATCH_SEC = 600  # Partida Automatch/Custom encerrada há <10 min pode ainda estar sendo parseada no cohdb.com
BG_FOLLOWUP_DELAY_SEC = 240  # Onda 1: disparo de confirmação no próximo ciclo de 5 min (+4-5 min)
BG_FOLLOWUP_WAVE2_DELAY_SEC = 1500  # Onda 2: segunda confirmação ~25-30 min após a partida (cobre atraso em Custom)
BG_FOLLOWUP_TTL_SEC = 3600  # Descarta agendamento se ficar obsoleto (>60 min)

URL_FORBIDDEN_CHARS_RE = re.compile(r"[\x00-\x20\x7f]")
URL_ENCODED_CTRL_RE = re.compile(r"%0[0-9a-f]|%1[0-9a-f]|%7f", re.IGNORECASE)
URL_PATH_TRAVERSAL_RE = re.compile(r"(?:^|/)\.\.(?:/|$)|%2e%2e|%5c|\\", re.IGNORECASE)
SAFE_GH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
SAFE_GH_REF_RE = re.compile(r"^[A-Za-z0-9_./-]{1,128}$")
STEAM_ID64_VALID_RE = re.compile(r"^7656119\d{10}$")
CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]+")

WEBHOOK_REDACT_RE = re.compile(
    r"https?://[^\s/\"'<>]+/api/(?:v\d+/)?webhooks/[^\s\"'<>]+",
    re.IGNORECASE,
)
TOKEN_ENV_REDACT_RE = re.compile(
    r"((?:TRACKER_DISPATCH_TOKEN|RANKING_DISPATCH_TOKEN|GITHUB_TOKEN|GH_TOKEN|DISCORD_(?:LOBBY_)?WEBHOOK_URL|STEAM_WEB_API_KEY)\s*[=:]\s*[\"']?)[^\s\"'<>]+",
    re.IGNORECASE,
)
STEAM_API_KEY_QUERY_REDACT_RE = re.compile(r"([?&]key=)[^&\s\"'<>]+", re.IGNORECASE)
GITHUB_TOKEN_REDACT_RE = re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b")
AUTH_HEADER_REDACT_RE = re.compile(
    r"(Authorization\s*:\s*(?:Bearer|Basic|Token)\s+)[^\s\"'<>]+",
    re.IGNORECASE,
)


def redact_secrets(text: Any) -> str:
    """Remove tokens GitHub/dispatch, cabeçalhos Authorization, Webhooks e chaves Steam de logs e outputs."""
    raw = str(text or "")
    if not raw:
        return ""
    scrubbed = WEBHOOK_REDACT_RE.sub("[WEBHOOK_REDACTED]", raw)
    scrubbed = TOKEN_ENV_REDACT_RE.sub(r"\1[SECRET_REDACTED]", scrubbed)
    scrubbed = STEAM_API_KEY_QUERY_REDACT_RE.sub(r"\1[STEAM_KEY_REDACTED]", scrubbed)
    scrubbed = GITHUB_TOKEN_REDACT_RE.sub("[GH_TOKEN_REDACTED]", scrubbed)
    return AUTH_HEADER_REDACT_RE.sub(r"\1[AUTH_REDACTED]", scrubbed)


def _sanitize_alias(raw: Any, fallback: str = "Player") -> str:
    """Sanitiza apelidos externos contra injeção de comandos Actions (`::`), controle, newlines, links Markdown ou HTML."""
    text = CONTROL_CHARS_RE.sub(" ", str(raw or ""))
    text = text.replace("::", ":").replace("<", "").replace(">", "").replace("`", "").replace("](", "] (")
    text = " ".join(text.split()).strip()[:40]
    return text or fallback


def _sanitize_log_line(text: Any) -> str:
    """Garante saída em linha única e sem segredos, bloqueando injeção de comandos GitHub Actions por quebra de linha."""
    one_line = CONTROL_CHARS_RE.sub(" ", str(text or "")).strip()
    return redact_secrets(one_line)


def _format_sample(items: list[str], limit: int = 4) -> str:
    sample = ", ".join(items[:limit])
    extra = f" +{len(items) - limit}" if len(items) > limit else ""
    return f"{sample}{extra}"


def resolve_ranking_dispatch_token(ranking_token: str = "", tracker_token: str = "") -> str:
    """Retorna RANKING_DISPATCH_TOKEN quando definido, com fallback automático para TRACKER_DISPATCH_TOKEN."""
    clean_ranking = (ranking_token or "").strip()
    if clean_ranking:
        return clean_ranking
    return (tracker_token or "").strip()


def get_ranking_daily_date(now_brt: datetime) -> str:
    """Retorna a data BRT (YYYY-MM-DD) do Boletim Diário + Jogo do Dia a partir das 08:03 BRT, ou '' antes da janela."""
    if (now_brt.hour, now_brt.minute) >= (8, 3):
        return now_brt.strftime("%Y-%m-%d")
    return ""


def get_ranking_unified_slot(now_brt: datetime) -> str:
    """Retorna o slot 2x/dia do Ranking Unificado + Times 2v2 (00:02 BRT e 12:02 BRT), ou '' fora da janela."""
    date_str = now_brt.strftime("%Y-%m-%d")
    hm = (now_brt.hour, now_brt.minute)
    if (0, 2) <= hm < (12, 0):
        return f"{date_str}-00"
    if hm >= (12, 2):
        return f"{date_str}-12"
    return ""


def get_ranking_bg_slot(now_brt: datetime) -> str:
    """Retorna o slot semanal ISO (YYYY-Www) de Campeões BR por Battlegroup (sáb >=12:05 até dom 11:59 BRT), ou ''."""
    wd = now_brt.weekday()
    hm = (now_brt.hour, now_brt.minute)
    if (wd == 5 and hm >= (12, 5)) or (wd == 6 and hm <= (11, 59)):
        iso_year, iso_week, _ = now_brt.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    return ""


# Fallback embutido caso players_roster.json (base única compartilhada) esteja temporariamente inacessível (48 contas)
FALLBACK_MONITORED_PROFILES: tuple[tuple[int, str], ...] = (
    (79327, "Chacineiro"),
    (1333418, "Charles Bronson (Smurf: Chacineiro)"),
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
    (944488, "gbytes™"),
    (21206, "hjax"),
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
    (557890, "gabe44arg (Smurf: Gabe)"),
    (26910, "nigo_BR ᵀᴹ"),
    (6557, "Dragoness"),
    (1226910, "Lupus Alba"),
    (1005730, "arc"),
    (1423426, "geladeira velha (Smurf: arc)"),
    (1487738, "Državljan H (Smurf: arc)"),
    (1482961, "Vog (Smurf: arc)"),
    (917917, "arc (Smurf: arc)"),
    (1160699, "Bubebolos (Smurf: arc)"),
    (918241, "Faint (Smurf: arc)"),
    (31145, "vonMises"),
    (58737, "INFIEL"),
    (431802, "Conflict.BR"),
    (59864, "Lion Heart"),
    (84821, "TJBR"),
    (99722, "MonteiroBR"),
    (31061, "ACB"),
    (737235, "meuqsaco"),
    (1097608, "Winter Animal"),
    (975688, "Kefbr"),
    (1390350, "TearsInTheRain (Smurf: Chacineiro)"),
    (1119784, "Tracker"),
)

# Mapa fallback SteamID64 -> profile_id (permite consumir `last_steam_fast_sig[0]` do Radar sem usar chave Steam)
FALLBACK_STEAM_ID_TO_PID: dict[str, int] = {
    "76561197963172584": 79327,    # Chacineiro
    "76561198698969876": 1333418,  # Charles Bronson (Smurf: Chacineiro)
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
    "76561198063124280": 944488,   # gbytes™
    "76561198061434984": 21206,    # hjax
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
    "76561199538846112": 557890,   # gabe44arg (Smurf: Gabe)
    "76561197963741792": 26910,    # nigo_BR ᵀᴹ
    "76561198135142137": 6557,     # Dragoness
    "76561198140782132": 1226910,  # Lupus Alba
    "76561199791811246": 1005730,  # arc
    "76561199482388323": 1423426,  # geladeira velha (Smurf: arc)
    "76561198602236177": 1487738,  # Državljan H (Smurf: arc)
    "76561199886227102": 1482961,  # Vog (Smurf: arc)
    "76561198287157768": 917917,   # arc (Smurf: arc)
    "76561199814547622": 1160699,  # Bubebolos (Smurf: arc)
    "76561199794093952": 918241,   # Faint (Smurf: arc)
    "76561197985268629": 31145,    # vonMises
    "76561197993922195": 58737,    # INFIEL
    "76561198000099422": 431802,   # Conflict.BR
    "76561198123446420": 59864,    # Lion Heart
    "76561198076576055": 84821,    # TJBR
    "76561198021071716": 99722,    # MonteiroBR
    "76561198009056438": 31061,    # ACB
    "76561198026394732": 737235,   # meuqsaco
    "76561199498421192": 1097608,  # Winter Animal
    "76561197994245218": 975688,   # Kefbr
    "76561198089278659": 1390350,  # TearsInTheRain (Smurf: Chacineiro)
    "76561198000067932": 1119784,  # Tracker
}

# Contas secundárias mescladas preservadas quando a conta principal está no players_roster.json remoto (vazio: todas independentes)
LOCAL_MERGED_ACCOUNT_FALLBACKS: tuple[tuple[int, str, str, int], ...] = ()

MONITORED_PROFILES: tuple[tuple[int, str], ...] = FALLBACK_MONITORED_PROFILES
MONITORED_PID_SET: frozenset[int] = frozenset(pid for pid, _ in MONITORED_PROFILES)
STEAM_ID_TO_PID: dict[str, int] = dict(FALLBACK_STEAM_ID_TO_PID)
_LAST_FETCHED_STEAM_ID_TO_PID: dict[str, int] = {}

HTTPS_KEEPALIVE_IDLE_SEC = 30.0
HTTPS_KEEPALIVE_MAX_PER_HOST = 8

_TLS_LOCALS = threading.local()
_RAW_JSON_CACHE_LOCK = threading.Lock()
_RAW_JSON_ETAG_CACHE: dict[str, tuple[str, dict[str, Any]]] = {}
_KA_POOL_LOCK = threading.Lock()
_KA_IDLE_CONNS: dict[tuple[str, int], list[tuple[http.client.HTTPSConnection, float]]] = {}


def close_keepalive_pool() -> None:
    """Fecha e descarta todas as conexões HTTPS ociosas do pool Keep-Alive (evita sockets pendurados entre ciclos)."""
    with _KA_POOL_LOCK:
        buckets = list(_KA_IDLE_CONNS.values())
        _KA_IDLE_CONNS.clear()
    for items in buckets:
        for conn, _ in items:
            try:
                conn.close()
            except Exception:
                pass


def _clear_http_caches() -> None:
    """Limpa caches HTTP em memória e fecha conexões ociosas do pool Keep-Alive."""
    with _RAW_JSON_CACHE_LOCK:
        _RAW_JSON_ETAG_CACHE.clear()
    close_keepalive_pool()


def _acquire_keepalive_conn(
    host: str,
    port: int,
    timeout: float,
    context: ssl.SSLContext,
    can_pool: bool,
) -> tuple[http.client.HTTPSConnection, bool]:
    key = (host.lower(), int(port))
    if can_pool:
        now = time.monotonic()
        stale: list[http.client.HTTPSConnection] = []
        chosen: http.client.HTTPSConnection | None = None
        with _KA_POOL_LOCK:
            bucket = _KA_IDLE_CONNS.get(key)
            while bucket:
                conn, last_used = bucket.pop()
                if (now - last_used) <= HTTPS_KEEPALIVE_IDLE_SEC and getattr(conn, "sock", None) is not None:
                    chosen = conn
                    break
                stale.append(conn)
            if bucket is not None and not bucket:
                _KA_IDLE_CONNS.pop(key, None)
        for old in stale:
            try:
                old.close()
            except Exception:
                pass
        if chosen is not None:
            try:
                chosen.timeout = timeout
                if chosen.sock is not None:
                    chosen.sock.settimeout(timeout)
            except Exception:
                pass
            return chosen, True
    return http.client.HTTPSConnection(host, int(port), timeout=timeout, context=context), False


def _release_keepalive_conn(key: tuple[str, int], conn: http.client.HTTPSConnection) -> None:
    if getattr(conn, "sock", None) is None:
        try:
            conn.close()
        except Exception:
            pass
        return
    overflow = False
    with _KA_POOL_LOCK:
        bucket = _KA_IDLE_CONNS.setdefault(key, [])
        if len(bucket) < HTTPS_KEEPALIVE_MAX_PER_HOST:
            bucket.append((conn, time.monotonic()))
        else:
            overflow = True
    if overflow:
        try:
            conn.close()
        except Exception:
            pass


class _PooledHTTPSResponse:
    """Wrapper sobre `http.client.HTTPResponse` que só devolve a conexão TLS ao pool se o corpo for drenado até EOF."""

    def __init__(
        self,
        resp: http.client.HTTPResponse,
        conn: http.client.HTTPSConnection,
        pool_key: tuple[str, int],
        url: str,
        keep_alive: bool,
    ) -> None:
        self._resp = resp
        self._conn = conn
        self._pool_key = pool_key
        self._keep_alive = keep_alive
        self._eof = False
        self._closed = False
        self.url = url
        self.code = int(resp.status)
        self.status = int(resp.status)
        self.msg = str(resp.reason or "")
        self.headers = resp.headers

    def getcode(self) -> int:
        return self.code

    def geturl(self) -> str:
        return self.url

    def info(self) -> Any:
        return self.headers

    def read(self, amt: int | None = None) -> bytes:
        chunk = self._resp.read() if amt is None else self._resp.read(amt)
        if amt is None or not chunk or self._resp.isclosed():
            self._eof = True
        return chunk

    def readline(self, limit: int = -1) -> bytes:
        line = self._resp.readline(limit)
        if not line or self._resp.isclosed():
            self._eof = True
        return line

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        reusable = (
            self._keep_alive
            and (self._eof or self._resp.isclosed())
            and not self._resp.will_close
            and getattr(self._conn, "sock", None) is not None
        )
        if reusable:
            _release_keepalive_conn(self._pool_key, self._conn)
        else:
            try:
                self._resp.close()
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass

    def __enter__(self) -> _PooledHTTPSResponse:
        return self

    def __exit__(self, *args: object) -> bool:
        self.close()
        return False

    def __del__(self) -> None:
        if not self._closed:
            self._keep_alive = False
            try:
                self.close()
            except Exception:
                pass


class _KeepAliveHTTPSHandler(urllib.request.HTTPSHandler):
    """Handler HTTPS TLS 1.2+ com reuso Keep-Alive para GET idempotente (elimina ~42 handshakes TLS por ciclo)."""

    def __init__(self, context: ssl.SSLContext) -> None:
        super().__init__(context=context)
        self._ssl_context = context

    def https_open(self, req: urllib.request.Request) -> Any:
        full_url = req.full_url
        parsed = urllib.parse.urlparse(full_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise urllib.error.URLError("Somente requisições HTTPS com host válido são permitidas.")
        host = parsed.hostname
        port = parsed.port or 443
        target = (parsed.path or "/") + (f"?{parsed.query}" if parsed.query else "")
        method = req.get_method().upper()
        body = req.data
        timeout = float(getattr(req, "timeout", 15) or 15)
        can_pool = method == "GET" and body is None

        req_headers: dict[str, str] = {}
        for k, v in req.header_items():
            if k.lower() in ("host", "connection"):
                continue
            req_headers[k] = v
        req_headers["Connection"] = "keep-alive" if can_pool else "close"
        if body is not None and not any(k.lower() == "content-length" for k in req_headers):
            req_headers["Content-Length"] = str(len(body))

        conn, reused = _acquire_keepalive_conn(host, port, timeout, self._ssl_context, can_pool=can_pool)
        try:
            try:
                conn.request(method, target, body=body, headers=req_headers)
                resp = conn.getresponse()
            except (
                http.client.RemoteDisconnected,
                http.client.BadStatusLine,
                BrokenPipeError,
                ConnectionResetError,
                TimeoutError,
                OSError,
            ) as exc:
                if not reused:
                    raise
                try:
                    conn.close()
                except Exception:
                    pass
                conn = http.client.HTTPSConnection(host, port, timeout=timeout, context=self._ssl_context)
                conn.request(method, target, body=body, headers=req_headers)
                resp = conn.getresponse()
        except Exception as exc:
            try:
                conn.close()
            except Exception:
                pass
            raise urllib.error.URLError(exc) from exc

        if resp.status not in (200, 204):
            try:
                err_bytes = resp.read(4096)
            except Exception:
                err_bytes = b""
            hdrs = resp.headers
            status_code = int(resp.status)
            reason = str(resp.reason or "")
            try:
                resp.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            wrapped = urllib.response.addinfourl(io.BytesIO(err_bytes), hdrs, full_url, code=status_code)
            wrapped.msg = reason  # type: ignore[attr-defined]
            return wrapped

        return _PooledHTTPSResponse(
            resp=resp,
            conn=conn,
            pool_key=(host.lower(), port),
            url=full_url,
            keep_alive=(can_pool and resp.status == 200 and not resp.will_close),
        )


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
        # ProxyHandler({}) bloqueia redirecionamento implícito via HTTP_PROXY/HTTPS_PROXY (protege headers Authorization)
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _HttpsOnlySchemeHandler(),
            _KeepAliveHTTPSHandler(context=ctx),
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
    """Valida esquema HTTPS, ASCII estrito, host exato, porta 443, ausência de credenciais/fragmentos/traversal e prefixo."""
    clean = (url or "").strip()
    if not clean or not clean.isascii() or URL_FORBIDDEN_CHARS_RE.search(clean) or URL_ENCODED_CTRL_RE.search(clean):
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
    raw_len = str(resp.headers.get("Content-Length") or "").strip() if hasattr(resp, "headers") else ""
    if raw_len.isdigit() and int(raw_len) > max_bytes:
        raise ValueError(f"Resposta HTTP anunciou tamanho acima do limite seguro ({max_bytes} bytes).")

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


def _safe_literal_eval(raw: str, max_len: int = 65536, max_depth: int = MAX_AST_NESTING_DEPTH) -> Any:
    """Wrapper defensivo sobre `ast.literal_eval` com limite de tamanho e profundidade de aninhamento (anti-DoS)."""
    if not isinstance(raw, str) or not (2 <= len(raw) <= max_len):
        return None
    depth = 0
    in_quote = ""
    escaped = False
    for ch in raw:
        if escaped:
            escaped = False
            continue
        if in_quote:
            if ch == "\\":
                escaped = True
            elif ch == in_quote:
                in_quote = ""
            continue
        if ch in ("'", '"'):
            in_quote = ch
        elif ch in "([{":
            depth += 1
            if depth > max_depth:
                return None
        elif ch in ")]}":
            depth = max(0, depth - 1)
    try:
        return ast.literal_eval(raw)
    except (ValueError, SyntaxError, TypeError, RecursionError, MemoryError):
        return None


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

    dig = _safe_literal_eval(data.get("last_digest") or "")
    if isinstance(dig, tuple) and dig and isinstance(dig[0], tuple):
        vet_tuple = dig[0]
        if len(vet_tuple) >= 1:
            active_pids.update(_extract_pids_from_seq(vet_tuple[0]))
        if len(vet_tuple) >= 3:
            active_pids.update(_extract_pids_from_seq(vet_tuple[2]))

    fast = _safe_literal_eval(data.get("last_steam_fast_sig") or "")
    if isinstance(fast, tuple) and fast:
        for sid in _extract_steam_ids_from_seq(fast[0]):
            mapped_pid = sid_map.get(sid)
            if mapped_pid is not None and mapped_pid > 0:
                active_pids.add(int(mapped_pid))

    return active_pids & allowed_pids


def _fetch_github_raw_json(
    url: str,
    expected_path: str,
    max_bytes: int = MAX_RADAR_BYTES,
    timeout: int = 8,
) -> dict[str, Any] | None:
    """Lê JSON público de `raw.githubusercontent.com` com validação estrita de URL, limite de bytes e cache ETag/304."""
    parsed = _validate_outbound_https(url, RADAR_HOST, path_prefix=expected_path, allow_query=False)
    if parsed.path != expected_path:
        raise ValueError("Caminho raw.githubusercontent.com fora da allowlist.")

    with _RAW_JSON_CACHE_LOCK:
        cached_entry = _RAW_JSON_ETAG_CACHE.get(url)

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "Cache-Control": "no-cache",
    }
    if cached_entry is not None and cached_entry[0]:
        headers["If-None-Match"] = cached_entry[0]

    for attempt in range(2):
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with _get_opener().open(req, timeout=timeout) as resp:
                data = _read_bounded_json(resp, max_bytes=max_bytes)
                etag = str(resp.headers.get("ETag") or "").strip() if hasattr(resp, "headers") else ""
            if etag:
                with _RAW_JSON_CACHE_LOCK:
                    _RAW_JSON_ETAG_CACHE[url] = (etag, copy.deepcopy(data))
            return data
        except urllib.error.HTTPError as exc:
            try:
                exc.read(256)
            except Exception:
                pass
            if int(exc.code or 0) == 304 and cached_entry is not None:
                return copy.deepcopy(cached_entry[1])
            if attempt == 0:
                time.sleep(0.6)
        except Exception:
            if attempt == 0:
                time.sleep(0.6)
    return None


def fetch_veteranos_radar_active_pids(max_age_sec: int = RADAR_MAX_AGE_SEC) -> tuple[set[int], int, bool]:
    """
    Lê (estritamente somente-leitura) o `state.json` público do Radar dos Veteranos
    (`nigosky/coh3-br-lobby-radar`) e retorna `(active_pids, last_sync_ts, ok)`.
    Não exige nem manipula `STEAM_WEB_API_KEY`.
    """
    data = _fetch_github_raw_json(RADAR_STATE_URL, expected_path=RADAR_STATE_PATH, max_bytes=MAX_RADAR_BYTES)
    if data is None:
        return set(), 0, False
    try:
        last_sync_ts = int(data.get("last_sync_ts", 0) or 0)
    except (TypeError, ValueError):
        last_sync_ts = 0
    now_ts = int(time.time())
    if max_age_sec > 0 and last_sync_ts > 0 and (now_ts - last_sync_ts) > max_age_sec:
        return set(), last_sync_ts, False
    return _parse_radar_state_active_pids(data), last_sync_ts, True


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
        alias = _sanitize_alias(row.get("original_alias") or row.get("alias"), fallback=f"Player {pid}")
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
    data = _fetch_github_raw_json(url, expected_path=SHARED_ROSTER_PATH, max_bytes=MAX_RADAR_BYTES)
    if data is None:
        return None
    try:
        profiles, sid_map = parse_shared_roster_details(data)
    except Exception:
        return None
    _LAST_FETCHED_STEAM_ID_TO_PID = dict(sid_map)
    return profiles


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


def _fetch_personal_stat_chunk(chunk: list[int]) -> dict[int, int]:
    """Consulta um lote (até 10 contas) em `getPersonalStat` com 1 retry automático."""
    target_set = set(chunk)
    encoded_ids = urllib.parse.quote("[" + ",".join(str(p) for p in chunk) + "]")
    url = f"{BASE_URL}/getPersonalStat?title=coh3&profile_ids={encoded_ids}"
    _validate_outbound_https(url, RELIC_HOST, path_prefix="/community/leaderboard/")

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
    }
    data: dict[str, Any] | None = None
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with _get_opener().open(req, timeout=12) as resp:
                data = _read_bounded_json(resp)
            break
        except Exception:
            if attempt == 0:
                time.sleep(0.5)
    if data is None:
        return {}

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

    chunk_latest: dict[int, int] = {}
    for st in data.get("leaderboardStats") or []:
        if not isinstance(st, dict):
            continue
        sg_id = int(st.get("statgroup_id", -1))
        lmd = int(st.get("lastmatchdate", 0) or 0)
        if lmd <= 0 or sg_id not in sg_to_pids:
            continue
        for pid in sg_to_pids[sg_id]:
            if lmd > chunk_latest.get(pid, 0):
                chunk_latest[pid] = lmd
    return chunk_latest


def fetch_personal_stat_last_match(pids: list[int]) -> dict[int, int]:
    """
    Consulta `getPersonalStat` da Relic (lotes de até 10 contas, em paralelo para auditorias amplas),
    retornando `{profile_id: max_lastmatchdate}`.
    """
    clean_pids = sorted({int(p) for p in pids if int(p) > 0})
    if not clean_pids:
        return {}

    chunks = [clean_pids[idx : idx + 10] for idx in range(0, len(clean_pids), 10)]
    if len(chunks) == 1:
        return _fetch_personal_stat_chunk(chunks[0])

    latest: dict[int, int] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(3, len(chunks))) as pool:
        for chunk_res in pool.map(_fetch_personal_stat_chunk, chunks):
            for pid, lmd in chunk_res.items():
                if lmd > latest.get(pid, 0):
                    latest[pid] = lmd
    return latest


def fetch_latest_player_marker(profile_id: int) -> tuple[int, int, int, bool, int, int]:
    """Retorna (profile_id, max_match_id, max_completion_time, ok, max_bg_eligible_id, max_bg_eligible_ct) para uma conta monitorada."""
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
            with _get_opener().open(req, timeout=12) as resp:
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
                if mt_id in BG_ELIGIBLE_TYPE_IDS:
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
    inputs: dict[str, Any] | None = None,
) -> tuple[bool, int]:
    """Dispara workflow_dispatch em repositório privado via HTTPS TLS 1.2+ sem expor segredos."""
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

    payload_obj: dict[str, Any] = {"ref": clean_ref}
    if inputs is not None:
        if not isinstance(inputs, dict) or len(inputs) > 25:
            raise ValueError("Parâmetro GitHub inválido (inputs).")
        clean_inputs: dict[str, Any] = {}
        for key, val in inputs.items():
            k = str(key or "").strip()
            if not k or not SAFE_GH_SEGMENT_RE.fullmatch(k):
                raise ValueError(f"Chave de input GitHub inválida ({k!r}).")
            if not isinstance(val, (str, bool, int, float)):
                raise ValueError(f"Tipo de input GitHub inválido para {k!r}.")
            clean_inputs[k] = val
        payload_obj["inputs"] = clean_inputs

    body = json.dumps(payload_obj).encode("utf-8")
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
                resp.read(1024)
                if last_code == 204:
                    return True, 204
        except urllib.error.HTTPError as exc:
            last_code = int(exc.code or 0)
            try:
                exc.read(1024)
            except Exception:
                pass
            if last_code in (400, 401, 403, 404, 422):
                break
        except Exception:
            last_code = 0

        if attempt < 3:
            time.sleep(attempt * 3)

    return False, last_code


def evaluate_ranking_dispatches(
    now_brt: datetime,
    prev_state: dict[str, Any],
    ranking_token: str = "",
    tracker_token: str = "",
    dry_run: bool = False,
    owner: str = RANKING_TARGET_OWNER,
    repo: str = RANKING_TARGET_REPO,
    ref: str = RANKING_TARGET_REF,
    daily_workflow: str = RANKING_DAILY_WORKFLOW,
    unified_workflow: str = RANKING_UNIFIED_WORKFLOW,
    bg_workflow: str = RANKING_BG_WORKFLOW,
) -> tuple[dict[str, str], list[dict[str, Any]], bool]:
    """
    Avalia e dispara os 3 workflows agendados de `nigosky/coh3-br-ranking`:
    1. Boletim Diário + Jogo do Dia (`boletim_diario.yml`) >= 08:03 BRT (`last_ranking_daily_date`)
    2. Ranking Unificado + Times 2v2 (`ranking_unificado.yml`) >= 00:02 e >= 12:02 BRT (`last_ranking_slot`)
    3. Campeões BR por Battlegroup (`campeoes_battlegroups.yml`) sáb >= 12:05 até dom 11:59 BRT (`last_ranking_bg_slot`)

    Retorna `(next_markers, events, state_changed)`.
    Cada marcador só avança se o respectivo disparo retornar HTTP 204 (ou em `dry_run`, sem persistir em disco).
    """
    markers: dict[str, str] = {
        "last_ranking_daily_date": str(prev_state.get("last_ranking_daily_date") or ""),
        "last_ranking_slot": str(prev_state.get("last_ranking_slot") or ""),
        "last_ranking_bg_slot": str(prev_state.get("last_ranking_bg_slot") or ""),
    }
    schedule_specs: tuple[tuple[str, str, str, str, dict[str, Any] | None], ...] = (
        (
            "last_ranking_daily_date",
            "Boletim Diario + Jogo do Dia",
            daily_workflow,
            get_ranking_daily_date(now_brt),
            None,
        ),
        (
            "last_ranking_slot",
            "Ranking Unificado + Times 2v2",
            unified_workflow,
            get_ranking_unified_slot(now_brt),
            dict(RANKING_UNIFIED_INPUTS),
        ),
        (
            "last_ranking_bg_slot",
            "Campeoes BR por Battlegroup",
            bg_workflow,
            get_ranking_bg_slot(now_brt),
            None,
        ),
    )

    clean_ranking_token = (ranking_token or "").strip()
    clean_tracker_token = (tracker_token or "").strip()
    effective_token = resolve_ranking_dispatch_token(clean_ranking_token, clean_tracker_token)
    if clean_ranking_token:
        token_source = "RANKING_DISPATCH_TOKEN"
    elif clean_tracker_token:
        token_source = "TRACKER_DISPATCH_TOKEN (fallback)"
    else:
        token_source = "AUSENTE"

    events: list[dict[str, Any]] = []
    state_changed = False

    for marker_key, label, workflow, target_slot, inputs in schedule_specs:
        if not target_slot or markers[marker_key] == target_slot:
            continue

        if dry_run:
            ok, http_code = True, 204
        elif effective_token:
            ok, http_code = dispatch_private_tracker(
                token=effective_token,
                owner=owner,
                repo=repo,
                workflow=workflow,
                ref=ref,
                inputs=inputs,
            )
        else:
            ok, http_code = False, 0

        events.append(
            {
                "marker_key": marker_key,
                "label": label,
                "owner": owner,
                "repo": repo,
                "workflow": workflow,
                "ref": ref,
                "slot": target_slot,
                "inputs": dict(inputs) if inputs is not None else None,
                "ok": ok,
                "http_code": http_code,
                "token_source": token_source,
            }
        )
        if ok:
            markers[marker_key] = target_slot
            state_changed = True

    return markers, events, state_changed


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
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp_name, 0o644)
        if path.is_symlink():
            raise RuntimeError(f"Escrita bloqueada em symlink não confiável: {path}")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def merge_states_monotonic(remote_state: dict[str, Any], local_state: dict[str, Any]) -> dict[str, Any]:
    """
    Mescla dois snapshots de `state.json` de forma estritamente monotônica (sem conflito textual de git):
    - Timestamps numéricos e marcadores ISO de ranking/semana nunca regridem (`max`).
    - Entradas de `players` preservam os maiores `max_match_id`, `max_completion_time` e `max_stat_lmd`.
    - Estado de follow-up BG e lista do Radar prevalecem do snapshot mais recente (empate favorece `local_state`).
    """
    rem = remote_state if isinstance(remote_state, dict) else {}
    loc = local_state if isinstance(local_state, dict) else {}

    def _int(d: dict[str, Any], key: str, default: int = 0) -> int:
        try:
            return int(d.get(key, default) or default)
        except (TypeError, ValueError):
            return default

    def _str(d: dict[str, Any], key: str) -> str:
        return str(d.get(key) or "").strip()

    rem_dispatch_ts = _int(rem, "last_dispatch_ts")
    loc_dispatch_ts = _int(loc, "last_dispatch_ts")
    bg_src = loc if loc_dispatch_ts >= rem_dispatch_ts else rem

    rem_radar_ts = _int(rem, "radar_last_sync_ts")
    loc_radar_ts = _int(loc, "radar_last_sync_ts")
    radar_src = loc if loc_radar_ts >= rem_radar_ts else rem

    merged_players: dict[str, dict[str, int]] = {}
    rem_players = rem.get("players") if isinstance(rem.get("players"), dict) else {}
    loc_players = loc.get("players") if isinstance(loc.get("players"), dict) else {}
    for key in sorted(set(rem_players.keys()) | set(loc_players.keys())):
        if not str(key).isdigit():
            continue
        r_ent = rem_players.get(key) if isinstance(rem_players.get(key), dict) else {}
        l_ent = loc_players.get(key) if isinstance(loc_players.get(key), dict) else {}
        entry: dict[str, int] = {
            "max_match_id": max(_int(r_ent, "max_match_id"), _int(l_ent, "max_match_id")),
            "max_completion_time": max(_int(r_ent, "max_completion_time"), _int(l_ent, "max_completion_time")),
        }
        max_stat = max(_int(r_ent, "max_stat_lmd"), _int(l_ent, "max_stat_lmd"))
        if max_stat > 0:
            entry["max_stat_lmd"] = max(max_stat, entry["max_completion_time"])
        merged_players[str(key)] = entry

    return {
        "schema_version": max(2, _int(rem, "schema_version", 2), _int(loc, "schema_version", 2)),
        "last_checked_ts": max(_int(rem, "last_checked_ts"), _int(loc, "last_checked_ts")),
        "last_dispatch_ts": max(rem_dispatch_ts, loc_dispatch_ts),
        "last_full_stat_audit_ts": max(_int(rem, "last_full_stat_audit_ts"), _int(loc, "last_full_stat_audit_ts")),
        "last_weekly_summary_iso_week": max(
            _str(rem, "last_weekly_summary_iso_week"),
            _str(loc, "last_weekly_summary_iso_week"),
        ),
        "last_ranking_daily_date": max(_str(rem, "last_ranking_daily_date"), _str(loc, "last_ranking_daily_date")),
        "last_ranking_slot": max(_str(rem, "last_ranking_slot"), _str(loc, "last_ranking_slot")),
        "last_ranking_bg_slot": max(_str(rem, "last_ranking_bg_slot"), _str(loc, "last_ranking_bg_slot")),
        "pending_bg_followup_due_ts": _int(bg_src, "pending_bg_followup_due_ts"),
        "pending_bg_followup_match_id": _int(bg_src, "pending_bg_followup_match_id"),
        "pending_bg_followup_wave": _int(bg_src, "pending_bg_followup_wave"),
        "radar_coh3_pids": sorted(_extract_pids_from_seq(radar_src.get("radar_coh3_pids"))),
        "radar_last_sync_ts": max(rem_radar_ts, loc_radar_ts),
        "players": merged_players,
    }


def write_github_outputs(outputs: dict[str, str]) -> None:
    gh_out = os.environ.get("GITHUB_OUTPUT", "").strip()
    if not gh_out:
        return
    out_path = Path(gh_out)
    if out_path.is_symlink():
        raise RuntimeError("GITHUB_OUTPUT não pode ser symlink.")
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(out_path, flags, 0o600)
    except OSError as exc:
        raise RuntimeError("Falha segura ao abrir GITHUB_OUTPUT (symlink bloqueado).") from exc
    with os.fdopen(fd, "a", encoding="utf-8") as fh:
        for k, v in outputs.items():
            clean_k = str(k).strip()
            if not clean_k or not re.fullmatch(r"[A-Za-z0-9_]+", clean_k):
                continue
            clean_v = _sanitize_log_line(v).replace("<", "").replace(">", "").replace("`", "")
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
    ranking_token: str = "",
    ranking_owner: str = RANKING_TARGET_OWNER,
    ranking_repo: str = RANKING_TARGET_REPO,
    ranking_ref: str = RANKING_TARGET_REF,
    ranking_daily_workflow: str = RANKING_DAILY_WORKFLOW,
    ranking_unified_workflow: str = RANKING_UNIFIED_WORKFLOW,
    ranking_bg_workflow: str = RANKING_BG_WORKFLOW,
) -> tuple[bool, bool, str, dict[str, Any]]:
    """
    Executa 1 ciclo de pré-checagem (Relic History + Radar dos Veteranos + Relic PersonalStat + Auditoria de Silêncio 1h)
    e avalia o agendamento dos workflows de `nigosky/coh3-br-ranking`.
    Retorna `(should_dispatch, dispatched_ok, reason_str, meta)`.
    Importante:
    - Só avança marcadores do tracker se `dispatched_ok` for True (evita perder partidas caso o dispatch falhe).
    - Persiste `state.json` mesmo em ciclos sem disparo do tracker quando algum workflow de ranking é disparado com sucesso.
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
    prev_bg_wave = int(prev.get("pending_bg_followup_wave", 0) or 0)
    prev_full_audit_ts = max(
        int(prev.get("last_full_stat_audit_ts", 0) or 0),
        int(last_full_stat_audit_ts_override or 0),
    )
    prev_radar_pids = (
        set(prev_radar_pids_override)
        if prev_radar_pids_override is not None
        else _extract_pids_from_seq(prev.get("radar_coh3_pids"))
    )

    alias_by_pid = {pid: _sanitize_alias(alias, fallback=f"Player {pid}") for pid, alias in MONITORED_PROFILES}
    all_monitored_pids = [pid for pid, _ in MONITORED_PROFILES]

    # Consulta Relic History + Radar dos Veteranos em paralelo (6 workers Keep-Alive evita throttling da Relic)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        radar_future = pool.submit(fetch_veteranos_radar_active_pids)
        results = list(pool.map(fetch_latest_player_marker, all_monitored_pids))
        curr_radar_pids, radar_sync_ts, radar_ok = radar_future.result()

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

    radar_failed_pids: set[int] = set()
    for fpid in sorted(retry_candidate_pids):
        _, _, _, ok_first, _, _ = results_by_pid.get(fpid, (fpid, 0, 0, False, 0, 0))
        if not ok_first:
            retry_res = fetch_latest_player_marker(fpid)
            results_by_pid[fpid] = retry_res
            if not retry_res[3] and fpid in focus_radar_pids:
                radar_failed_pids.add(fpid)

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
        _, max_mid, max_ct, ok, max_auto_mid, max_auto_ct = results_by_pid.get(pid, (pid, 0, 0, False, 0, 0))
        key = str(pid)
        if not ok:
            continue
        ok_count += 1
        is_existing_player = key in prev_players
        old_entry = prev_players.get(key) or {}
        old_mid = int(old_entry.get("max_match_id", 0) or 0)
        old_ct = int(old_entry.get("max_completion_time", 0) or 0)
        old_stat_lmd = int(old_entry.get("max_stat_lmd", 0) or 0)
        if prev_players and (max_mid > old_mid or max_ct > old_ct):
            # Contas recém-adicionadas ao roster com última partida antiga (>72h) apenas semeiam o baseline (evita falso positivo)
            if is_existing_player or (max_ct > 0 and (now_ts - max_ct) <= QUIET_STAT_RECENT_WINDOW_SEC):
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
    stat_lmd_map: dict[int, int] = {}
    stat_target_pids = sorted(set(all_monitored_pids) if run_full_stat_audit else focus_radar_pids)
    if stat_target_pids:
        stat_lmd_map = fetch_personal_stat_last_match(stat_target_pids)
        for fpid in stat_target_pids:
            stat_lmd = int(stat_lmd_map.get(fpid, 0) or 0)
            if stat_lmd <= 0:
                continue
            key = str(fpid)
            is_existing_player = key in prev_players
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
            if not is_existing_player and (now_ts - stat_lmd) > QUIET_STAT_RECENT_WINDOW_SEC:
                continue
            if fpid in focus_radar_pids:
                if stat_lmd > (baseline_ts + 10):
                    radar_stat_triggers.append(alias_by_pid.get(fpid, str(fpid)))
            elif (
                (now_ts - stat_lmd) <= QUIET_STAT_RECENT_WINDOW_SEC
                and stat_lmd > (baseline_ts + QUIET_STAT_TOLERANCE_SEC)
            ):
                quiet_stat_triggers.append(alias_by_pid.get(fpid, str(fpid)))

    # Só dispara fallback do Radar se getPersonalStat TAMBÉM não respondeu para a conta com falha
    radar_failed_aliases = [
        alias_by_pid.get(fpid, str(fpid))
        for fpid in sorted(radar_failed_pids)
        if int(stat_lmd_map.get(fpid, 0) or 0) <= 0
    ]

    reasons: list[str] = []
    mark_weekly_done = False

    if force_dispatch:
        reasons.append("disparo manual forcado (workflow_dispatch)")
    if not prev_players:
        reasons.append("inicializacao do baseline de estado (1a execucao)")
    if new_activity_players:
        reasons.append(f"nova partida detectada: {_format_sample(new_activity_players)}")
    if radar_stat_triggers:
        reasons.append(f"Radar Veteranos + Relic PersonalStat detectou partida recente: {_format_sample(radar_stat_triggers)}")
    if quiet_stat_triggers:
        reasons.append(f"Auditoria de silencio (1h) + Relic PersonalStat detectou partida recente: {_format_sample(quiet_stat_triggers)}")
    if radar_failed_aliases:
        reasons.append(f"Radar Veteranos: fallback p/ rechecagem Relic ({_format_sample(radar_failed_aliases)})")
    bg_followup_due = (
        prev_bg_due_ts > 0
        and now_ts >= prev_bg_due_ts
        and (now_ts - prev_bg_due_ts) <= BG_FOLLOWUP_TTL_SEC
    )
    active_bg_wave = prev_bg_wave if prev_bg_wave in (1, 2) else 1
    if bg_followup_due and not new_activity_players and not radar_stat_triggers and not quiet_stat_triggers:
        mid_hint = f" #{prev_bg_match_id}" if prev_bg_match_id > 0 else ""
        delay_hint = "+5 min" if active_bg_wave == 1 else "+30 min"
        reasons.append(f"confirmacao de Battlegroups cohdb (onda {active_bg_wave}/2, {delay_hint} apos partida{mid_hint})")
    if now_brt.weekday() == 0 and (now_brt.hour, now_brt.minute) >= (0, 5) and last_weekly_week != current_iso_week:
        reasons.append(f"janela do Resumo Semanal ({current_iso_week} segunda >= 00:05 BRT)")
        mark_weekly_done = True
    outage_cooldown_active = False
    if ok_count == 0:
        if last_dispatch_ts <= 0 or (now_ts - last_dispatch_ts) >= RELIC_OUTAGE_FALLBACK_COOLDOWN_SEC:
            reasons.append("fallback de seguranca (Relic API indisponivel na pre-checagem)")
        else:
            outage_cooldown_active = True
    elif last_dispatch_ts <= 0 or (now_ts - last_dispatch_ts) >= HEARTBEAT_INTERVAL_SEC:
        elapsed_h = round((now_ts - last_dispatch_ts) / 3600.0, 1) if last_dispatch_ts > 0 else 0.0
        reasons.append(f"heartbeat periodico de auditoria profunda ({elapsed_h}h desde ultimo ciclo)")

    should_dispatch = len(reasons) > 0
    audit_suffix = " | auditoria silencio 1h OK (100% PersonalStat)" if run_full_stat_audit else ""
    if reasons:
        reason_str = " | ".join(reasons)
    elif outage_cooldown_active:
        reason_str = f"Relic API indisponivel (0/{len(MONITORED_PROFILES)} contas OK; circuit breaker 30m ativo)"
    else:
        reason_str = f"sem novas partidas ({ok_count}/{len(MONITORED_PROFILES)} contas verificadas{audit_suffix})"

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

    ranking_markers, ranking_events, ranking_state_changed = evaluate_ranking_dispatches(
        now_brt=now_brt,
        prev_state=prev,
        ranking_token=ranking_token,
        tracker_token=dispatch_token,
        dry_run=dry_run,
        owner=ranking_owner,
        repo=ranking_repo,
        ref=ranking_ref,
        daily_workflow=ranking_daily_workflow,
        unified_workflow=ranking_unified_workflow,
        bg_workflow=ranking_bg_workflow,
    )

    state_persisted = False
    tracker_state_changed = bool(should_dispatch and dispatched_ok)
    if not dry_run:
        if tracker_state_changed:
            if fresh_automatch_mid > 0 or radar_stat_triggers or quiet_stat_triggers:
                next_bg_due_ts = now_ts + BG_FOLLOWUP_DELAY_SEC
                next_bg_mid = fresh_automatch_mid
                next_bg_wave = 1
            elif bg_followup_due and active_bg_wave == 1:
                next_bg_due_ts = now_ts + BG_FOLLOWUP_WAVE2_DELAY_SEC
                next_bg_mid = prev_bg_match_id
                next_bg_wave = 2
            elif prev_bg_due_ts > 0 and now_ts < (prev_bg_due_ts - 60):
                next_bg_due_ts = prev_bg_due_ts
                next_bg_mid = prev_bg_match_id
                next_bg_wave = active_bg_wave
            else:
                next_bg_due_ts = 0
                next_bg_mid = 0
                next_bg_wave = 0
            next_state = {
                "schema_version": 2,
                "last_checked_ts": now_ts,
                "last_dispatch_ts": now_ts,
                "last_full_stat_audit_ts": now_ts if run_full_stat_audit else prev_full_audit_ts,
                "last_weekly_summary_iso_week": current_iso_week if mark_weekly_done else last_weekly_week,
                "last_ranking_daily_date": ranking_markers["last_ranking_daily_date"],
                "last_ranking_slot": ranking_markers["last_ranking_slot"],
                "last_ranking_bg_slot": ranking_markers["last_ranking_bg_slot"],
                "pending_bg_followup_due_ts": next_bg_due_ts,
                "pending_bg_followup_match_id": next_bg_mid,
                "pending_bg_followup_wave": next_bg_wave,
                "radar_coh3_pids": sorted(curr_radar_pids) if radar_ok else sorted(prev_radar_pids),
                "radar_last_sync_ts": radar_sync_ts if radar_ok else int(prev.get("radar_last_sync_ts", 0) or 0),
                "players": updated_players,
            }
            atomic_write_json(state_path, next_state)
            state_persisted = True
        elif ranking_state_changed:
            # Persiste marcadores de ranking mesmo em ciclos sem disparo do tracker, preservando estado do tracker intacto
            next_state = dict(prev)
            next_state["schema_version"] = int(prev.get("schema_version", 2) or 2)
            next_state["last_checked_ts"] = now_ts
            next_state["last_ranking_daily_date"] = ranking_markers["last_ranking_daily_date"]
            next_state["last_ranking_slot"] = ranking_markers["last_ranking_slot"]
            next_state["last_ranking_bg_slot"] = ranking_markers["last_ranking_bg_slot"]
            atomic_write_json(state_path, next_state)
            state_persisted = True

    close_keepalive_pool()
    meta = {
        "ok_accounts": ok_count,
        "total_accounts": len(MONITORED_PROFILES),
        "http_code": http_code,
        "radar_ok": radar_ok,
        "radar_active_pids": sorted(curr_radar_pids),
        "radar_exited_pids": sorted(exited_radar_pids),
        "full_stat_audit_ran": run_full_stat_audit,
        "full_stat_audit_ts": now_ts if run_full_stat_audit else prev_full_audit_ts,
        "outage_cooldown_active": outage_cooldown_active,
        "ranking_dispatches": ranking_events,
        "ranking_dispatches_ok": sum(1 for ev in ranking_events if ev["ok"]),
        "ranking_dispatches_failed": sum(1 for ev in ranking_events if not ev["ok"]),
        "state_persisted": state_persisted,
    }
    return should_dispatch, dispatched_ok, reason_str, meta


def main() -> int:
    parser = argparse.ArgumentParser(description="CoH3 BR Smart Pre-Check Scheduler")
    parser.add_argument("--loop-cycles", type=int, default=1, help="Quantidade de ciclos por sessao (1..24)")
    parser.add_argument("--loop-interval-sec", type=int, default=300, help="Intervalo em segundos entre ciclos")
    parser.add_argument(
        "--max-session-sec",
        type=int,
        default=DEFAULT_MAX_SESSION_SEC,
        help="Orcamento maximo da sessao em segundos antes de encerrar graciosamente (0 = ilimitado)",
    )
    parser.add_argument("--force", action="store_true", help="Forca disparo no 1o ciclo")
    parser.add_argument("--dry-run", action="store_true", help="Simula checagem sem disparar nem gravar estado")
    parser.add_argument(
        "--merge-state",
        type=str,
        default="",
        help="Mescla monotonicamente um snapshot local JSON com o state.json atual e encerra",
    )
    args = parser.parse_args()

    repo_dir = Path(__file__).resolve().parent
    state_path = repo_dir / "state.json"

    if args.merge_state:
        candidate_path = Path(args.merge_state)
        if not candidate_path.is_file() or candidate_path.is_symlink():
            print("::error::Arquivo de merge invalido ou symlink bloqueado.", flush=True)
            return 1
        remote_state = load_state(state_path)
        local_state = load_state(candidate_path)
        if not local_state:
            print("::error::Snapshot local de state.json vazio ou invalido para merge.", flush=True)
            return 1
        merged = merge_states_monotonic(remote_state, local_state)
        atomic_write_json(state_path, merged)
        return 0

    cycles = max(1, min(24, int(args.loop_cycles)))
    interval_sec = max(30, min(900, int(args.loop_interval_sec)))
    max_session_sec = max(0, min(7200, int(args.max_session_sec)))

    force_first = args.force or (os.environ.get("FORCE_DISPATCH", "false").strip().lower() == "true")

    dispatch_token = os.environ.get("TRACKER_DISPATCH_TOKEN", "").strip()
    ranking_token = os.environ.get("RANKING_DISPATCH_TOKEN", "").strip()
    target_owner = os.environ.get("TARGET_OWNER", "nigosky").strip() or "nigosky"
    target_repo = os.environ.get("TARGET_REPO", "coh3-br-tracker").strip() or "coh3-br-tracker"
    target_workflow = os.environ.get("TARGET_WORKFLOW", "coh3_monitor.yml").strip() or "coh3_monitor.yml"
    target_ref = os.environ.get("TARGET_REF", "main").strip() or "main"

    ranking_owner = os.environ.get("RANKING_TARGET_OWNER", RANKING_TARGET_OWNER).strip() or RANKING_TARGET_OWNER
    ranking_repo = os.environ.get("RANKING_TARGET_REPO", RANKING_TARGET_REPO).strip() or RANKING_TARGET_REPO
    ranking_ref = os.environ.get("RANKING_TARGET_REF", RANKING_TARGET_REF).strip() or RANKING_TARGET_REF
    ranking_daily_workflow = (
        os.environ.get("RANKING_DAILY_WORKFLOW", RANKING_DAILY_WORKFLOW).strip() or RANKING_DAILY_WORKFLOW
    )
    ranking_unified_workflow = (
        os.environ.get("RANKING_UNIFIED_WORKFLOW", RANKING_UNIFIED_WORKFLOW).strip() or RANKING_UNIFIED_WORKFLOW
    )
    ranking_bg_workflow = (
        os.environ.get("RANKING_BG_WORKFLOW", RANKING_BG_WORKFLOW).strip() or RANKING_BG_WORKFLOW
    )

    if not args.dry_run and not dispatch_token:
        print(
            "::error::O Secret TRACKER_DISPATCH_TOKEN nao esta configurado em Settings -> Secrets and variables -> Actions.",
            flush=True,
        )
        return 1

    stop_event = threading.Event()
    interrupted_signal = ""

    def _handle_stop(signum: int, _frame: Any) -> None:
        nonlocal interrupted_signal
        try:
            interrupted_signal = signal.Signals(signum).name
        except Exception:
            interrupted_signal = f"SIG{signum}"
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_stop)
        except (ValueError, OSError):
            pass

    session_start = time.monotonic()
    executed_cycles = 0
    total_dispatches = 0
    total_ranking_dispatches = 0
    total_skips = 0
    last_ok_str = f"0/{len(MONITORED_PROFILES)}"
    dispatch_events: list[str] = []
    mem_radar_pids: set[int] | None = None
    mem_last_full_stat_audit_ts: int | None = None

    for idx in range(1, cycles + 1):
        if stop_event.is_set():
            break
        cycle_start = time.monotonic()
        if idx == 1 or (idx - 1) % 3 == 0:
            synced = refresh_monitored_profiles()
            if idx == 1:
                src_lbl = "players_roster.json (compartilhado)" if synced else "fallback embutido"
                print(
                    _sanitize_log_line(
                        f"[ROSTER] Base sincronizada ({src_lbl}): {len(MONITORED_PROFILES)} contas monitoradas."
                    ),
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
            ranking_token=ranking_token,
            ranking_owner=ranking_owner,
            ranking_repo=ranking_repo,
            ranking_ref=ranking_ref,
            ranking_daily_workflow=ranking_daily_workflow,
            ranking_unified_workflow=ranking_unified_workflow,
            ranking_bg_workflow=ranking_bg_workflow,
        )
        executed_cycles += 1
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
                    _sanitize_log_line(
                        f"[CICLO {idx}/{cycles} • {now_brt_str}] DISPARADO (HTTP {meta['http_code']}) -> "
                        f"{target_owner}/{target_repo}@{target_ref} | Contas OK={last_ok_str} | {radar_tag} | Motivo: {reason}"
                    ),
                    flush=True,
                )
            else:
                print(
                    "::warning::"
                    + _sanitize_log_line(
                        f"[CICLO {idx}/{cycles} • {now_brt_str}] Falha ao disparar {target_owner}/{target_repo} "
                        f"(HTTP {meta['http_code']}); estado preservado para retentativa no proximo ciclo! | {radar_tag} | Motivo: {reason}"
                    ),
                    flush=True,
                )
        else:
            total_skips += 1
            print(
                _sanitize_log_line(
                    f"[CICLO {idx}/{cycles} • {now_brt_str}] ECONOMIZADO (SKIP) | Contas OK={last_ok_str} | {radar_tag} | {reason}"
                ),
                flush=True,
            )

        for ev in meta.get("ranking_dispatches") or []:
            if ev["ok"]:
                total_ranking_dispatches += 1
                dispatch_events.append(f"[{now_brt_str}] Ranking: {ev['label']} ({ev['workflow']}, slot={ev['slot']})")
                print(
                    _sanitize_log_line(
                        f"[CICLO {idx}/{cycles} • {now_brt_str}] RANKING DISPARADO (HTTP {ev['http_code']}) -> "
                        f"{ev['owner']}/{ev['repo']}/{ev['workflow']}@{ev['ref']} | Slot={ev['slot']} | "
                        f"Token={ev['token_source']} | {ev['label']}"
                    ),
                    flush=True,
                )
            else:
                print(
                    "::warning::"
                    + _sanitize_log_line(
                        f"[CICLO {idx}/{cycles} • {now_brt_str}] Falha ao disparar "
                        f"{ev['owner']}/{ev['repo']}/{ev['workflow']} (HTTP {ev['http_code']}, "
                        f"Token={ev['token_source']}); marcador {ev['marker_key']} preservado para retentativa "
                        f"no proximo ciclo! | Slot={ev['slot']} | {ev['label']}"
                    ),
                    flush=True,
                )

        if idx < cycles:
            elapsed = time.monotonic() - cycle_start
            sleep_for = max(1.0, float(interval_sec) - elapsed)
            if max_session_sec > 0 and (time.monotonic() - session_start + sleep_for) >= float(max_session_sec):
                print(
                    _sanitize_log_line(
                        f"[SESSAO] Limite seguro de {max_session_sec}s atingido apos ciclo {executed_cycles}/{cycles}; "
                        "encerrando graciosamente para persistir estado e acionar Self-Relay."
                    ),
                    flush=True,
                )
                break
            if stop_event.wait(sleep_for):
                print(
                    _sanitize_log_line(
                        f"[SESSAO] Sinal {interrupted_signal or 'STOP'} recebido apos ciclo {executed_cycles}/{cycles}; "
                        "encerrando graciosamente sem processos orfaos."
                    ),
                    flush=True,
                )
                break

    summary_reason = (
        " ; ".join(dispatch_events[-3:])
        if dispatch_events
        else f"Nenhuma partida nova nos {executed_cycles} ciclo(s) ({total_skips} skips)"
    )
    if interrupted_signal:
        summary_reason = f"{summary_reason} [encerrado via {interrupted_signal}]"

    write_github_outputs(
        {
            "total_cycles": str(executed_cycles),
            "dispatches_count": str(total_dispatches),
            "ranking_dispatches_count": str(total_ranking_dispatches),
            "skips_count": str(total_skips),
            "ok_accounts": last_ok_str,
            "summary_reason": summary_reason,
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
