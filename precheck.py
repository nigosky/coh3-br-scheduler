#!/usr/bin/env python3
"""
Pré-checagem inteligente (stdlib-only, TLS 1.2+) com loop contínuo de 5 min para `nigosky/coh3-br-scheduler`.

Consulta diretamente a API pública da Relic (`getRecentMatchHistoryByProfileId`) das 45 contas Steam
monitoradas (cobrindo tanto Automatch ranqueadas quanto partidas Customizadas!) a cada 5 minutos
durante uma sessão autônoma de 60 minutos (12 ciclos) e dispara (`workflow_dispatch`) o repositório
privado `nigosky/coh3-br-tracker` imediatamente assim que qualquer partida nova termina!

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

RELIC_HOST = "coh3-api.reliclink.com"
GITHUB_API_HOST = "api.github.com"
RADAR_HOST = "raw.githubusercontent.com"
RADAR_STATE_PATH = "/nigosky/coh3-br-lobby-radar/main/state.json"
RADAR_STATE_URL = f"https://{RADAR_HOST}{RADAR_STATE_PATH}"
BASE_URL = f"https://{RELIC_HOST}/community/leaderboard"
MAX_HTTP_BYTES = 5 * 1024 * 1024  # 5 MB limite estrito anti-Gzip-bomb
MAX_RADAR_BYTES = 256 * 1024  # 256 KB limite estrito para state.json do Radar dos Veteranos
RADAR_MAX_AGE_SEC = 7200  # Ignora estado do Radar se estiver há >2h sem sincronizar
BRT_TZ = timezone(timedelta(hours=-3))
HEARTBEAT_INTERVAL_SEC = 3 * 3600  # Garante 1 disparo de segurança a cada 3h (limpeza 72h, Open Data, retries)
AUTOMATCH_TYPE_IDS: frozenset[int] = frozenset({1, 2, 3, 4})  # 1v1, 2v2, 3v3, 4v4 Automatch (exclui Custom=0)
BG_FOLLOWUP_FRESH_MATCH_SEC = 300  # Partida Automatch encerrada há <5 min pode ainda estar sendo parseada no cohdb.com
BG_FOLLOWUP_DELAY_SEC = 240  # Agenda 1 disparo único de confirmação no próximo ciclo de 5 min (+4-5 min)
BG_FOLLOWUP_TTL_SEC = 2700  # Descarta agendamento se ficar obsoleto (>45 min)

URL_FORBIDDEN_CHARS_RE = re.compile(r"[\x00-\x20\x7f]")
SAFE_GH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
SAFE_GH_REF_RE = re.compile(r"^[A-Za-z0-9_./-]{1,128}$")

# As 45 contas vinculadas (44 jogadores únicos) monitoradas no CoH3 Brasil Tracker
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

MONITORED_PID_SET: frozenset[int] = frozenset(pid for pid, _ in MONITORED_PROFILES)

_TLS_LOCALS = threading.local()


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
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx), _NoRedirectHandler())
        _TLS_LOCALS.opener = opener
    return opener


def _validate_outbound_https(url: str, allowed_host: str, path_prefix: str = "/") -> urllib.parse.ParseResult:
    """Valida esquema HTTPS, host exato, porta 443, ausência de credenciais/controles e prefixo de caminho."""
    clean = (url or "").strip()
    if not clean or URL_FORBIDDEN_CHARS_RE.search(clean):
        raise ValueError("URL inválida ou com caracteres de controle proibidos.")
    parsed = urllib.parse.urlparse(clean)
    if parsed.scheme != "https" or (parsed.hostname or "").lower() != allowed_host.lower():
        raise ValueError(f"Host fora da allowlist ({allowed_host}).")
    if parsed.username or parsed.password:
        raise ValueError("Credenciais embutidas na URL são bloqueadas.")
    if parsed.port not in (None, 443):
        raise ValueError("Porta fora do padrão HTTPS (443) bloqueada.")
    if path_prefix and not (parsed.path or "").startswith(path_prefix):
        raise ValueError("Caminho de URL fora do escopo autorizado.")
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


def fetch_veteranos_radar_active_pids(max_age_sec: int = RADAR_MAX_AGE_SEC) -> tuple[set[int], int, bool]:
    """
    Lê (estritamente somente-leitura) o `state.json` público do Radar dos Veteranos
    (`nigosky/coh3-br-lobby-radar`) e retorna `(active_pids, last_sync_ts, ok)`.
    """
    parsed = _validate_outbound_https(RADAR_STATE_URL, RADAR_HOST, path_prefix=RADAR_STATE_PATH)
    if parsed.path != RADAR_STATE_PATH:
        raise ValueError("Caminho do Radar fora da allowlist.")

    headers = {
        "User-Agent": "CoH3-BR-SmartScheduler/2.1",
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

            return active_pids & MONITORED_PID_SET, last_sync_ts, True
        except Exception:
            if attempt == 0:
                time.sleep(0.6)
    return set(), 0, False


def fetch_personal_stat_last_match(pids: list[int]) -> dict[int, int]:
    """
    Consulta `getPersonalStat` da Relic (lotes de até 10 contas) apenas para o subconjunto
    sinalizado pelo Radar dos Veteranos, retornando `{profile_id: max_lastmatchdate}`.
    """
    clean_pids = sorted({int(p) for p in pids if int(p) > 0})
    if not clean_pids:
        return {}

    headers = {
        "User-Agent": "CoH3-BR-SmartScheduler/2.1",
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
        "User-Agent": "CoH3-BR-SmartScheduler/2.2",
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
    _validate_outbound_https(url, GITHUB_API_HOST, path_prefix="/repos/")

    body = json.dumps({"ref": clean_ref}).encode("utf-8")
    headers = {
        "User-Agent": "CoH3-BR-SmartScheduler/2.1",
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
) -> tuple[bool, bool, str, dict[str, Any]]:
    """
    Executa 1 ciclo de pré-checagem (Relic History + Radar dos Veteranos + Relic PersonalStat).
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
    prev_radar_pids = (
        set(prev_radar_pids_override)
        if prev_radar_pids_override is not None
        else _extract_pids_from_seq(prev.get("radar_coh3_pids"))
    )

    alias_by_pid = {pid: alias for pid, alias in MONITORED_PROFILES}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(fetch_latest_player_marker, [pid for pid, _ in MONITORED_PROFILES]))

    # Checagem cruzada somente-leitura com o Radar dos Veteranos (nigosky/coh3-br-lobby-radar)
    curr_radar_pids, radar_sync_ts, radar_ok = fetch_veteranos_radar_active_pids()
    exited_radar_pids = (prev_radar_pids - curr_radar_pids) if radar_ok else set()
    focus_radar_pids = (curr_radar_pids | exited_radar_pids) if radar_ok else set()

    results_by_pid: dict[int, tuple[int, int, int, bool, int, int]] = {
        pid: (pid, mid, ct, ok, a_mid, a_ct) for pid, mid, ct, ok, a_mid, a_ct in results
    }

    # Rechecagem prioritária imediata se um Veterano ativo/saindo do Radar falhou na 1ª passada da Relic
    radar_failed_aliases: list[str] = []
    for fpid in sorted(focus_radar_pids):
        _, _, _, ok_first, _, _ = results_by_pid.get(fpid, (fpid, 0, 0, False, 0, 0))
        if not ok_first:
            retry_res = fetch_latest_player_marker(fpid)
            results_by_pid[fpid] = retry_res
            if not retry_res[3]:
                radar_failed_aliases.append(alias_by_pid.get(fpid, str(fpid)))

    ok_count = 0
    new_activity_players: list[str] = []
    new_activity_pids: set[int] = set()
    fresh_automatch_mid = 0
    updated_players: dict[str, dict[str, int]] = {k: dict(v) for k, v in prev_players.items()}

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

    # Cross-check Relic getPersonalStat (1 única requisição leve!) para Veteranos sinalizados pelo Radar
    radar_stat_triggers: list[str] = []
    if focus_radar_pids:
        stat_lmd_map = fetch_personal_stat_last_match(sorted(focus_radar_pids))
        for fpid in sorted(focus_radar_pids):
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

            if prev_players and fpid not in new_activity_pids and stat_lmd > (baseline_ts + 10):
                radar_stat_triggers.append(alias_by_pid.get(fpid, str(fpid)))

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
    if radar_failed_aliases:
        sample_f = ", ".join(radar_failed_aliases[:4])
        reasons.append(f"Radar Veteranos: fallback p/ rechecagem Relic ({sample_f})")
    bg_followup_due = (
        prev_bg_due_ts > 0
        and now_ts >= prev_bg_due_ts
        and (now_ts - prev_bg_due_ts) <= BG_FOLLOWUP_TTL_SEC
    )
    if bg_followup_due and not new_activity_players and not radar_stat_triggers:
        mid_hint = f" #{prev_bg_match_id}" if prev_bg_match_id > 0 else ""
        reasons.append(f"confirmacao de Battlegroups cohdb (+5 min apos partida{mid_hint})")
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
        else f"sem novas partidas ({ok_count}/{len(MONITORED_PROFILES)} contas verificadas)"
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
        if fresh_automatch_mid > 0 or radar_stat_triggers:
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

    for idx in range(1, cycles + 1):
        cycle_start = time.monotonic()
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
        )
        if meta.get("radar_ok"):
            mem_radar_pids = set(meta.get("radar_active_pids") or [])
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
