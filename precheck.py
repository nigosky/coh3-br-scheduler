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
import concurrent.futures
import gzip
import io
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
GITHUB_API_HOST = "api.github.com"
BASE_URL = f"https://{RELIC_HOST}/community/leaderboard"
MAX_HTTP_BYTES = 5 * 1024 * 1024  # 5 MB limite estrito anti-Gzip-bomb
BRT_TZ = timezone(timedelta(hours=-3))
HEARTBEAT_INTERVAL_SEC = 3 * 3600  # Garante 1 disparo de segurança a cada 3h (limpeza 72h, Open Data, retries)

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
        "User-Agent": "CoH3-BR-SmartScheduler/2.0",
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

    url = f"https://{GITHUB_API_HOST}/repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches"
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != GITHUB_API_HOST:
        raise ValueError("Host fora da allowlist da GitHub API.")

    body = json.dumps({"ref": ref}).encode("utf-8")
    headers = {
        "User-Agent": "CoH3-BR-SmartScheduler/2.0",
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
) -> tuple[bool, bool, str, dict[str, Any]]:
    """
    Executa 1 ciclo de pré-checagem.
    Retorna `(should_dispatch, dispatched_ok, reason_str, meta)`.
    Importante: só avança `state.json` se `dispatched_ok` for True (evita perder partidas caso o dispatch falhe).
    """
    now_ts = int(time.time())
    now_brt = datetime.fromtimestamp(now_ts, tz=BRT_TZ)
    today_brt = now_brt.strftime("%Y-%m-%d")
    iso_year, iso_week, _ = now_brt.isocalendar()
    current_iso_week = f"{iso_year}-W{iso_week:02d}"

    prev = load_state(state_path)
    prev_players: dict[str, dict[str, int]] = dict(prev.get("players") or {})
    last_dispatch_ts = int(prev.get("last_dispatch_ts", 0) or 0)
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
    mark_weekly_done = False

    if force_dispatch:
        reasons.append("disparo manual forcado (workflow_dispatch)")
    if not prev_players:
        reasons.append("inicializacao do baseline de estado (1a execucao)")
    if new_activity_players:
        sample = ", ".join(new_activity_players[:4])
        extra = f" +{len(new_activity_players) - 4}" if len(new_activity_players) > 4 else ""
        reasons.append(f"nova partida detectada: {sample}{extra}")
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
        next_state = {
            "schema_version": 1,
            "last_checked_ts": now_ts,
            "last_dispatch_ts": now_ts,
            "last_weekly_summary_iso_week": current_iso_week if mark_weekly_done else last_weekly_week,
            "players": updated_players,
        }
        atomic_write_json(state_path, next_state)

    meta = {
        "ok_accounts": ok_count,
        "total_accounts": len(MONITORED_PROFILES),
        "http_code": http_code,
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
        )
        last_ok_str = f"{meta['ok_accounts']}/{meta['total_accounts']}"

        if should_dispatch:
            if dispatched_ok:
                total_dispatches += 1
                dispatch_events.append(f"[{now_brt_str}] {reason}")
                print(
                    f"[CICLO {idx}/{cycles} • {now_brt_str}] DISPARADO (HTTP {meta['http_code']}) -> "
                    f"{target_owner}/{target_repo}@{target_ref} | Contas OK={last_ok_str} | Motivo: {reason}",
                    flush=True,
                )
            else:
                print(
                    f"::warning::[CICLO {idx}/{cycles} • {now_brt_str}] Falha ao disparar {target_owner}/{target_repo} "
                    f"(HTTP {meta['http_code']}); estado preservado para retentativa no proximo ciclo! Motivo: {reason}",
                    flush=True,
                )
        else:
            total_skips += 1
            print(
                f"[CICLO {idx}/{cycles} • {now_brt_str}] ECONOMIZADO (SKIP) | Contas OK={last_ok_str} | {reason}",
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
