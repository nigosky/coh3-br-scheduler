"""Testes unitários do consumo da base compartilhada, Radar Steam fast_sig e segurança no Scheduler."""

from __future__ import annotations

import os
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

import precheck


class TestSchedulerSharedRoster(unittest.TestCase):
    def tearDown(self) -> None:
        precheck.MONITORED_PROFILES = precheck.FALLBACK_MONITORED_PROFILES
        precheck.MONITORED_PID_SET = frozenset(pid for pid, _ in precheck.FALLBACK_MONITORED_PROFILES)
        precheck.STEAM_ID_TO_PID = dict(precheck.FALLBACK_STEAM_ID_TO_PID)
        precheck._LAST_FETCHED_STEAM_ID_TO_PID = {}

    def test_fallback_profiles_and_steam_map_stay_aligned(self) -> None:
        self.assertEqual(len(precheck.FALLBACK_MONITORED_PROFILES), 48)
        self.assertEqual(len(precheck.FALLBACK_STEAM_ID_TO_PID), 48)
        self.assertEqual(precheck.LOCAL_MERGED_ACCOUNT_FALLBACKS, ())
        self.assertEqual(
            set(precheck.FALLBACK_STEAM_ID_TO_PID.values()),
            {pid for pid, _ in precheck.FALLBACK_MONITORED_PROFILES},
        )
        by_pid = dict(precheck.FALLBACK_MONITORED_PROFILES)
        self.assertEqual(by_pid.get(1119784), "Tracker")
        self.assertEqual(precheck.FALLBACK_STEAM_ID_TO_PID.get("76561198000067932"), 1119784)
        expected_smurfs = {
            1333418: ("76561198698969876", "Charles Bronson (Smurf: Chacineiro)"),
            557890: ("76561199538846112", "gabe44arg (Smurf: Gabe)"),
            1423426: ("76561199482388323", "geladeira velha (Smurf: arc)"),
            1487738: ("76561198602236177", "Državljan H (Smurf: arc)"),
            1482961: ("76561199886227102", "Vog (Smurf: arc)"),
            917917: ("76561198287157768", "arc (Smurf: arc)"),
            1160699: ("76561199814547622", "Bubebolos (Smurf: arc)"),
            918241: ("76561199794093952", "Faint (Smurf: arc)"),
            1390350: ("76561198089278659", "TearsInTheRain (Smurf: Chacineiro)"),
        }
        for spid, (expected_sid, expected_alias) in expected_smurfs.items():
            self.assertEqual(precheck.FALLBACK_STEAM_ID_TO_PID.get(expected_sid), spid)
            self.assertEqual(by_pid.get(spid), expected_alias)
        for moved_pid in (19747, 23250, 28937, 81751, 3895, 462597, 1297821, 1318162, 509690, 1297853, 844225, 1297804, 164152, 1076221):
            self.assertNotIn(moved_pid, precheck.MONITORED_PID_SET)

    def test_parse_shared_roster_profiles_reads_veteranos_only_and_tags_secondary(self) -> None:
        payload = {
            "schema_version": 1,
            "veteranos": [
                {"profile_id": 79327, "steam_id": "76561197963172584", "original_alias": "Chacineiro", "country": "br"},
                {
                    "profile_id": 557890,
                    "steam_id": "76561199538846112",
                    "original_alias": "É o Gabe",
                    "merged_into_profile_id": 559962,
                },
                {"profile_id": 975688, "steam_id": "76561197994245218", "original_alias": "Kefbr", "in_ranking": False},
            ],
            "monitorados": [
                {"profile_id": 33487, "steam_id": "76561197995712470", "original_alias": "Generick", "country": "us"},
                {"profile_id": 164152, "steam_id": "76561198029749621", "original_alias": "Sgt.Yoda", "country": "br"},
            ],
        }
        parsed, sid_map = precheck.parse_shared_roster_details(payload)
        self.assertEqual(parsed, ((79327, "Chacineiro"), (557890, "É o Gabe (sec)"), (975688, "Kefbr")))
        self.assertEqual(
            sid_map,
            {
                "76561197963172584": 79327,
                "76561199538846112": 557890,
                "76561197994245218": 975688,
            },
        )
        self.assertNotIn("76561197995712470", sid_map)
        self.assertEqual(precheck.parse_shared_roster_profiles(payload), parsed)

    def test_parse_rejects_empty_or_invalid_schema(self) -> None:
        for bad in ({}, {"schema_version": 1, "veteranos": []}, {"schema_version": 0, "veteranos": [{"profile_id": 1}]}):
            with self.assertRaises(ValueError):
                precheck.parse_shared_roster_profiles(bad)  # type: ignore[arg-type]

    def test_fetch_shared_monitored_profiles_blocks_untrusted_urls_and_queries(self) -> None:
        for bad_url in (
            "http://raw.githubusercontent.com/nigosky/coh3-br-lobby-radar/main/players_roster.json",
            "https://evil.com/nigosky/coh3-br-lobby-radar/main/players_roster.json",
            "https://raw.githubusercontent.com/attacker/repo/main/players_roster.json",
            "https://raw.githubusercontent.com/nigosky/coh3-br-lobby-radar/main/players_roster.json?key=leak",
            "https://raw.githubusercontent.com/nigosky/coh3-br-lobby-radar/main/players_roster.json#frag",
        ):
            with self.assertRaises(ValueError):
                precheck.fetch_shared_monitored_profiles(url=bad_url)

    def test_refresh_monitored_profiles_updates_globals_and_keeps_fallback_on_failure(self) -> None:
        with patch.object(precheck, "fetch_shared_monitored_profiles", return_value=((101, "Alpha"), (202, "Beta"))):
            self.assertTrue(precheck.refresh_monitored_profiles())
            self.assertEqual(precheck.MONITORED_PROFILES, ((101, "Alpha"), (202, "Beta")))
            self.assertEqual(precheck.MONITORED_PID_SET, frozenset({101, 202}))

        # Sem LOCAL_MERGED_ACCOUNT_FALLBACKS padrão, reflete fielmente o roster remoto sem injetar contas legadas
        with patch.object(precheck, "fetch_shared_monitored_profiles", return_value=((79327, "Chacineiro"), (202, "Beta"))):
            self.assertTrue(precheck.refresh_monitored_profiles())
            self.assertEqual(precheck.MONITORED_PROFILES, ((79327, "Chacineiro"), (202, "Beta")))
            self.assertEqual(precheck.MONITORED_PID_SET, frozenset({79327, 202}))

        # Mecanismo genérico LOCAL_MERGED_ACCOUNT_FALLBACKS continua preservando fallback quando explicitamente configurado
        with (
            patch.object(
                precheck,
                "LOCAL_MERGED_ACCOUNT_FALLBACKS",
                ((1333418, "76561198698969876", "Charles Bronson (Chacineiro)", 79327),),
            ),
            patch.object(precheck, "fetch_shared_monitored_profiles", return_value=((79327, "Chacineiro"), (202, "Beta"))),
        ):
            self.assertTrue(precheck.refresh_monitored_profiles())
            self.assertEqual(
                precheck.MONITORED_PROFILES,
                ((79327, "Chacineiro"), (1333418, "Charles Bronson (Chacineiro)"), (202, "Beta")),
            )
            self.assertEqual(precheck.STEAM_ID_TO_PID.get("76561198698969876"), 1333418)

        with patch.object(precheck, "fetch_shared_monitored_profiles", return_value=None):
            self.assertFalse(precheck.refresh_monitored_profiles())
            # Mantém o estado anterior válido sem zerar
            self.assertEqual(precheck.MONITORED_PID_SET, frozenset({79327, 1333418, 202}))

    def test_parse_radar_state_reads_last_steam_fast_sig_coh3_only_and_ignores_other_games(self) -> None:
        state_payload = {
            "last_sync_ts": 1_800_000_000,
            "last_alert_coh3_pids": [79327],
            "last_digest": repr(
                (
                    (
                        [(639157, False, False, False, False, "Itaperuna", "")],
                        [(26910, "Marvel Rivals", False, False, False, "nigo_BR", "")],
                        [(110078, "Bineto", "")],
                    ),
                    ([], [], []),
                )
            ),
            # fast[0] = Veteranos no CoH3 (inclui vonMises, arc sec e Chacineiro smurf via Steam Web API),
            # fast[1] = Online/outros jogos (nigo_BR em Marvel Rivals — NÃO deve ser incluído!)
            "last_steam_fast_sig": repr(
                (
                    [
                        ("76561197985268629", False, False, False, "vonMises"),
                        ("76561199482388323", False, False, False, "arc"),
                        ("76561198698969876", False, False, False, "Charles Bronson"),
                        ("76561199999999999", False, False, False, "DesconhecidoNaoMonitorado"),
                    ],
                    [
                        ("76561197963741792", "Marvel Rivals", False, False, False, "nigo_BR"),
                        ("76561198034510845", "Age of Empires IV", False, False, False, "Jimmy Duelista"),
                    ],
                )
            ),
        }
        active = precheck._parse_radar_state_active_pids(state_payload)
        self.assertEqual(active, {79327, 639157, 110078, 31145, 1423426, 1333418})
        self.assertNotIn(26910, active)
        self.assertNotIn(735940, active)

    def test_opener_blocks_non_https_schemes_and_atomic_write_blocks_symlinks(self) -> None:
        opener = precheck._get_opener()
        for bad_url in (
            "file:///etc/passwd",
            "http://169.254.169.254/latest/meta-data/",
            "ftp://example.com/file",
            "data:text/plain;base64,SGVsbG8=",
        ):
            with self.assertRaises(urllib.error.URLError):
                opener.open(bad_url, timeout=2)

        with tempfile.TemporaryDirectory() as td:
            real_file = Path(td) / "real.json"
            real_file.write_text('{"ok": true}\n', encoding="utf-8")
            link_path = Path(td) / "state.json"
            os.symlink(real_file, link_path)
            self.assertEqual(precheck.load_state(link_path), {})
            with self.assertRaises(RuntimeError):
                precheck.atomic_write_json(link_path, {"hacked": True})
            self.assertEqual(real_file.read_text(encoding="utf-8"), '{"ok": true}\n')

    def test_validate_outbound_https_and_gzip_bomb_protection(self) -> None:
        import gzip
        import io

        for bad in (
            "https://coh3-api.reliclink.com:abc/community/leaderboard/getPersonalStat",
            "https://coh3-api.reliclink.com/community/leaderboard/../admin",
            "https://coh3-api.reliclink.com/community/leaderboard/%2e%2e/admin",
            "https://coh3-api.reliclink.com/community/leaderboard/x#frag",
            "https://coh3-api.reliclink.com/community/leaderboard/x;param=1",
        ):
            with self.assertRaises(ValueError, msg=f"URL insegura não bloqueada: {bad!r}"):
                precheck._validate_outbound_https(bad, precheck.RELIC_HOST, path_prefix="/community/leaderboard/")

        class _FakeResp:
            def __init__(self, data: bytes, hdrs: dict[str, str]) -> None:
                self._buf = io.BytesIO(data)
                self.headers = hdrs

            def read(self, n: int = -1) -> bytes:
                return self._buf.read(n)

        bomb = gzip.compress(b'{"a":"' + (b"X" * 300_000) + b'"}')
        with self.assertRaises(ValueError):
            precheck._read_bounded_json(_FakeResp(bomb, {"Content-Encoding": "gzip"}), max_bytes=32_768)

        ok_payload = b'{"ok": true}'
        parsed = precheck._read_bounded_json(
            _FakeResp(gzip.compress(ok_payload), {"Content-Encoding": "gzip"}),
            max_bytes=4096,
        )
        self.assertEqual(parsed, {"ok": True})

    def test_quiet_period_hourly_personalstat_audit_catches_missed_private_player(self) -> None:
        import json

        now_ts = 1_795_536_000  # 2026-11-24 16:00:00 UTC (terça-feira)
        precheck.MONITORED_PROFILES = ((79327, "Chacineiro"), (3895, "Liquid"))
        precheck.MONITORED_PID_SET = frozenset({79327, 3895})

        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "state.json"
            # Último disparo há 1h10m (>1h, <3h heartbeat) e nenhuma partida nova na 1ª passada
            state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "last_dispatch_ts": now_ts - 4200,
                        "last_full_stat_audit_ts": now_ts - 4200,
                        "players": {
                            "79327": {"max_match_id": 100, "max_completion_time": now_ts - 7200},
                            "3895": {"max_match_id": 200, "max_completion_time": now_ts - 7200},
                        },
                    }
                ),
                encoding="utf-8",
            )

            def _fake_marker(pid: int) -> tuple[int, int, int, bool, int, int]:
                mid = 100 if pid == 79327 else 200
                return (pid, mid, now_ts - 7200, True, mid, now_ts - 7200)

            # Caso 1: apenas +1s de drift em 3895 -> auditoria roda, mas NÃO dispara falso positivo
            with (
                patch.object(precheck.time, "time", return_value=now_ts),
                patch.object(precheck, "fetch_latest_player_marker", side_effect=_fake_marker),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), now_ts, True)),
                patch.object(
                    precheck,
                    "fetch_personal_stat_last_match",
                    return_value={79327: now_ts - 7200, 3895: now_ts - 7199},
                ) as mock_ps,
            ):
                should_disp, _ok, reason, meta = precheck.evaluate_precheck(state_path=state_path, dry_run=True)
                self.assertFalse(should_disp)
                self.assertTrue(meta["full_stat_audit_ran"])
                self.assertIn("auditoria silencio 1h OK", reason)
                mock_ps.assert_called_once_with([3895, 79327])

            # Caso 2: dentro do cooldown de 1h em memória -> não repete getPersonalStat completo
            with (
                patch.object(precheck.time, "time", return_value=now_ts + 300),
                patch.object(precheck, "fetch_latest_player_marker", side_effect=_fake_marker),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), now_ts + 300, True)),
                patch.object(precheck, "fetch_personal_stat_last_match") as mock_ps_skip,
            ):
                should_disp2, _ok2, _reason2, meta2 = precheck.evaluate_precheck(
                    state_path=state_path,
                    dry_run=True,
                    last_full_stat_audit_ts_override=now_ts,
                )
                self.assertFalse(should_disp2)
                self.assertFalse(meta2["full_stat_audit_ran"])
                mock_ps_skip.assert_not_called()

            # Caso 3: jogador com Steam privado (fora do Radar) teve partida real (+1h à frente) -> dispara imediatamente!
            with (
                patch.object(precheck.time, "time", return_value=now_ts),
                patch.object(precheck, "fetch_latest_player_marker", side_effect=_fake_marker),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), now_ts, True)),
                patch.object(
                    precheck,
                    "fetch_personal_stat_last_match",
                    return_value={79327: now_ts - 600, 3895: now_ts - 7199},
                ),
            ):
                should_disp3, ok3, reason3, meta3 = precheck.evaluate_precheck(state_path=state_path, dry_run=True)
                self.assertTrue(should_disp3)
                self.assertTrue(ok3)
                self.assertTrue(meta3["full_stat_audit_ran"])
                self.assertIn("Auditoria de silencio (1h)", reason3)
                self.assertIn("Chacineiro", reason3)
                self.assertNotIn("Liquid", reason3)

    def test_redact_secrets_and_bounded_dispatch_response(self) -> None:
        fake_gh = "ghp_" + ("Z" * 36)
        fake_pat = "github_pat_" + ("Y" * 32)
        fake_wh = "https://discord.com" + "/api/" + "webhooks/" + "123456789012345678/" + ("A" * 40)
        fake_steam_key = "ABCDEF12" + "34567890"
        leaked = (
            f"Authorization: Bearer {fake_gh} TRACKER_DISPATCH_TOKEN=tok_secret_123 "
            f"pat={fake_pat} wh={fake_wh} ?key={fake_steam_key}"
        )
        scrubbed = precheck.redact_secrets(leaked)
        for secret in (fake_gh, fake_pat, "tok_secret_123", "123456789012345678", fake_steam_key):
            self.assertNotIn(secret, scrubbed)
        self.assertIn("[AUTH_REDACTED]", scrubbed)
        self.assertIn("[SECRET_REDACTED]", scrubbed)
        self.assertIn("[GH_TOKEN_REDACTED]", scrubbed)
        self.assertIn("[WEBHOOK_REDACTED]", scrubbed)
        self.assertIn("[STEAM_KEY_REDACTED]", scrubbed)

        read_sizes: list[int] = []

        class _FakeDispatchResp:
            def __enter__(self) -> _FakeDispatchResp:
                return self

            def __exit__(self, *args: object) -> bool:
                return False

            def getcode(self) -> int:
                return 204

            def read(self, n: int = -1) -> bytes:
                read_sizes.append(n)
                return b""

        class _FakeOpener:
            def open(self, req: object, timeout: int = 20) -> _FakeDispatchResp:
                return _FakeDispatchResp()

        with patch.object(precheck, "_get_opener", return_value=_FakeOpener()):
            ok, code = precheck.dispatch_private_tracker("tok_test_valid")
        self.assertTrue(ok)
        self.assertEqual(code, 204)
        self.assertEqual(read_sizes, [1024])

    def test_custom_match_triggers_two_wave_battlegroup_followup(self) -> None:
        import json

        self.assertIn(0, precheck.BG_ELIGIBLE_TYPE_IDS)
        self.assertNotIn(5, precheck.BG_ELIGIBLE_TYPE_IDS)

        now_ts = 1_795_540_000
        precheck.MONITORED_PROFILES = ((79327, "Chacineiro"),)
        precheck.MONITORED_PID_SET = frozenset({79327})

        # 1) fetch_latest_player_marker deve capturar Custom (mt=0) e ignorar Co-Op (mt=5)
        fake_history = {
            "matchHistoryStats": [
                {"id": 92719373, "completiontime": now_ts - 60, "matchtype_id": 0},
                {"id": 92719999, "completiontime": now_ts - 30, "matchtype_id": 5},
            ]
        }

        class _FakeResp:
            def __enter__(self) -> _FakeResp:
                return self

            def __exit__(self, *args: object) -> bool:
                return False

        class _FakeOpener:
            def open(self, req: object, timeout: int = 15) -> _FakeResp:
                return _FakeResp()

        with (
            patch.object(precheck, "_get_opener", return_value=_FakeOpener()),
            patch.object(precheck, "_read_bounded_json", return_value=fake_history),
        ):
            pid, max_mid, max_ct, ok_m, bg_mid, bg_ct = precheck.fetch_latest_player_marker(79327)
        self.assertTrue(ok_m)
        self.assertEqual(pid, 79327)
        self.assertEqual(max_mid, 92719999)
        self.assertEqual(bg_mid, 92719373)
        self.assertEqual(bg_ct, now_ts - 60)

        # 2) Ciclo completo 2 ondas: T=0 (nova Custom) -> Onda 1 (+5m) -> Onda 2 (+30m) -> encerrado
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "last_dispatch_ts": now_ts - 1800,
                        "players": {"79327": {"max_match_id": 92700000, "max_completion_time": now_ts - 3600}},
                    }
                ),
                encoding="utf-8",
            )

            # T=0: Nova Custom #92719373 -> agenda Onda 1
            with (
                patch.object(precheck.time, "time", return_value=now_ts),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 92719373, now_ts - 60, True, 92719373, now_ts - 60),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), now_ts, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)),
            ):
                disp0, ok0, _reason0, _meta0 = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_valid_test",
                )
            self.assertTrue(disp0 and ok0)
            st0 = precheck.load_state(state_path)
            self.assertEqual(st0["pending_bg_followup_match_id"], 92719373)
            self.assertEqual(st0["pending_bg_followup_wave"], 1)
            self.assertEqual(st0["pending_bg_followup_due_ts"], now_ts + precheck.BG_FOLLOWUP_DELAY_SEC)

            # T=+5m: Onda 1 dispara e agenda Onda 2 (+25m)
            t_wave1 = now_ts + 300
            with (
                patch.object(precheck.time, "time", return_value=t_wave1),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 92719373, now_ts - 60, True, 92719373, now_ts - 60),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_wave1, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)),
            ):
                disp1, ok1, reason1, _meta1 = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_valid_test",
                )
            self.assertTrue(disp1 and ok1)
            self.assertIn("onda 1/2", reason1)
            st1 = precheck.load_state(state_path)
            self.assertEqual(st1["pending_bg_followup_wave"], 2)
            self.assertEqual(st1["pending_bg_followup_due_ts"], t_wave1 + precheck.BG_FOLLOWUP_WAVE2_DELAY_SEC)

            # T=+30m: Onda 2 dispara e limpa agendamento
            t_wave2 = t_wave1 + precheck.BG_FOLLOWUP_WAVE2_DELAY_SEC + 30
            with (
                patch.object(precheck.time, "time", return_value=t_wave2),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 92719373, now_ts - 60, True, 92719373, now_ts - 60),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_wave2, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)),
            ):
                disp2, ok2, reason2, _meta2 = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_valid_test",
                )
            self.assertTrue(disp2 and ok2)
            self.assertIn("onda 2/2", reason2)
            st2 = precheck.load_state(state_path)
            self.assertEqual(st2["pending_bg_followup_wave"], 0)
            self.assertEqual(st2["pending_bg_followup_due_ts"], 0)

    def test_dispatch_private_tracker_serializes_optional_inputs_and_stops_on_4xx(self) -> None:
        import json

        captured_bodies: list[dict[str, object]] = []
        captured_urls: list[str] = []

        class _FakeResp:
            def __enter__(self) -> _FakeResp:
                return self

            def __exit__(self, *args: object) -> bool:
                return False

            def getcode(self) -> int:
                return 204

            def read(self, n: int = -1) -> bytes:
                return b""

        class _FakeOpener:
            def open(self, req: object, timeout: int = 20) -> _FakeResp:
                captured_urls.append(getattr(req, "full_url", ""))
                captured_bodies.append(json.loads(getattr(req, "data", b"{}").decode("utf-8")))
                return _FakeResp()

        with patch.object(precheck, "_get_opener", return_value=_FakeOpener()):
            ok1, code1 = precheck.dispatch_private_tracker(
                token="tok_valid",
                owner="nigosky",
                repo="coh3-br-ranking",
                workflow="ranking_unificado.yml",
                ref="main",
                inputs={"force_update": False},
            )
            ok2, code2 = precheck.dispatch_private_tracker(
                token="tok_valid",
                owner="nigosky",
                repo="coh3-br-ranking",
                workflow="boletim_diario.yml",
                ref="main",
            )

        self.assertTrue(ok1 and code1 == 204)
        self.assertTrue(ok2 and code2 == 204)
        self.assertEqual(
            captured_urls,
            [
                "https://api.github.com/repos/nigosky/coh3-br-ranking/actions/workflows/ranking_unificado.yml/dispatches",
                "https://api.github.com/repos/nigosky/coh3-br-ranking/actions/workflows/boletim_diario.yml/dispatches",
            ],
        )
        self.assertEqual(captured_bodies[0], {"ref": "main", "inputs": {"force_update": False}})
        self.assertEqual(captured_bodies[1], {"ref": "main"})

        # HTTP 403 nao deve repetir 3x com backoff desnecessario
        attempts = 0

        class _Fake403Opener:
            def open(self, req: object, timeout: int = 20) -> object:
                nonlocal attempts
                attempts += 1
                raise urllib.error.HTTPError(getattr(req, "full_url", ""), 403, "Forbidden", {}, None)

        with (
            patch.object(precheck, "_get_opener", return_value=_Fake403Opener()),
            patch.object(precheck.time, "sleep") as mock_sleep,
        ):
            ok_err, code_err = precheck.dispatch_private_tracker(
                token="tok_valid",
                owner="nigosky",
                repo="coh3-br-ranking",
                workflow="boletim_diario.yml",
            )
        self.assertFalse(ok_err)
        self.assertEqual(code_err, 403)
        self.assertEqual(attempts, 1)
        mock_sleep.assert_not_called()

    def test_ranking_daily_boletim_schedule_and_persistence_without_tracker_dispatch(self) -> None:
        import json
        from datetime import datetime

        def _brt_ts(y: int, m: int, d: int, hh: int, mm: int) -> int:
            return int(datetime(y, m, d, hh, mm, 0, tzinfo=precheck.BRT_TZ).timestamp())

        precheck.MONITORED_PROFILES = ((79327, "Chacineiro"),)
        precheck.MONITORED_PID_SET = frozenset({79327})

        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "state.json"
            t_0802 = _brt_ts(2026, 11, 24, 8, 2)
            base_state = {
                "schema_version": 2,
                "last_checked_ts": t_0802 - 300,
                "last_dispatch_ts": t_0802 - 600,
                "last_full_stat_audit_ts": t_0802 - 600,
                "last_weekly_summary_iso_week": "2026-W48",
                "last_ranking_slot": "2026-11-24-00",
                "pending_bg_followup_due_ts": 0,
                "pending_bg_followup_match_id": 0,
                "pending_bg_followup_wave": 0,
                "radar_coh3_pids": [],
                "radar_last_sync_ts": t_0802 - 300,
                "players": {"79327": {"max_match_id": 500, "max_completion_time": t_0802 - 3600}},
            }
            state_path.write_text(json.dumps(base_state), encoding="utf-8")

            # 08:02 BRT -> antes da janela (08:03): nao dispara nem grava state.json
            with (
                patch.object(precheck.time, "time", return_value=t_0802),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 500, t_0802 - 3600, True, 500, t_0802 - 3600),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_0802, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_disp,
            ):
                should_disp, _, _, meta = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_tracker",
                )
            self.assertFalse(should_disp)
            self.assertFalse(meta["state_persisted"])
            self.assertEqual(meta["ranking_dispatches"], [])
            mock_disp.assert_not_called()

            # 08:03 BRT (terca) -> tracker fica em SKIP, mas boletim_diario.yml dispara e persiste state.json!
            t_0803 = _brt_ts(2026, 11, 24, 8, 3)
            with (
                patch.object(precheck.time, "time", return_value=t_0803),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 500, t_0802 - 3600, True, 500, t_0802 - 3600),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_0803, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_disp,
            ):
                should_disp2, _, _, meta2 = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_tracker",
                )
            self.assertFalse(should_disp2)
            self.assertTrue(meta2["state_persisted"])
            self.assertEqual(meta2["ranking_dispatches_ok"], 1)
            mock_disp.assert_called_once_with(
                token="tok_tracker",
                owner="nigosky",
                repo="coh3-br-ranking",
                workflow="boletim_diario.yml",
                ref="main",
                inputs=None,
            )
            saved = precheck.load_state(state_path)
            self.assertEqual(saved["last_ranking_daily_date"], "2026-11-24")
            self.assertEqual(saved["last_ranking_slot"], "2026-11-24-00")
            # Estado do tracker deve permanecer intacto (nao sobrescreveu last_dispatch_ts nem players)
            self.assertEqual(saved["last_dispatch_ts"], t_0802 - 600)
            self.assertEqual(saved["players"]["79327"]["max_match_id"], 500)
            self.assertEqual(saved["last_weekly_summary_iso_week"], "2026-W48")

            # 08:08 BRT (mesmo dia) -> idempotente, nao dispara novamente
            t_0808 = _brt_ts(2026, 11, 24, 8, 8)
            with (
                patch.object(precheck.time, "time", return_value=t_0808),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 500, t_0802 - 3600, True, 500, t_0802 - 3600),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_0808, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_disp_skip,
            ):
                _, _, _, meta3 = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_tracker",
                )
            self.assertEqual(meta3["ranking_dispatches"], [])
            self.assertFalse(meta3["state_persisted"])
            mock_disp_skip.assert_not_called()

    def test_ranking_unified_twice_daily_slots_and_force_update_false_input(self) -> None:
        import json
        from datetime import datetime

        def _brt_dt(y: int, m: int, d: int, hh: int, mm: int) -> datetime:
            return datetime(y, m, d, hh, mm, 0, tzinfo=precheck.BRT_TZ)

        self.assertEqual(precheck.get_ranking_unified_slot(_brt_dt(2026, 11, 24, 0, 1)), "")
        self.assertEqual(precheck.get_ranking_unified_slot(_brt_dt(2026, 11, 24, 0, 2)), "2026-11-24-00")
        self.assertEqual(precheck.get_ranking_unified_slot(_brt_dt(2026, 11, 24, 11, 59)), "2026-11-24-00")
        self.assertEqual(precheck.get_ranking_unified_slot(_brt_dt(2026, 11, 24, 12, 0)), "")
        self.assertEqual(precheck.get_ranking_unified_slot(_brt_dt(2026, 11, 24, 12, 1)), "")
        self.assertEqual(precheck.get_ranking_unified_slot(_brt_dt(2026, 11, 24, 12, 2)), "2026-11-24-12")
        self.assertEqual(precheck.get_ranking_unified_slot(_brt_dt(2026, 11, 24, 23, 59)), "2026-11-24-12")

        precheck.MONITORED_PROFILES = ((79327, "Chacineiro"),)
        precheck.MONITORED_PID_SET = frozenset({79327})

        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "state.json"
            t_0002 = int(_brt_dt(2026, 11, 24, 0, 2).timestamp())
            state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "last_dispatch_ts": t_0002 - 600,
                        "last_ranking_daily_date": "2026-11-24",
                        "players": {"79327": {"max_match_id": 500, "max_completion_time": t_0002 - 3600}},
                    }
                ),
                encoding="utf-8",
            )

            # 00:02 BRT -> dispara slot 00 com inputs {"force_update": false} e salva sem disparo do tracker
            with (
                patch.object(precheck.time, "time", return_value=t_0002),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 500, t_0002 - 3600, True, 500, t_0002 - 3600),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_0002, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_disp1,
            ):
                should_disp1, _, _, meta1 = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_tracker",
                )
            self.assertFalse(should_disp1)
            self.assertTrue(meta1["state_persisted"])
            mock_disp1.assert_called_once_with(
                token="tok_tracker",
                owner="nigosky",
                repo="coh3-br-ranking",
                workflow="ranking_unificado.yml",
                ref="main",
                inputs={"force_update": False},
            )
            self.assertEqual(precheck.load_state(state_path)["last_ranking_slot"], "2026-11-24-00")

            # 12:02 BRT -> dispara slot 12 com inputs {"force_update": false}
            t_1202 = int(_brt_dt(2026, 11, 24, 12, 2).timestamp())
            st_mid = precheck.load_state(state_path)
            st_mid["last_dispatch_ts"] = t_1202 - 600  # mantem tracker em SKIP (sem heartbeat 3h)
            precheck.atomic_write_json(state_path, st_mid)

            with (
                patch.object(precheck.time, "time", return_value=t_1202),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 500, t_0002 - 3600, True, 500, t_0002 - 3600),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_1202, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_disp2,
            ):
                should_disp2, _, _, meta2 = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_tracker",
                )
            self.assertFalse(should_disp2)
            self.assertTrue(meta2["state_persisted"])
            mock_disp2.assert_called_once_with(
                token="tok_tracker",
                owner="nigosky",
                repo="coh3-br-ranking",
                workflow="ranking_unificado.yml",
                ref="main",
                inputs={"force_update": False},
            )
            self.assertEqual(precheck.load_state(state_path)["last_ranking_slot"], "2026-11-24-12")

    def test_ranking_battlegroup_champions_saturday_and_sunday_recovery_window(self) -> None:
        from datetime import datetime

        def _brt_dt(y: int, m: int, d: int, hh: int, mm: int) -> datetime:
            return datetime(y, m, d, hh, mm, 0, tzinfo=precheck.BRT_TZ)

        # Sexta 2026-11-27 23:59 -> fora
        self.assertEqual(precheck.get_ranking_bg_slot(_brt_dt(2026, 11, 27, 23, 59)), "")
        # Sabado 2026-11-28 12:04 -> antes das 12:05
        self.assertEqual(precheck.get_ranking_bg_slot(_brt_dt(2026, 11, 28, 12, 4)), "")
        # Sabado 2026-11-28 12:05 -> abre slot semanal ISO 2026-W48
        self.assertEqual(precheck.get_ranking_bg_slot(_brt_dt(2026, 11, 28, 12, 5)), "2026-W48")
        # Domingo 2026-11-29 11:59 -> mesmo slot ISO 2026-W48 (janela de recuperacao)
        self.assertEqual(precheck.get_ranking_bg_slot(_brt_dt(2026, 11, 29, 11, 59)), "2026-W48")
        # Domingo 2026-11-29 12:00 -> janela de recuperacao encerrada
        self.assertEqual(precheck.get_ranking_bg_slot(_brt_dt(2026, 11, 29, 12, 0)), "")

        # Sabado 12:05 dispara e grava last_ranking_bg_slot; domingo 10:00 nao duplica
        prev_sat = {
            "last_ranking_daily_date": "2026-11-28",
            "last_ranking_slot": "2026-11-28-12",
            "last_ranking_bg_slot": "2026-W47",
        }
        with patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_sat:
            markers_sat, events_sat, changed_sat = precheck.evaluate_ranking_dispatches(
                now_brt=_brt_dt(2026, 11, 28, 12, 5),
                prev_state=prev_sat,
                tracker_token="tok_tracker",
            )
        self.assertTrue(changed_sat)
        self.assertEqual(markers_sat["last_ranking_bg_slot"], "2026-W48")
        self.assertEqual(len(events_sat), 1)
        mock_sat.assert_called_once_with(
            token="tok_tracker",
            owner="nigosky",
            repo="coh3-br-ranking",
            workflow="campeoes_battlegroups.yml",
            ref="main",
            inputs=None,
        )

        # Domingo 10:00 com marcador 2026-W48 ja salvo -> nao duplica
        prev_sun_done = {
            "last_ranking_daily_date": "2026-11-29",
            "last_ranking_slot": "2026-11-29-00",
            "last_ranking_bg_slot": "2026-W48",
        }
        with patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_sun_skip:
            _, events_sun_skip, changed_sun_skip = precheck.evaluate_ranking_dispatches(
                now_brt=_brt_dt(2026, 11, 29, 10, 0),
                prev_state=prev_sun_done,
                tracker_token="tok_tracker",
            )
        self.assertFalse(changed_sun_skip)
        self.assertEqual(events_sun_skip, [])
        mock_sun_skip.assert_not_called()

        # Recuperacao no domingo 11:59 BRT quando sabado nao rodou -> dispara normalmente!
        prev_sun_missed = {
            "last_ranking_daily_date": "2026-11-29",
            "last_ranking_slot": "2026-11-29-00",
            "last_ranking_bg_slot": "2026-W47",
        }
        with patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_sun_rec:
            markers_rec, events_rec, changed_rec = precheck.evaluate_ranking_dispatches(
                now_brt=_brt_dt(2026, 11, 29, 11, 59),
                prev_state=prev_sun_missed,
                tracker_token="tok_tracker",
            )
        self.assertTrue(changed_rec)
        self.assertEqual(markers_rec["last_ranking_bg_slot"], "2026-W48")
        self.assertEqual(len(events_rec), 1)
        mock_sun_rec.assert_called_once()

        # Domingo 12:00 BRT mesmo sem ter rodado no sabado -> nao dispara (janela encerrada)
        with patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_sun_late:
            _, events_late, changed_late = precheck.evaluate_ranking_dispatches(
                now_brt=_brt_dt(2026, 11, 29, 12, 0),
                prev_state={
                    "last_ranking_daily_date": "2026-11-29",
                    "last_ranking_slot": "2026-11-29-12",
                    "last_ranking_bg_slot": "2026-W47",
                },
                tracker_token="tok_tracker",
            )
        self.assertFalse(changed_late)
        self.assertEqual(events_late, [])
        mock_sun_late.assert_not_called()

    def test_ranking_token_priority_fallback_retry_and_tracker_preserves_markers(self) -> None:
        import json
        from datetime import datetime

        self.assertEqual(precheck.resolve_ranking_dispatch_token("rank_tok", "trk_tok"), "rank_tok")
        self.assertEqual(precheck.resolve_ranking_dispatch_token("", "trk_tok"), "trk_tok")
        self.assertEqual(precheck.resolve_ranking_dispatch_token("   ", "trk_tok"), "trk_tok")

        precheck.MONITORED_PROFILES = ((79327, "Chacineiro"),)
        precheck.MONITORED_PID_SET = frozenset({79327})
        t_0805 = int(datetime(2026, 11, 24, 8, 5, 0, tzinfo=precheck.BRT_TZ).timestamp())

        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "last_dispatch_ts": t_0805 - 600,
                        "last_ranking_slot": "2026-11-24-00",
                        "last_ranking_bg_slot": "2026-W47",
                        "players": {"79327": {"max_match_id": 500, "max_completion_time": t_0805 - 3600}},
                    }
                ),
                encoding="utf-8",
            )

            # 1) Falha HTTP 500 no ranking usando RANKING_DISPATCH_TOKEN -> marcador NAO avanca
            with (
                patch.object(precheck.time, "time", return_value=t_0805),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 500, t_0805 - 3600, True, 500, t_0805 - 3600),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_0805, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(False, 500)) as mock_fail,
            ):
                _, _, _, meta_fail = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_tracker",
                    ranking_token="tok_ranking_dedicated",
                )
            self.assertFalse(meta_fail["state_persisted"])
            self.assertEqual(meta_fail["ranking_dispatches_failed"], 1)
            self.assertEqual(meta_fail["ranking_dispatches"][0]["token_source"], "RANKING_DISPATCH_TOKEN")
            mock_fail.assert_called_once_with(
                token="tok_ranking_dedicated",
                owner="nigosky",
                repo="coh3-br-ranking",
                workflow="boletim_diario.yml",
                ref="main",
                inputs=None,
            )
            self.assertEqual(precheck.load_state(state_path).get("last_ranking_daily_date", ""), "")

            # 2) Proximo ciclo sem RANKING_DISPATCH_TOKEN -> fallback automatico para TRACKER_DISPATCH_TOKEN e sucesso!
            with (
                patch.object(precheck.time, "time", return_value=t_0805 + 300),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 500, t_0805 - 3600, True, 500, t_0805 - 3600),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_0805 + 300, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_ok,
            ):
                _, _, _, meta_ok = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_tracker_fallback",
                    ranking_token="",
                )
            self.assertTrue(meta_ok["state_persisted"])
            self.assertEqual(meta_ok["ranking_dispatches"][0]["token_source"], "TRACKER_DISPATCH_TOKEN (fallback)")
            mock_ok.assert_called_once_with(
                token="tok_tracker_fallback",
                owner="nigosky",
                repo="coh3-br-ranking",
                workflow="boletim_diario.yml",
                ref="main",
                inputs=None,
            )

            # 3) Disparo posterior do tracker (nova partida) preserva os 3 marcadores de ranking em state.json!
            with (
                patch.object(precheck.time, "time", return_value=t_0805 + 600),
                patch.object(
                    precheck,
                    "fetch_latest_player_marker",
                    return_value=(79327, 501, t_0805 + 500, True, 501, t_0805 + 500),
                ),
                patch.object(precheck, "fetch_veteranos_radar_active_pids", return_value=(set(), t_0805 + 600, True)),
                patch.object(precheck, "dispatch_private_tracker", return_value=(True, 204)) as mock_trk,
            ):
                should_trk, ok_trk, _, meta_trk = precheck.evaluate_precheck(
                    state_path=state_path,
                    dispatch_token="tok_tracker_only",
                    ranking_token="tok_ranking_dedicated",
                )
            self.assertTrue(should_trk and ok_trk)
            self.assertEqual(meta_trk["ranking_dispatches"], [])
            mock_trk.assert_called_once_with(
                token="tok_tracker_only",
                owner="nigosky",
                repo="coh3-br-tracker",
                workflow="coh3_monitor.yml",
                ref="main",
            )
            saved_after_trk = precheck.load_state(state_path)
            self.assertEqual(saved_after_trk["last_ranking_daily_date"], "2026-11-24")
            self.assertEqual(saved_after_trk["last_ranking_slot"], "2026-11-24-00")
            self.assertEqual(saved_after_trk["last_ranking_bg_slot"], "2026-W47")
            self.assertEqual(saved_after_trk["players"]["79327"]["max_match_id"], 501)

    def test_ranking_token_redaction_and_scheduler_workflow_secret_gate(self) -> None:
        import re
        import subprocess

        fake_rank_tok = "rank_secret_" + ("9" * 16)
        scrubbed = precheck.redact_secrets(f"env RANKING_DISPATCH_TOKEN={fake_rank_tok} ok")
        self.assertNotIn(fake_rank_tok, scrubbed)
        self.assertIn("RANKING_DISPATCH_TOKEN=[SECRET_REDACTED]", scrubbed)

        repo_root = Path(__file__).resolve().parent
        wf_path = repo_root / ".github" / "workflows" / "scheduler.yml"
        wf_text = wf_path.read_text(encoding="utf-8")
        self.assertIn("RANKING_DISPATCH_TOKEN: ${{ secrets.RANKING_DISPATCH_TOKEN }}", wf_text)
        m = re.search(r'SECRET_PATTERN="([^"]+)"', wf_text)
        self.assertIsNotNone(m, "SECRET_PATTERN nao encontrado em scheduler.yml")
        pattern = m.group(1)  # type: ignore[union-attr]

        # Valida com o mesmo grep -Ei usado no gate de CI (POSIX ERE)
        for sample in (
            "RANKING_DISPATCH_TOKEN=" + ("A" * 20),
            "TRACKER_DISPATCH_TOKEN=" + ("B" * 20),
        ):
            proc = subprocess.run(
                ["grep", "-Ei", pattern],
                input=sample,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=f"SECRET_PATTERN nao bloqueou: {sample!r}")

        # Garante que os arquivos-fonte do repositorio nao disparam falso positivo no gate anti-vazamento
        clean_proc = subprocess.run(
            ["grep", "-InEi", pattern, str(repo_root / "precheck.py"), str(repo_root / "test_precheck.py"), str(wf_path)],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(clean_proc.returncode, 1, msg=f"Falso positivo no gate anti-vazamento:\n{clean_proc.stdout}")


if __name__ == "__main__":
    unittest.main()
