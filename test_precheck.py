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
        self.assertEqual(len(precheck.FALLBACK_MONITORED_PROFILES), 45)
        self.assertEqual(len(precheck.FALLBACK_STEAM_ID_TO_PID), 45)
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


if __name__ == "__main__":
    unittest.main()
