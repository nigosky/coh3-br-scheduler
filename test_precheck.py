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
        self.assertEqual(len(precheck.FALLBACK_MONITORED_PROFILES), 47)
        self.assertEqual(len(precheck.FALLBACK_STEAM_ID_TO_PID), 47)
        self.assertEqual(
            set(precheck.FALLBACK_STEAM_ID_TO_PID.values()),
            {pid for pid, _ in precheck.FALLBACK_MONITORED_PROFILES},
        )
        # Conta secundária do arc (#1423426) e smurf do Chacineiro (#1333418) presentes no fallback e mapa SteamID64
        self.assertEqual(precheck.FALLBACK_STEAM_ID_TO_PID.get("76561199482388323"), 1423426)
        self.assertEqual(precheck.FALLBACK_STEAM_ID_TO_PID.get("76561198698969876"), 1333418)

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
            "manual_other_br": [
                {"profile_id": 33487, "steam_id": "76561197995712470", "original_alias": "Generick", "country": "us"},
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

        # Quando Chacineiro principal (#79327) está no roster remoto sem a smurf #1333418, preserva a smurf automaticamente
        with patch.object(precheck, "fetch_shared_monitored_profiles", return_value=((79327, "Chacineiro"), (202, "Beta"))):
            self.assertTrue(precheck.refresh_monitored_profiles())
            self.assertEqual(
                precheck.MONITORED_PROFILES,
                ((79327, "Chacineiro"), (1333418, "Chacineiro (sec)"), (202, "Beta")),
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
            # fast[0] = Veteranos no CoH3 (inclui Razi, arc sec e Chacineiro smurf via Steam Web API),
            # fast[1] = Online/outros jogos (nigo_BR em Marvel Rivals — NÃO deve ser incluído!)
            "last_steam_fast_sig": repr(
                (
                    [
                        ("76561198080023560", False, False, False, "Razi"),
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
        self.assertEqual(active, {79327, 639157, 110078, 81751, 1423426, 1333418})
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


if __name__ == "__main__":
    unittest.main()
