#!/usr/bin/env python3
"""Exercise coverage-script fixture discovery without running Cargo."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parent.parent
COVERAGE_SCRIPT = ROOT / "scripts" / "check-coverage.sh"
LIBRARIES = (
    "module_clock",
    "module_clock_two",
    "module_wrong_target",
    "module_context",
    "module_context_configured",
    "module_context_factory",
    "module_context_eager",
    "module_trusted_migration",
    "module_host_callback",
    "module_host_stream_callback",
    "module_controlled_init",
    "module_terminal",
)
TERMINAL_TESTS = (
    "terminal_runtime_tests::compiled_native_fixture_first",
    "terminal_runtime_tests::compiled_native_fixture_second",
)
IGNORE_REGEX = (
    r"crates/tinybus/src/ports/(listener|transport)\.rs$"
    r"|crates/tinybus/src/module/github\.rs$"
    r"|crates/tinybus/src/module/(host|mod)\.rs$"
    r"|crates/tinybus/src/bin/(tinybus\.rs|tinybus/unix\.rs)$"
    r"|crates/tinybus/src/connection\.rs$"
    r"|crates/tinybus/src/test_support\.rs$"
)


FAKE_CARGO = r"""#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
target = Path(os.environ["FAKE_TARGET_DIR"])
suffix = os.environ["FAKE_FIXTURE_SUFFIX"]
log = Path(os.environ["FAKE_CARGO_LOG"])
entry = {"args": args}

if args and args[0] == "build":
    examples = target / "debug" / "examples"
    examples.mkdir(parents=True, exist_ok=True)
    for name in (
        "module_clock",
        "module_clock_two",
        "module_wrong_target",
        "module_context",
        "module_context_configured",
        "module_context_factory",
        "module_context_eager",
        "module_trusted_migration",
        "module_host_callback",
        "module_host_stream_callback",
        "module_controlled_init",
        "module_terminal",
    ):
        (examples / f"lib{name}.{suffix}").write_bytes(name.encode())
elif args[:2] == ["metadata", "--format-version"]:
    print(json.dumps({"target_directory": str(target)}))
elif args[:2] == ["llvm-cov", "--workspace"]:
    fixture_variables = (
        "TINYBUS_TEST_MODULE",
        "TINYBUS_TEST_MODULE_TWO",
        "TINYBUS_TEST_WRONG_TARGET",
        "TINYBUS_TEST_CONTEXT_MODULE",
        "TINYBUS_TEST_CONFIGURED_CONTEXT_MODULE",
        "TINYBUS_TEST_CONTEXT_FACTORY_MODULE",
        "TINYBUS_TEST_CONTEXT_EAGER_MODULE",
        "TINYBUS_TEST_MIGRATION_MODULE",
        "TINYBUS_TEST_HOST_CALLBACK_MODULE",
        "TINYBUS_TEST_HOST_STREAM_CALLBACK_MODULE",
        "TINYBUS_TEST_CONTROLLED_INIT_MODULE",
        "TINYBUS_TEST_TERMINAL_MODULE",
    )
    fixtures = {name: os.environ.get(name, "") for name in fixture_variables}
    if any(not Path(path).is_file() for path in fixtures.values()):
        print("coverage did not receive every compiled fixture", file=sys.stderr)
        sys.exit(3)
    for variable in ("TINYBUS_TEST_MODULE_TWO", "TINYBUS_TEST_TERMINAL_MODULE"):
        path = Path(fixtures[variable])
        manifest = path.parent / "modules.toml"
        expected = f'"{path.name}" = "{hashlib.sha256(path.read_bytes()).hexdigest()}"'
        if not manifest.is_file() or expected not in manifest.read_text():
            print(f"invalid private fixture manifest for {variable}", file=sys.stderr)
            sys.exit(4)
    entry["fixture_paths"] = fixtures
elif args[:2] == ["test", "--locked"] and "--list" in args:
    for name in (
        "terminal_runtime_tests::compiled_native_fixture_first",
        "terminal_runtime_tests::compiled_native_fixture_second",
    ):
        print(f"{name}: test")
elif args[:2] == ["llvm-cov", "test"]:
    if "--ignored" not in args or "--exact" not in args:
        print("terminal scenario lost isolated ignored/exact execution", file=sys.stderr)
        sys.exit(5)
elif args[:2] == ["llvm-cov", "report"]:
    if args[-2:] != ["--fail-under-file-lines", "90"]:
        print("coverage report threshold changed", file=sys.stderr)
        sys.exit(6)
else:
    print(f"unexpected fake Cargo invocation: {args!r}", file=sys.stderr)
    sys.exit(7)

with log.open("a", encoding="utf-8") as output:
    output.write(json.dumps(entry) + "\n")
"""


class CoverageScriptPlatformTests(unittest.TestCase):
    def run_coverage_script(self, suffix: str) -> tuple[subprocess.CompletedProcess[str], list[dict]]:
        with tempfile.TemporaryDirectory(prefix="tinybus-coverage-script-") as directory:
            temporary = Path(directory)
            checkout = temporary / "checkout"
            scripts = checkout / "scripts"
            scripts.mkdir(parents=True)
            shutil.copyfile(COVERAGE_SCRIPT, scripts / COVERAGE_SCRIPT.name)

            fake_bin = temporary / "bin"
            fake_bin.mkdir()
            cargo = fake_bin / "cargo"
            cargo.write_text(textwrap.dedent(FAKE_CARGO), encoding="utf-8")
            cargo.chmod(0o755)

            target = checkout / "target"
            call_log = temporary / "cargo-calls.jsonl"
            environment = os.environ.copy()
            environment.update(
                {
                    "FAKE_CARGO_LOG": str(call_log),
                    "FAKE_FIXTURE_SUFFIX": suffix,
                    "FAKE_TARGET_DIR": str(target),
                    "PATH": os.pathsep.join((str(fake_bin), os.environ["PATH"])),
                }
            )
            result = subprocess.run(
                ["bash", str(scripts / COVERAGE_SCRIPT.name)],
                cwd=checkout,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            calls = [
                json.loads(line)
                for line in call_log.read_text(encoding="utf-8").splitlines()
            ] if call_log.exists() else []
            return result, calls

    def assert_coverage_contract(
        self,
        result: subprocess.CompletedProcess[str],
        calls: list[dict],
        suffix: str,
    ) -> None:
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        args = [call["args"] for call in calls]

        self.assertEqual(
            args[0],
            [
                "build",
                "--locked",
                "--example",
                "module_clock",
                "--example",
                "module_clock_two",
                "--example",
                "module_wrong_target",
                "--example",
                "module_context",
                "--example",
                "module_context_configured",
                "--example",
                "module_context_factory",
                "--example",
                "module_context_eager",
                "--example",
                "module_trusted_migration",
                "--example",
                "module_host_callback",
                "--example",
                "module_host_stream_callback",
                "--example",
                "module_controlled_init",
                "--example",
                "module_terminal",
                "--no-default-features",
                "--features",
                "uds,modules,macros,__native-fixture-support",
            ],
        )
        self.assertEqual(
            args[1],
            ["metadata", "--format-version", "1", "--no-deps"],
        )

        coverage_call = next(call for call in calls if call["args"][:2] == ["llvm-cov", "--workspace"])
        self.assertEqual(
            coverage_call["args"],
            [
                "llvm-cov",
                "--workspace",
                "--all-features",
                "--ignore-filename-regex",
                IGNORE_REGEX,
                "--no-report",
                "--",
                "--include-ignored",
                "--skip",
                "terminal_runtime_tests::compiled_native_",
            ],
        )
        fixture_paths = coverage_call["fixture_paths"]
        self.assertEqual(len(fixture_paths), len(LIBRARIES))
        for variable, library in zip(fixture_paths, LIBRARIES, strict=True):
            self.assertTrue(fixture_paths[variable].endswith(f"lib{library}.{suffix}"))

        list_call = next(call for call in calls if call["args"][:2] == ["test", "--locked"])
        self.assertEqual(
            list_call["args"],
            [
                "test",
                "--locked",
                "-p",
                "tinybus-module",
                "--all-features",
                "--features",
                "tinybus/macros,tinybus/uds",
                "--",
                "--list",
                "--ignored",
            ],
        )
        isolated_calls = [call["args"] for call in calls if call["args"][:2] == ["llvm-cov", "test"]]
        self.assertEqual(
            isolated_calls,
            [
                [
                    "llvm-cov",
                    "test",
                    "--no-report",
                    "-p",
                    "tinybus-module",
                    "--all-features",
                    "--features",
                    "tinybus/macros,tinybus/uds",
                    test,
                    "--",
                    "--ignored",
                    "--exact",
                ]
                for test in TERMINAL_TESTS
            ],
        )
        report_call = next(call for call in calls if call["args"][:2] == ["llvm-cov", "report"])
        self.assertEqual(
            report_call["args"],
            [
                "llvm-cov",
                "report",
                "--ignore-filename-regex",
                IGNORE_REGEX,
                "--fail-under-file-lines",
                "90",
            ],
        )

    def test_linux_so_fixtures_reach_the_unchanged_coverage_gate(self) -> None:
        result, calls = self.run_coverage_script("so")
        self.assert_coverage_contract(result, calls, "so")

    def test_macos_dylib_fixtures_reach_the_unchanged_coverage_gate(self) -> None:
        result, calls = self.run_coverage_script("dylib")
        self.assert_coverage_contract(result, calls, "dylib")

    def test_fake_cargo_harness_is_limited_to_the_posix_feature_lane(self) -> None:
        lines = (ROOT / "scripts" / "test-features.sh").read_text(
            encoding="utf-8"
        ).splitlines()
        case_line = 'case "${RUNNER_OS:-$(uname -s)}" in'
        self.assertIn(
            case_line,
            lines,
            "the fake-Cargo coverage harness must be scoped to the POSIX runner branch",
        )
        case_start = lines.index(case_line)
        windows_branch = next(
            index
            for index, line in enumerate(lines)
            if line.startswith("  Windows|")
        )
        posix_branch = next(
            index
            for index, line in enumerate(lines)
            if line.strip() == "Linux|Darwin|macOS)"
        )
        posix_end = next(
            index
            for index, line in enumerate(lines[posix_branch + 1 :], posix_branch + 1)
            if line.strip() == ";;"
        )
        case_end = next(
            index
            for index, line in enumerate(lines[windows_branch + 1 :], windows_branch + 1)
            if line.strip() == "esac"
        )
        invocations = [
            index
            for index, line in enumerate(lines)
            if line.strip() == "python3 scripts/test_check_coverage.py"
        ]

        self.assertEqual(len(invocations), 1)
        self.assertGreater(invocations[0], posix_branch)
        self.assertLess(invocations[0], posix_end)
        self.assertNotIn(
            "python3 scripts/test_check_coverage.py",
            [line.strip() for line in lines[windows_branch + 1 : case_end]],
        )
        self.assertGreater(windows_branch, case_start)
        self.assertEqual(
            [line.strip() for line in lines if line.strip().startswith("cargo ")],
            [
                "cargo check --locked --workspace --no-default-features",
                "cargo check --locked --package tinybus --tests",
                "cargo check --locked --package tinybus --no-default-features --features modules",
                "cargo metadata --locked --format-version 1 --manifest-path examples/Cargo.toml >/dev/null",
                "cargo test --locked --workspace --no-default-features --features macros",
                "cargo test --locked --workspace --all-features",
            ],
        )


if __name__ == "__main__":
    unittest.main()
