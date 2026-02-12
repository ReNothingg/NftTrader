#!/usr/bin/env python3
"""Launcher for running multiple market snipers in parallel."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_CONFIG: Dict[str, Any] = {
    "shutdown_timeout_sec": 8,
    "markets": [
        {
            "name": "portal",
            "enabled": True,
            "command": ["${PYTHON}", "-m", "src.services.portal.sniper"],
            "cwd": ".",
            "restart": True,
            "restart_delay_sec": 2,
            "env": {"PYTHONUNBUFFERED": "1"},
        },
        {
            "name": "tonnel",
            "enabled": True,
            "command": ["${PYTHON}", "-m", "src.services.tonnel.sniper", "--mode", "mock"],
            "cwd": ".",
            "restart": True,
            "restart_delay_sec": 2,
            "env": {"PYTHONUNBUFFERED": "1"},
        },
        {
            "name": "mrkt",
            "enabled": True,
            "command": ["${PYTHON}", "-m", "src.services.mrkt.sniper", "--mode", "mock"],
            "cwd": ".",
            "restart": True,
            "restart_delay_sec": 2,
            "env": {"PYTHONUNBUFFERED": "1"},
        },
    ],
}


@dataclass
class MarketSpec:
    name: str
    enabled: bool
    command: List[str]
    cwd: Path
    restart: bool
    restart_delay_sec: float
    env: Dict[str, str]


class ManagedProcess:
    def __init__(self, project_root: Path, spec: MarketSpec) -> None:
        self.project_root = project_root
        self.spec = spec
        self.process: Optional[subprocess.Popen[str]] = None
        self._output_thread: Optional[threading.Thread] = None
        self.next_restart_at = 0.0

    def start(self) -> None:
        if self.process and self.process.poll() is None:
            return

        command = [expand_token(token, self.project_root) for token in self.spec.command]
        env = os.environ.copy()
        env.update(self.spec.env)
        env.setdefault("PYTHONUNBUFFERED", "1")
        env.setdefault("PROJECT_ROOT", str(self.project_root))

        log(self.spec.name, f"starting: {' '.join(command)}")
        self.process = subprocess.Popen(
            command,
            cwd=str(self.spec.cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._output_thread = threading.Thread(
            target=self._stream_output,
            name=f"{self.spec.name}-output",
            daemon=True,
        )
        self._output_thread.start()

    def _stream_output(self) -> None:
        if not self.process or not self.process.stdout:
            return

        for line in self.process.stdout:
            text = line.rstrip("\r\n")
            if text:
                log(self.spec.name, text)

    def poll(self) -> Optional[int]:
        if not self.process:
            return None
        return self.process.poll()

    def stop(self, timeout_sec: float) -> None:
        if not self.process:
            return
        if self.process.poll() is not None:
            return

        log(self.spec.name, "stopping...")
        self.process.terminate()
        try:
            self.process.wait(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            log(self.spec.name, "force kill")
            self.process.kill()


def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log(source: str, message: str) -> None:
    print(f"[{now_str()}] [{source}] {message}", flush=True)


def expand_token(token: str, project_root: Path) -> str:
    if token == "${PYTHON}":
        return sys.executable
    if token == "${PROJECT_ROOT}":
        return str(project_root)
    return token


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise RuntimeError(f"Config must be a JSON object: {path}")
    return data


def write_default_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        f.write("\n")


def load_specs(config_path: Path, project_root: Path) -> tuple[List[MarketSpec], float]:
    if not config_path.exists():
        write_default_config(config_path)
        log("launcher", f"created default config: {config_path}")

    raw = read_json(config_path)
    shutdown_timeout_sec = float(raw.get("shutdown_timeout_sec", 8))

    raw_markets = raw.get("markets", [])
    if not isinstance(raw_markets, list):
        raise RuntimeError("'markets' must be an array")

    specs: List[MarketSpec] = []
    for idx, item in enumerate(raw_markets):
        if not isinstance(item, dict):
            raise RuntimeError(f"markets[{idx}] must be an object")

        name = str(item.get("name", f"market_{idx + 1}")).strip() or f"market_{idx + 1}"
        enabled = bool(item.get("enabled", True))

        command = item.get("command")
        if not isinstance(command, list) or not command:
            raise RuntimeError(f"markets[{idx}].command must be a non-empty array")
        command_tokens = [str(token) for token in command]

        cwd_raw = str(item.get("cwd", "."))
        cwd = (project_root / cwd_raw).resolve()
        if not cwd.exists() or not cwd.is_dir():
            raise RuntimeError(f"markets[{idx}] invalid cwd: {cwd}")

        env_raw = item.get("env", {})
        if env_raw is None:
            env_raw = {}
        if not isinstance(env_raw, dict):
            raise RuntimeError(f"markets[{idx}].env must be an object")
        env = {str(k): str(v) for k, v in env_raw.items()}

        restart = bool(item.get("restart", True))
        restart_delay_sec = max(0.0, float(item.get("restart_delay_sec", 2)))

        specs.append(
            MarketSpec(
                name=name,
                enabled=enabled,
                command=command_tokens,
                cwd=cwd,
                restart=restart,
                restart_delay_sec=restart_delay_sec,
                env=env,
            )
        )

    return specs, shutdown_timeout_sec


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run all configured market snipers")
    parser.add_argument(
        "--config",
        default=os.getenv("SNIPERS_CONFIG", "configs/snipers.json"),
        help="Path to launcher config JSON",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    config_path = (project_root / args.config).resolve()

    try:
        specs, shutdown_timeout_sec = load_specs(config_path, project_root)
    except Exception as e:
        log("launcher", f"config error: {e}")
        return 1

    managed: List[ManagedProcess] = [
        ManagedProcess(project_root, spec) for spec in specs if spec.enabled
    ]
    if not managed:
        log("launcher", "no enabled markets in config")
        return 1

    stop_event = threading.Event()

    def _handle_signal(signum: int, _frame: Any) -> None:
        log("launcher", f"signal {signum} received")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    for proc in managed:
        proc.start()

    try:
        while not stop_event.is_set():
            for proc in managed:
                exit_code = proc.poll()
                if exit_code is None:
                    continue

                if not proc.spec.restart:
                    continue

                now = time.time()
                if proc.next_restart_at == 0.0:
                    proc.next_restart_at = now + proc.spec.restart_delay_sec
                    log(
                        "launcher",
                        f"{proc.spec.name} exited with code {exit_code}, restart in {proc.spec.restart_delay_sec:.1f}s",
                    )
                    continue

                if now >= proc.next_restart_at:
                    proc.next_restart_at = 0.0
                    proc.start()

            time.sleep(0.4)
    finally:
        for proc in managed:
            proc.stop(shutdown_timeout_sec)
        log("launcher", "all markets stopped")

    return 0


if __name__ == "__main__":
    sys.exit(main())
