# Updated auto_sim_and_log_loop.py with wait guards and streaming-safe snapshot handling

from core import config
import argparse
import sys
import os
from core.bootstrap import *  # noqa
from dotenv import load_dotenv
from core.logger import get_logger

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PYTHON = sys.executable

load_dotenv()
logger = get_logger(__name__)

parser = argparse.ArgumentParser("Auto sim loop")
parser.add_argument("--debug", action="store_true", help="Enable debug logging")
parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
args = parser.parse_args()

config.DEBUG_MODE = args.debug
config.VERBOSE_MODE = args.verbose
if config.DEBUG_MODE:
    print("\U0001f9ea DEBUG_MODE ENABLED — Verbose output activated")

import time
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from core.utils import now_eastern
from core.odds_fetcher import fetch_all_market_odds, save_market_odds_to_file
from core.snapshot_core import load_latest_snapshot

EDGE_THRESHOLD = 0.05
MIN_EV = 0.05
SIM_INTERVAL = 60 * 30
LOG_INTERVAL = 60 * 5
last_sim_time = 0
last_log_time = 0
last_snapshot_time = 0

closing_monitor_proc = None
active_processes: list[dict] = []

def run_subprocess(cmd):
    timestamp = now_eastern()
    logger.info("\n%s", "═" * 60)
    logger.info("🚀 [%s] Starting subprocess:", timestamp)
    logger.info("👉 %s", " ".join(cmd))
    logger.info("%s\n", "═" * 60)
    try:
        proc = subprocess.run(
            cmd,
            cwd=ROOT_DIR,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            check=True,
            env={**os.environ, "PYTHONPATH": ROOT_DIR},
        )
        if proc.stdout:
            logger.debug("\ud83d\udce4 STDOUT:\n%s", proc.stdout)
        if proc.stderr:
            logger.debug("\u26a0\ufe0f STDERR:\n%s", proc.stderr)
        logger.info("\n✅ [%s] Subprocess completed with exit code %s", now_eastern(), proc.returncode)
        return proc.returncode
    except subprocess.CalledProcessError as e:
        if e.stdout:
            logger.debug("\ud83d\udce4 STDOUT (on error):\n%s", e.stdout)
        if e.stderr:
            logger.debug("\u26a0\ufe0f STDERR (on error):\n%s", e.stderr)
        logger.error("\n❌ [%s] Command %s exited with code %s", now_eastern(), " ".join(cmd), e.returncode)
        return e.returncode

def launch_process(name: str, cmd: list[str]) -> subprocess.Popen:
    proc = subprocess.Popen(cmd, cwd=ROOT_DIR, env={**os.environ, "PYTHONPATH": ROOT_DIR})
    active_processes.append({"name": name, "proc": proc, "start": time.time()})
    logger.info("🚀 [%s] Started %s (PID %d)", now_eastern(), name, proc.pid)
    return proc

def poll_active_processes():
    for entry in list(active_processes):
        proc = entry["proc"]
        ret = proc.poll()
        if ret is not None:
            runtime = time.time() - entry["start"]
            if ret == 0:
                logger.info("✅ [%s] Subprocess '%s' (PID %d) completed in %.1fs", now_eastern(), entry["name"], proc.pid, runtime)
            else:
                logger.error("❌ [%s] Subprocess '%s' (PID %d) exited with code %s", now_eastern(), entry["name"], proc.pid, ret)
            active_processes.remove(entry)

def wait_for_dispatch_and_snapshot():
    while any(p["name"].startswith("dispatch") or "unified_snapshot_generator" in p["name"] for p in active_processes):
        poll_active_processes()
        logger.info("⏳ Waiting for snapshot/dispatch scripts to complete...")
        time.sleep(3)

def wait_for_loggers():
    while any("logbets" in p["name"].lower() for p in active_processes):
        poll_active_processes()
        logger.info("⏳ Waiting for LogBets process to complete before monitoring...")
        time.sleep(3)

def fetch_and_cache_odds_snapshot():
    logger.info("\n📡 [%s] Fetching market odds for today and tomorrow...", now_eastern())
    odds = fetch_all_market_odds(lookahead_days=2)
    if not odds or not isinstance(odds, dict) or len(odds) == 0:
        logger.error("❌ Fetched odds snapshot is empty or invalid — skipping loop cycle.")
        return None
    timestamp = now_eastern().strftime("%Y%m%dT%H%M")
    tag = f"market_odds_{timestamp}"
    odds_path = save_market_odds_to_file(odds, tag)
    logger.info("✅ [%s] Saved shared odds snapshot: %s", now_eastern(), odds_path)
    return odds_path

def run_logger(odds_path: str):
    today_str = now_eastern().strftime("%Y-%m-%d")
    tomorrow_str = (now_eastern() + timedelta(days=1)).strftime("%Y-%m-%d")
    for date_str in [today_str, tomorrow_str]:
        eval_folder = os.path.join("backtest/sims", date_str)
        if date_str == tomorrow_str:
            if not os.path.isdir(eval_folder) or not any(f.endswith(".json") for f in os.listdir(eval_folder)):
                logger.info("⏭ Skipping tomorrow's eval: sim data not ready yet")
                continue
        cmd = [PYTHON, "-m", "cli.log_betting_evals", "--eval-folder", eval_folder, f"--odds-path={odds_path}", f"--min-ev={MIN_EV}", "--debug", "--output-dir=logs"]
        launch_process(f"LogBets {date_str}", cmd)

def run_unified_snapshot_and_dispatch(odds_path: str):
    today_str = now_eastern().strftime("%Y-%m-%d")
    tomorrow_str = (now_eastern() + timedelta(days=1)).strftime("%Y-%m-%d")
    date_arg = f"{today_str},{tomorrow_str}"
    run_subprocess([PYTHON, "-m", "core.unified_snapshot_generator", "--odds-path", odds_path, "--date", date_arg])
    for script in ["dispatch_live_snapshot.py", "dispatch_fv_drop_snapshot.py", "dispatch_best_book_snapshot.py", "dispatch_personal_snapshot.py", "dispatch_sim_only_snapshot.py"]:
        cmd = [PYTHON, f"core/{script}", "--output-discord"]
        launch_process(script, cmd)
    launch_process("dispatch_clv_snapshot.py", [PYTHON, "core/dispatch_clv_snapshot.py", "--output-discord"])

# INITIAL LAUNCH
logger.info("🔄 [%s] Starting auto loop...", now_eastern())
initial_odds = fetch_and_cache_odds_snapshot()
if initial_odds:
    run_unified_snapshot_and_dispatch(initial_odds)
    wait_for_dispatch_and_snapshot()
    run_logger(initial_odds)
    wait_for_loggers()
    logger.info("✅ Initial snapshot and logging complete — entering monitor loop")

while True:
    poll_active_processes()
    now = time.time()
    if now - last_log_time > LOG_INTERVAL:
        odds_file = fetch_and_cache_odds_snapshot()
        if odds_file:
            run_unified_snapshot_and_dispatch(odds_file)
            wait_for_dispatch_and_snapshot()
            run_logger(odds_file)
            wait_for_loggers()
        last_log_time = now
    if now - last_sim_time > SIM_INTERVAL:
        # simulate (if needed)
        last_sim_time = now
    logger.info("⏱ Sleeping for 10 seconds...\n")
    time.sleep(10)
