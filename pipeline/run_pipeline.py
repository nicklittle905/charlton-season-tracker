import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]


def _run(cmd, cwd: Path) -> subprocess.CompletedProcess:
    """Run a subprocess and capture output."""
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )


def run_refresh(team_id: int) -> Dict[str, Any]:
    """
    Run ingest + dbt for the provided team_id.

    Returns a dict: {
        "ingest_ok": bool,
        "dbt_ok": bool,
        "ingest_stdout": str,
        "dbt_stdout": str,
    }
    """
    ingest_proc = _run([sys.executable, "-m", "ingest.load_raw"], ROOT)
    ingest_ok = ingest_proc.returncode == 0
    ingest_out = (ingest_proc.stdout or "") + (ingest_proc.stderr or "")

    if ingest_ok:
        dbt_cmd = [
            "dbt",
            "run",
            "--project-dir",
            "charlton_dbt",
            "--vars",
            json.dumps({"team_id": int(team_id)}),
        ]
        dbt_proc = _run(dbt_cmd, ROOT)
        dbt_ok = dbt_proc.returncode == 0
        dbt_out = (dbt_proc.stdout or "") + (dbt_proc.stderr or "")
    else:
        dbt_ok = False
        dbt_out = "Skipped dbt run because ingest failed."

    return {
        "ingest_ok": ingest_ok,
        "dbt_ok": dbt_ok,
        "ingest_stdout": ingest_out,
        "dbt_stdout": dbt_out,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run ingest + dbt for a team_id")
    parser.add_argument("--team-id", type=int, default=348, help="Team ID to build marts for")
    args = parser.parse_args(argv)

    result = run_refresh(args.team_id)

    print(f"Ingest OK: {result['ingest_ok']}")
    print(f"dbt OK: {result['dbt_ok']}")

    if not result["ingest_ok"]:
        print("\nIngest output:\n")
        print(result["ingest_stdout"])
    if not result["dbt_ok"]:
        print("\ndbt output:\n")
        print(result["dbt_stdout"])

    return 0 if (result["ingest_ok"] and result["dbt_ok"]) else 1


if __name__ == "__main__":
    sys.exit(main())
