import argparse
import json
import requests
import sys
from pathlib import Path
from typing import Any


ENDPOINTS = {
    "power": "/api/power",
    "cpu_utilization": "/api/cpu_utilization",
}


def fetch_json(url: str, timeout: float) -> Any:
    response = requests.get(url, headers={"accept": "application/json"}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export OpenDT API metrics to JSON files.")
    parser.add_argument("--base-url", default="http://localhost:3001", help="API base URL")
    parser.add_argument("--interval-seconds", type=int, default=60, help="interval_seconds query value")
    parser.add_argument("--output-dir", required=True, help="Directory where JSON files are written")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Keep successful endpoint files even if one endpoint fails",
    )
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    had_error = False
    for name, path in ENDPOINTS.items():
        url = f"{base_url}{path}?interval_seconds={args.interval_seconds}"
        output_path = output_dir / f"{name}.json"
        try:
            payload = fetch_json(url, args.timeout)
            write_json(output_path, payload)
            print(f"Wrote {output_path}")
        except (requests.RequestException, ValueError, OSError) as exc:
            had_error = True
            print(f"Failed to export {name} from {url}: {type(exc).__name__}: {exc}", file=sys.stderr)
            if not args.allow_partial:
                return 1

    if had_error:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
