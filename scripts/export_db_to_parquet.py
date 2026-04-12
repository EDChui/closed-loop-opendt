import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import pandas as pd
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import make_url
except ModuleNotFoundError as exc:
    missing = exc.name or "required dependency"
    print(
        "Error: missing Python dependency '"
        f"{missing}' needed for database export. "
        "Install project dependencies first.",
        file=sys.stderr,
    )
    sys.exit(1)


DEFAULT_DB_URL = "postgresql+psycopg://opendt:opendt@localhost:5433/opendt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export PostgreSQL tables to Parquet")
    parser.add_argument("--database-url", default=DEFAULT_DB_URL, help="SQLAlchemy database URL")
    parser.add_argument("--output-dir", required=True, help="Directory where Parquet files are written")
    parser.add_argument("--run-id", default=None, help="Optional run id to record in the export manifest")
    return parser.parse_args()


def export_public_tables(database_url: str, output_dir: Path, run_id: str | None) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    engine = create_engine(database_url, pool_pre_ping=True)
    inspector = inspect(engine)
    table_names = sorted(inspector.get_table_names(schema="public"))

    manifest: dict[str, Any] = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "database_url": make_url(database_url).render_as_string(hide_password=True),
        "schema": "public",
        "tables": [],
    }

    if not table_names:
        manifest_path = output_dir / "export_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest

    with engine.connect() as connection:
        for table_name in table_names:
            query = text(f'SELECT * FROM public."{table_name}"')
            dataframe = pd.read_sql_query(query, connection)

            output_path = output_dir / f"{table_name}.parquet"
            dataframe.to_parquet(output_path, index=False)

            manifest["tables"].append(
                {
                    "table": table_name,
                    "rows": int(len(dataframe.index)),
                    "columns": list(map(str, dataframe.columns.tolist())),
                    "dtypes": {column: str(dtype) for column, dtype in dataframe.dtypes.items()},
                    "file": output_path.name,
                }
            )

    manifest_path = output_dir / "export_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)

    try:
        manifest = export_public_tables(
            database_url=args.database_url,
            output_dir=output_dir,
            run_id=args.run_id,
        )
    except Exception as exc:
        print(f"Error: failed to export database to Parquet: {exc}", file=sys.stderr)
        return 1

    table_count = len(manifest["tables"])
    print(f"Exported {table_count} table(s) to {output_dir}")
    for table in manifest["tables"]:
        print(f"  - {table['table']}: {table['rows']} row(s) -> {table['file']}")
    if table_count == 0:
        print("  No tables found in public schema.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
