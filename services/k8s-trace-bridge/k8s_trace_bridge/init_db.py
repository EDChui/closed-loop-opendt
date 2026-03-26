import os
import time

from k8s_observability.persistence import Base, build_engine, test_connection
from k8s_observability.persistence.sqlachemy.tables import * # noqa: F401


def main() -> None:
    database_url = os.environ["DATABASE_URL"]
    engine = build_engine(database_url)

    for _ in range(30):
        if test_connection(engine):
            break
        time.sleep(2)
    else:
        raise RuntimeError("PostgreSQL did not become ready in time")

    Base.metadata.create_all(bind=engine)
    print("Database schema initialized")


if __name__ == "__main__":
    main()
