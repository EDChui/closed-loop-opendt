from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker


def build_engine(database_url: str, echo: bool = False) -> Engine:
    return create_engine(database_url, echo=echo, pool_pre_ping=True)

def build_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

def test_connection(engine: Engine) -> bool:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except Exception as e:
        return False
