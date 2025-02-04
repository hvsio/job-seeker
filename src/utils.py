import logging
import os

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import create_database, database_exists
from contextlib import contextmanager


@contextmanager
def get_db_connection() -> (Engine, Connection):
    """First creation of job database based on .env values.
    As a context manager allow to execute statements with created engine and connection
    and ensures disposal of resources afterwards.

    Returns:
        Engine: database engine
        Connection: database connection
    """
    load_dotenv(find_dotenv())

    db_url = (
        f"postgresql://{os.environ.get('db_user')}"
        f":{os.environ.get('db_pass')}"
        f"@{os.environ.get('db_host')}"
        f":{str(os.environ.get('db_port'))}"
        f"/{os.environ.get('tablename')}"
    )

    print(db_url)

    if not database_exists(db_url):
        create_database(db_url)

    try:
        engine = create_engine(db_url, echo=False)
        conn = engine.connect()
        yield engine, conn
    except SQLAlchemyError | ValueError as e:
        logging.error(
            f'Encountered error when establishing db environment: {e}'
        )
    finally:
        conn.close()
        engine.dispose()
