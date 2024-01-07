import logging
import os

from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import create_database, database_exists


def get_db_connection() -> (Engine, Connection):
    """First creation of base-recommender database.

    Args:
        config (dict): yaml config

    Returns:
        Connection: database connection
    """
    load_dotenv(find_dotenv())

    db_url = f"postgresql://{os.environ.get('db_user')}:{os.environ.get('db_pass')}@{os.environ.get('db_host')}:{str(os.environ.get('db_port'))}/{os.environ.get('tablename')}"
    if not database_exists(db_url):
        create_database(db_url)

    try:
        engine = create_engine(
            db_url, echo=False, isolation_level='AUTOCOMMIT'
        )
        conn = engine.connect()
        return engine, conn
    except SQLAlchemyError as e:
        logging.error(
            f'Encountered error when establishing db environment: {e}'
        )