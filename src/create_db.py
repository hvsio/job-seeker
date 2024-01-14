import logging
import os

from dotenv import find_dotenv, load_dotenv
from models import Base
from sqlalchemy import text
from sqlalchemy.schema import CreateSchema
from utils import get_db_connection

logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger('DBprep')

if __name__ == '__main__':
    load_dotenv(find_dotenv())

    with get_db_connection() as (engine, conn):
        logger.info(f'Established connection with db engine: {engine}')
        schema_name = os.environ.get(
            'schema_name'
        )   # defaults or check if they are not none
        tablename = os.environ.get('tablename')

        if not schema_name or tablename:
            raise ValueError('Missing DB values.')

        if not conn.dialect.has_schema(conn, schema_name):
            logger.info(f'Creating dedicated schema')
            conn.execute(CreateSchema(schema_name))
            Base.metadata.create_all(bind=engine)
            # sequence needed for AUTOINCREMENT id in PostgreSQL
            conn.execute(
                text(
                    f"""
                    CREATE SEQUENCE sequence_job START 1;
                    ALTER TABLE {schema_name}.{tablename} ALTER COLUMN id SET DEFAULT nextval('sequence_job');
                    """
                )
            )
