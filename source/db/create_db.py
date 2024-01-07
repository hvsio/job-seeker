import logging
import os

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema
from utils import get_db_connection

from models import *

logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger('DBprep')

if __name__ == '__main__':
    load_dotenv(find_dotenv())

    engine, conn = get_db_connection()
    logger.info(f'Established connection at {engine}')

    try:
        schema_name = os.environ.get('schema_name')
        tablename = os.environ.get('tablename')

        if not conn.dialect.has_schema(conn, schema_name):
            logger.info(f'Creating dedicated schema')
            conn.execute(CreateSchema(schema_name))
        Base.metadata.create_all(bind=engine)
        # sequence needed for AUTOINCREMENT id in PostgreSQL
        conn.execute(
            text(
                f"""
                        CREATE SEQUENCE seq_jobbb START 1;
                        ALTER TABLE {schema_name}.{tablename} ALTER COLUMN id SET DEFAULT nextval('seq_jobbb');
                        """
            )
        )
    except SQLAlchemyError as e:
        logger.error(f'Encountered error when establishing schema: {e}')
