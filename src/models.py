import os

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import DateTime, Integer, MetaData, Sequence, String, Text
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from datetime import datetime

load_dotenv(find_dotenv())

metadata = MetaData(schema=os.environ.get('schema_name'))
Base = declarative_base(metadata=metadata)


class Job(Base):
    """Tablename defnition for jobs acquired in initial Airflow tasks."""

    __tablename__ = os.environ.get('tablename')

    id: Mapped[int] = mapped_column(
        Integer,
        Sequence('sequence_job', start=1, increment=1),
        primary_key=True,
        autoincrement=True,
    )
    job_title: Mapped[str] = mapped_column(String(255))
    company: Mapped[str] = mapped_column(String(255))
    link: Mapped[str] = mapped_column(Text, nullable=True)
    type: Mapped[str] = mapped_column(String(255), nullable=True)
    region: Mapped[str] = mapped_column(String(255))
    salary: Mapped[int] = mapped_column(Integer, nullable=True)
    date: Mapped[datetime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        return f'Job(id={self.id}), job_title={self.job_title}'
