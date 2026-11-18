import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

DB_DSN = os.getenv(
    "DB_DSN",
    "postgresql+asyncpg://user:password@postgres:5432/app",
)

engine = create_async_engine(DB_DSN, echo=False, pool_pre_ping=True)
Session = async_sessionmaker(engine, expire_on_commit=False)