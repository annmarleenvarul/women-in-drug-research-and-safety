"""Configuration for the warehouse connection.

All values come from environment variables (see .env.example); nothing is hard-coded.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class WarehouseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "WarehouseConfig":
        try:
            user = os.environ["WAREHOUSE_USER"]
            password = os.environ["WAREHOUSE_PASSWORD"]
        except KeyError as exc:
            raise RuntimeError(
                f"Missing environment variable {exc.args[0]}. "
                "Copy .env.example to .env and fill it in."
            ) from exc

        return cls(
            host=os.getenv("WAREHOUSE_HOST", "localhost"),
            port=int(os.getenv("WAREHOUSE_PORT", "5432")),
            dbname=os.getenv("WAREHOUSE_DB", "warehouse"),
            user=user,
            password=password,
        )

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )
