"""
职责：
- 管理 asyncpg 连接池
- 提供基础 CRUD 操作
- 事务管理

依赖：asyncpg

对外接口：
- async def execute(query, *args) -> Any
- async def fetch_one(query, *args) -> Record | None
- async def fetch_all(query, *args) -> list[Record]
- async def transaction() -> AsyncContextManager

设计：
- 使用 asyncpg.Pool（在 __main__.py 初始化时创建）
- 避免 ORM（保持轻量）
"""
import asyncpg
from typing import Any, Optional, AsyncContextManager
from contextlib import asynccontextmanager

from ingestion_worker.utils.logging import get_logger


class DatabaseError(Exception):
    """数据库操作错误"""
    pass


class Database:
    """异步 Postgres 数据库客户端"""

    def __init__(self, db_url: str, pool_size: int = 10):
        """
        初始化数据库客户端

        Args:
            db_url: 数据库连接字符串 (postgresql://user:pass@host:port/db)
            pool_size: 连接池大小
        """
        self.db_url = db_url
        self.pool_size = pool_size
        self.pool: Optional[asyncpg.Pool] = None
        self.logger = get_logger(__name__)

    async def connect(self) -> None:
        """
        创建连接池

        Raises:
            DatabaseError: 连接失败
        """
        if self.pool is not None:
            self.logger.warning("连接池已存在，跳过创建")
            return

        try:
            self.logger.info(f"正在创建数据库连接池 (size={self.pool_size})...")
            self.pool = await asyncpg.create_pool(
                self.db_url,
                min_size=1,
                max_size=self.pool_size,
                command_timeout=60,  # 命令超时 60 秒
            )
            self.logger.info("✓ 数据库连接池创建成功")
        except Exception as e:
            self.logger.error(f"✗ 数据库连接失败: {e}")
            raise DatabaseError(f"Failed to connect to database: {e}") from e

    async def close(self) -> None:
        """关闭连接池"""
        if self.pool is None:
            return

        self.logger.info("正在关闭数据库连接池...")
        await self.pool.close()
        self.pool = None
        self.logger.info("✓ 数据库连接池已关闭")

    def _ensure_pool(self) -> asyncpg.Pool:
        """确保连接池已创建"""
        if self.pool is None:
            raise DatabaseError("Database pool not initialized. Call connect() first.")
        return self.pool

    async def execute(self, query: str, *args: Any) -> str:
        """
        执行 INSERT/UPDATE/DELETE 语句

        Args:
            query: SQL 语句
            *args: 查询参数

        Returns:
            执行结果状态 (如 "INSERT 0 1")

        Raises:
            DatabaseError: 执行失败
        """
        pool = self._ensure_pool()

        try:
            async with pool.acquire() as conn:
                result = await conn.execute(query, *args)
                return result
        except asyncpg.UniqueViolationError as e:
            self.logger.warning(f"唯一约束冲突: {e}")
            raise DatabaseError(f"Unique constraint violation: {e}") from e
        except asyncpg.ForeignKeyViolationError as e:
            self.logger.warning(f"外键约束冲突: {e}")
            raise DatabaseError(f"Foreign key violation: {e}") from e
        except asyncpg.PostgresError as e:
            self.logger.error(f"数据库错误: {e}")
            raise DatabaseError(f"Database error: {e}") from e
        except Exception as e:
            self.logger.error(f"未预期错误: {e}")
            raise DatabaseError(f"Unexpected error: {e}") from e

    async def fetch_one(self, query: str, *args: Any) -> Optional[asyncpg.Record]:
        """
        查询单条记录

        Args:
            query: SQL 语句
            *args: 查询参数

        Returns:
            单条记录或 None

        Raises:
            DatabaseError: 查询失败
        """
        pool = self._ensure_pool()

        try:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(query, *args)
                return result
        except asyncpg.PostgresError as e:
            self.logger.error(f"查询错误: {e}")
            raise DatabaseError(f"Query error: {e}") from e
        except Exception as e:
            self.logger.error(f"未预期错误: {e}")
            raise DatabaseError(f"Unexpected error: {e}") from e

    async def fetch_all(self, query: str, *args: Any) -> list[asyncpg.Record]:
        """
        查询多条记录

        Args:
            query: SQL 语句
            *args: 查询参数

        Returns:
            记录列表（可能为空）

        Raises:
            DatabaseError: 查询失败
        """
        pool = self._ensure_pool()

        try:
            async with pool.acquire() as conn:
                results = await conn.fetch(query, *args)
                return results
        except asyncpg.PostgresError as e:
            self.logger.error(f"查询错误: {e}")
            raise DatabaseError(f"Query error: {e}") from e
        except Exception as e:
            self.logger.error(f"未预期错误: {e}")
            raise DatabaseError(f"Unexpected error: {e}") from e

    @asynccontextmanager
    async def transaction(self) -> AsyncContextManager[asyncpg.Connection]:
        """
        事务上下文管理器

        使用示例:
            async with db.transaction() as conn:
                await conn.execute("INSERT INTO ...")
                await conn.execute("UPDATE ...")
            # 自动提交或回滚

        Yields:
            数据库连接

        Raises:
            DatabaseError: 事务执行失败
        """
        pool = self._ensure_pool()

        async with pool.acquire() as conn:
            async with conn.transaction():
                try:
                    yield conn
                except Exception as e:
                    self.logger.error(f"事务回滚: {e}")
                    raise DatabaseError(f"Transaction failed: {e}") from e