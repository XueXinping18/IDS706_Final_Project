"""
测试数据库模块

运行方式：
    python scripts/test_database.py
"""
import asyncio
import os
import sys

# 添加 src 到 path（如果还没安装包）
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from dotenv import load_dotenv
load_dotenv()  # 这行很重要！

from ingestion_worker.infrastructure.database import Database, DatabaseError
from ingestion_worker.utils.logging import setup_logging, get_logger


async def test_database():
    """测试数据库所有功能"""
    logger = get_logger(__name__)

    # 从环境变量读取数据库 URL
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("❌ 请设置 DATABASE_URL 环境变量")
        logger.info("示例: export DATABASE_URL='postgresql://user:pass@localhost:5432/testdb'")
        return False

    logger.info("=" * 60)
    logger.info("开始测试数据库模块")
    logger.info("=" * 60)

    db = Database(db_url, pool_size=5)

    try:
        # ========== 测试 1: 连接 ==========
        logger.info("\n测试 1: 创建连接池")
        await db.connect()
        logger.info("✓ 连接池创建成功")

        # ========== 测试 2: 简单查询 ==========
        logger.info("\n测试 2: 简单查询")
        result = await db.fetch_one("SELECT 1 as num, 'hello' as text")
        assert result is not None
        assert result['num'] == 1
        assert result['text'] == 'hello'
        logger.info(f"✓ 查询结果: num={result['num']}, text={result['text']}")

        # ========== 测试 3: 创建测试表 ==========
        logger.info("\n测试 3: 创建测试表")
        await db.execute("""
                         DROP TABLE IF EXISTS test_ingestion_worker
                         """)
        await db.execute("""
                         CREATE TABLE test_ingestion_worker
                         (
                             id         SERIAL PRIMARY KEY,
                             name       TEXT NOT NULL,
                             value      INTEGER,
                             created_at TIMESTAMPTZ DEFAULT NOW()
                         )
                         """)
        logger.info("✓ 测试表创建成功")

        # ========== 测试 4: 插入数据 ==========
        logger.info("\n测试 4: 插入数据")
        await db.execute(
            "INSERT INTO test_ingestion_worker (name, value) VALUES ($1, $2)",
            "test1", 100
        )
        await db.execute(
            "INSERT INTO test_ingestion_worker (name, value) VALUES ($1, $2)",
            "test2", 200
        )
        logger.info("✓ 插入 2 条数据成功")

        # ========== 测试 5: 查询多条 ==========
        logger.info("\n测试 5: 查询多条记录")
        rows = await db.fetch_all("SELECT * FROM test_ingestion_worker ORDER BY id")
        assert len(rows) == 2
        assert rows[0]['name'] == 'test1'
        assert rows[0]['value'] == 100
        assert rows[1]['name'] == 'test2'
        assert rows[1]['value'] == 200
        logger.info(f"✓ 查询到 {len(rows)} 条记录")
        for row in rows:
            logger.info(f"  - id={row['id']}, name={row['name']}, value={row['value']}")

        # ========== 测试 6: 更新数据 ==========
        logger.info("\n测试 6: 更新数据")
        await db.execute(
            "UPDATE test_ingestion_worker SET value = $1 WHERE name = $2",
            999, "test1"
        )
        updated = await db.fetch_one(
            "SELECT value FROM test_ingestion_worker WHERE name = $1",
            "test1"
        )
        assert updated['value'] == 999
        logger.info("✓ 更新成功，新值: 999")

        # ========== 测试 7: 事务（提交）==========
        logger.info("\n测试 7: 事务提交")
        async with db.transaction() as conn:
            await conn.execute(
                "INSERT INTO test_ingestion_worker (name, value) VALUES ($1, $2)",
                "test3", 300
            )
            await conn.execute(
                "INSERT INTO test_ingestion_worker (name, value) VALUES ($1, $2)",
                "test4", 400
            )

        count = await db.fetch_one("SELECT COUNT(*) as cnt FROM test_ingestion_worker")
        assert count['cnt'] == 4
        logger.info(f"✓ 事务提交成功，总记录数: {count['cnt']}")

        # ========== 测试 8: 事务（回滚）==========
        logger.info("\n测试 8: 事务回滚")
        try:
            async with db.transaction() as conn:
                await conn.execute(
                    "INSERT INTO test_ingestion_worker (name, value) VALUES ($1, $2)",
                    "test5", 500
                )
                # 故意抛出异常触发回滚
                raise ValueError("测试回滚")
        except (DatabaseError, ValueError):
            pass

        count = await db.fetch_one("SELECT COUNT(*) as cnt FROM test_ingestion_worker")
        assert count['cnt'] == 4  # 应该还是 4，test5 没插入
        logger.info(f"✓ 事务回滚成功，总记录数仍为: {count['cnt']}")

        # ========== 测试 9: 约束冲突 ==========
        logger.info("\n测试 9: 唯一约束冲突")
        await db.execute("""
                         ALTER TABLE test_ingestion_worker
                             ADD CONSTRAINT unique_name UNIQUE (name)
                         """)

        try:
            await db.execute(
                "INSERT INTO test_ingestion_worker (name, value) VALUES ($1, $2)",
                "test1", 111  # test1 已存在
            )
            logger.error("✗ 应该抛出唯一约束异常")
            return False
        except DatabaseError as e:
            assert "Unique constraint" in str(e)
            logger.info(f"✓ 正确捕获唯一约束异常: {e}")

        # ========== 清理 ==========
        logger.info("\n清理测试数据")
        await db.execute("DROP TABLE test_ingestion_worker")
        logger.info("✓ 测试表已删除")

        # ========== 测试 10: 关闭连接 ==========
        logger.info("\n测试 10: 关闭连接池")
        await db.close()
        logger.info("✓ 连接池已关闭")

        logger.info("\n" + "=" * 60)
        logger.info("✅ 所有测试通过！")
        logger.info("=" * 60)
        return True

    except Exception as e:
        logger.error(f"\n❌ 测试失败: {e}", exc_info=True)
        return False
    finally:
        if db.pool:
            await db.close()


if __name__ == "__main__":
    # 配置日志
    setup_logging(level="INFO")

    # 运行测试
    success = asyncio.run(test_database())

    # 退出码
    sys.exit(0 if success else 1)