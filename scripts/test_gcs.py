"""
测试 GCS 模块

运行方式：
    python scripts/test_gcs.py

前提条件：
1. 设置 GCP 认证：
   - gcloud auth application-default login
   或
   - export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

2. 创建测试 bucket 并上传测试文件：
   gsutil mb gs://test-ingestion-worker-bucket
   echo "Hello, World!" > /tmp/test-sample.txt
   gsutil cp /tmp/test-sample.txt gs://test-ingestion-worker-bucket/test/sample.txt
"""
import asyncio
import os
import sys
import tempfile

from dotenv import load_dotenv
load_dotenv()  # 这行很重要！

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ingestion_worker.infrastructure.gcs import GCSClient, GCSError
from ingestion_worker.config import Config
from ingestion_worker.utils.logging import setup_logging, get_logger


async def test_gcs():
    """测试 GCS 所有功能"""
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info("开始测试 GCS 模块")
    logger.info("=" * 60)

    # 准备配置
    try:
        config = Config.from_env()
    except Exception as e:
        logger.error(f"❌ 配置加载失败: {e}")
        logger.info("请确保设置了所有必需的环境变量")
        return False

    # 使用配置中的 bucket（或单独指定测试 bucket）
    test_bucket = os.getenv("TEST_GCS_BUCKET", config.raw_bucket)
    test_object = "infra-test/sample.txt"
    test_uri = f"gs://{test_bucket}/{test_object}"

    logger.info(f"测试 Bucket: {test_bucket}")
    logger.info(f"测试对象: {test_object}")

    try:
        # ========== 测试 1: 初始化客户端 ==========
        logger.info("\n测试 1: 初始化 GCS 客户端")
        gcs = GCSClient(config)
        logger.info("✓ GCS 客户端初始化成功")

        # ========== 测试 2: 检查对象是否存在 ==========
        logger.info("\n测试 2: 检查对象是否存在")
        exists = await gcs.exists(test_uri)
        if not exists:
            logger.warning(f"⚠️  测试对象不存在: {test_uri}")
            logger.info("请先创建测试文件：")
            logger.info(f"  echo 'Hello, World!' > /tmp/test-sample.txt")
            logger.info(f"  gsutil cp /tmp/test-sample.txt {test_uri}")
            return False
        logger.info(f"✓ 对象存在: {test_uri}")

        # ========== 测试 3: 生成 Signed GET URL ==========
        logger.info("\n测试 3: 生成 Signed GET URL")
        get_url = gcs.generate_signed_url(
            bucket=test_bucket,
            object_name=test_object,
            method="GET",
            ttl_seconds=300
        )
        logger.info(f"✓ Signed GET URL: {get_url[:80]}...")
        logger.info("  你可以在浏览器中访问此 URL 验证（5 分钟内有效）")

        # ========== 测试 4: 读取对象内容（文本）==========
        logger.info("\n测试 4: 读取对象内容（文本）")
        content = await gcs.read_text(test_uri)
        logger.info(f"✓ 读取成功，内容长度: {len(content)} 字符")
        logger.info(f"  内容预览: {content[:100]}...")

        # ========== 测试 5: 生成 Signed PUT URL ==========
        logger.info("\n测试 5: 生成 Signed PUT URL")
        put_object = "infra-test/uploaded-via-signed-url.txt"
        put_url = gcs.generate_signed_url(
            bucket=test_bucket,
            object_name=put_object,
            method="PUT",
            content_type="text/plain",
            ttl_seconds=300
        )
        logger.info(f"✓ Signed PUT URL: {put_url[:80]}...")

        # 用 curl 模拟上传
        logger.info("  模拟上传...")
        import aiohttp
        test_content = "This is uploaded via signed URL!\n"
        async with aiohttp.ClientSession() as session:
            async with session.put(
                    put_url,
                    data=test_content,
                    headers={"Content-Type": "text/plain"}
            ) as resp:
                if resp.status in (200, 201):
                    logger.info(f"✓ 上传成功: gs://{test_bucket}/{put_object}")
                else:
                    logger.error(f"✗ 上传失败: HTTP {resp.status}")
                    return False

        # 验证上传的内容
        uploaded_uri = f"gs://{test_bucket}/{put_object}"
        uploaded_content = await gcs.read_text(uploaded_uri)
        assert uploaded_content == test_content
        logger.info(f"✓ 验证上传内容成功")

        # ========== 测试 6: 读取 JSON 对象 ==========
        logger.info("\n测试 6: 读取 JSON 对象")

        # 先上传一个 JSON 对象
        json_object = "infra-test/sample.json"
        json_uri = f"gs://{test_bucket}/{json_object}"
        json_content = '{"key": "value", "number": 42}'

        json_put_url = gcs.generate_signed_url(
            bucket=test_bucket,
            object_name=json_object,
            method="PUT",
            content_type="application/json",
            ttl_seconds=300
        )

        async with aiohttp.ClientSession() as session:
            async with session.put(
                    json_put_url,
                    data=json_content,
                    headers={"Content-Type": "application/json"}
            ) as resp:
                if resp.status not in (200, 201):
                    logger.error(f"✗ JSON 上传失败: HTTP {resp.status}")
                    return False

        # 读取并解析 JSON
        json_data = await gcs.read_json(json_uri)
        assert json_data["key"] == "value"
        assert json_data["number"] == 42
        logger.info(f"✓ JSON 读取成功: {json_data}")

        # ========== 测试 7: parse_uri ==========
        logger.info("\n测试 7: 解析 GCS URI")
        bucket, object_name = gcs.parse_uri(test_uri)
        assert bucket == test_bucket
        assert object_name == test_object
        logger.info(f"✓ URI 解析成功: bucket={bucket}, object={object_name}")

        # ========== 测试 8: 检查不存在的对象 ==========
        logger.info("\n测试 8: 检查不存在的对象")
        fake_uri = f"gs://{test_bucket}/this/does/not/exist.txt"
        exists = await gcs.exists(fake_uri)
        assert not exists
        logger.info(f"✓ 正确返回不存在: {fake_uri}")

        # ========== 测试 9: 读取不存在的对象（应该抛异常）==========
        logger.info("\n测试 9: 读取不存在的对象")
        try:
            await gcs.read_text(fake_uri)
            logger.error("✗ 应该抛出 GCSError")
            return False
        except GCSError as e:
            assert "not found" in str(e).lower()
            logger.info(f"✓ 正确抛出异常: {e}")

        # ========== 清理测试文件 ==========
        logger.info("\n清理测试文件")
        logger.info(f"  上传的测试文件保留在: gs://{test_bucket}/test/")
        logger.info(f"  如需清理，运行: gsutil rm -r gs://{test_bucket}/test/")

        logger.info("\n" + "=" * 60)
        logger.info("✅ 所有测试通过！")
        logger.info("=" * 60)
        return True

    except Exception as e:
        logger.error(f"\n❌ 测试失败: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    # 配置日志
    setup_logging(level="INFO")

    # 运行测试
    success = asyncio.run(test_gcs())

    # 退出码
    sys.exit(0 if success else 1)