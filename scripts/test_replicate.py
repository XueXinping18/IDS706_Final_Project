"""
测试 Replicate 模块

运行方式：
    python scripts/test_replicate.py

前提条件：
1. 设置 REPLICATE_API_TOKEN 环境变量
2. 准备一个短音频文件（10秒以内）上传到 GCS
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from dotenv import load_dotenv
load_dotenv()


from ingestion_worker.infrastructure.replicate import ReplicateClient, ReplicateError
from ingestion_worker.infrastructure.gcs import GCSClient
from ingestion_worker.config import Config
from ingestion_worker.utils.logging import setup_logging, get_logger


async def test_replicate():
    """测试 Replicate 所有功能"""
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info("开始测试 Replicate 模块")
    logger.info("=" * 60)

    # 检查 API token
    api_token = os.getenv("REPLICATE_API_TOKEN")
    if not api_token or api_token.startswith("r8_test"):
        logger.error("❌ 请设置有效的 REPLICATE_API_TOKEN")
        logger.info("获取 token: https://replicate.com/account/api-tokens")
        return False

    try:
        # 加载配置
        config = Config.from_env()

        # ========== 测试 1: 初始化客户端 ==========
        logger.info("\n测试 1: 初始化 Replicate 客户端")
        replicate = ReplicateClient(config)
        logger.info("✓ 客户端初始化成功")

        # ========== 测试 2: 准备测试音频 ==========
        logger.info("\n测试 2: 准备测试音频")
        logger.info("我们将使用一个公开的示例音频 URL")

        # 使用 Replicate 提供的示例音频
        test_audio_url = "https://replicate.delivery/pbxt/KJJWJjLGN1JMdvMNF4PT7RzxQjqWAM5xKPeJJPzCNQoGz4SA/audio.mp3"
        logger.info(f"测试音频: {test_audio_url}")

        # ========== 测试 3: 提交任务 ==========
        logger.info("\n测试 3: 提交 WhisperX 任务")

        input_data = {
            "audio": test_audio_url,
            "language": "en",
            "align_output": True,
        }

        prediction_id = await replicate.submit_prediction(
            model=ReplicateClient.DEFAULT_MODEL,
            input_data=input_data,
        )

        logger.info(f"✓ 任务已提交: {prediction_id}")

        # ========== 测试 4: 查询状态 ==========
        logger.info("\n测试 4: 查询任务状态")
        prediction = await replicate.get_prediction(prediction_id)
        logger.info(f"✓ 当前状态: {prediction['status']}")

        # ========== 测试 5: 等待完成 ==========
        logger.info("\n测试 5: 等待任务完成（最多 5 分钟）")
        logger.info("这可能需要 1-3 分钟，请耐心等待...")

        try:
            result = await replicate.wait_for_prediction(
                prediction_id,
                max_wait_seconds=300,  # 5 分钟
                poll_interval_seconds=5,
            )

            logger.info("✓ 任务完成")

            # 检查输出
            output = result.get("output")
            if output:
                logger.info(f"✓ 输出内容: {str(output)[:200]}...")
            else:
                logger.warning("⚠️  没有输出内容")

        except ReplicateError as e:
            if "任务超时" in str(e):
                logger.warning("⚠️  任务超时（但可能仍在处理中）")
                logger.info("你可以稍后手动查询结果:")
                logger.info(f"  prediction_id: {prediction_id}")
            else:
                raise

        logger.info("\n" + "=" * 60)
        logger.info("✅ 所有测试通过！")
        logger.info("=" * 60)
        return True

    except Exception as e:
        logger.error(f"\n❌ 测试失败: {e}", exc_info=True)
        return False


async def test_replicate_with_gcs():
    """测试 Replicate + GCS 集成（Signed URL）"""
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info("测试 Replicate + GCS 集成")
    logger.info("=" * 60)

    try:
        config = Config.from_env()
        replicate = ReplicateClient(config)
        gcs = GCSClient(config)

        # 准备测试音频（需要你提前上传一个小音频文件）
        test_bucket = config.raw_bucket
        test_audio = "test/sample-audio.mp3"  # 你需要上传这个文件

        # 检查文件是否存在
        audio_uri = f"gs://{test_bucket}/{test_audio}"
        exists = await gcs.exists(audio_uri)

        if not exists:
            logger.warning(f"⚠️  测试音频不存在: {audio_uri}")
            logger.info("请上传一个小音频文件（10秒以内）:")
            logger.info(f"  gsutil cp your-audio.mp3 {audio_uri}")
            return False

        logger.info(f"✓ 找到测试音频: {audio_uri}")

        # 生成 Signed URLs
        logger.info("\n生成 Signed URLs...")

        audio_get_url = gcs.generate_signed_url(
            bucket=test_bucket,
            object_name=test_audio,
            method="GET",
            ttl_seconds=3600,  # 1 小时
        )
        logger.info("✓ Audio GET URL 已生成")

        vtt_put_url = gcs.generate_signed_url(
            bucket=config.transcript_bucket,
            object_name="test/output.vtt",
            method="PUT",
            content_type="text/vtt",
            ttl_seconds=3600,
        )
        logger.info("✓ VTT PUT URL 已生成")

        json_put_url = gcs.generate_signed_url(
            bucket=config.transcript_bucket,
            object_name="test/output.json",
            method="PUT",
            content_type="application/json",
            ttl_seconds=3600,
        )
        logger.info("✓ JSON PUT URL 已生成")

        # 提交任务
        logger.info("\n提交 WhisperX 任务...")
        prediction_id = await replicate.submit_whisperx(
            audio_url=audio_get_url,
            vtt_put_url=vtt_put_url,
            json_put_url=json_put_url,
            language="en",
        )

        logger.info(f"✓ 任务已提交: {prediction_id}")

        # 等待完成
        logger.info("\n等待任务完成...")
        result = await replicate.wait_for_prediction(
            prediction_id,
            max_wait_seconds=300,
        )

        logger.info("✓ 任务完成")

        # 验证输出文件
        logger.info("\n验证输出文件...")

        vtt_exists = await gcs.exists(f"gs://{config.transcript_bucket}/test/output.vtt")
        json_exists = await gcs.exists(f"gs://{config.transcript_bucket}/test/output.json")

        if vtt_exists:
            logger.info("✓ VTT 文件已生成")
        else:
            logger.warning("⚠️  VTT 文件未找到")

        if json_exists:
            logger.info("✓ JSON 文件已生成")
            # 读取并显示部分内容
            json_content = await gcs.read_json(f"gs://{config.transcript_bucket}/test/output.json")
            logger.info(f"  JSON 预览: {str(json_content)[:200]}...")
        else:
            logger.warning("⚠️  JSON 文件未找到")

        logger.info("\n" + "=" * 60)
        logger.info("✅ 集成测试通过！")
        logger.info("=" * 60)
        return True

    except Exception as e:
        logger.error(f"\n❌ 测试失败: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    setup_logging(level="INFO")

    logger = get_logger(__name__)

    # 选择测试模式
    mode = os.getenv("TEST_MODE", "simple")

    if mode == "gcs":
        logger.info("运行 GCS 集成测试模式")
        success = asyncio.run(test_replicate_with_gcs())
    else:
        logger.info("运行简单测试模式（使用公开音频）")
        success = asyncio.run(test_replicate())

    sys.exit(0 if success else 1)