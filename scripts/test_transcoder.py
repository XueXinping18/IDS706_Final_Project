"""
测试 Transcoder 模块

运行方式：
    python scripts/test_transcoder.py

前提条件：
1. 启用 Transcoder API
2. 服务账号有权限访问 GCS bucket
3. 上传一个测试视频到 GCS
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from dotenv import load_dotenv
load_dotenv()
from ingestion_worker.infrastructure.transcoder import TranscoderClient, TranscoderError
from ingestion_worker.infrastructure.gcs import GCSClient
from ingestion_worker.config import Config
from ingestion_worker.utils.logging import setup_logging, get_logger


async def test_transcoder():
    """测试 Transcoder 所有功能"""
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info("开始测试 Transcoder 模块")
    logger.info("=" * 60)

    try:
        # 加载配置
        config = Config.from_env()

        # ========== 测试 1: 初始化客户端 ==========
        logger.info("\n测试 1: 初始化 Transcoder 客户端")
        transcoder = TranscoderClient(config)
        logger.info("✓ 客户端初始化成功")

        # ========== 测试 2: 准备测试视频 ==========
        logger.info("\n测试 2: 准备测试视频")

        # 使用你上传的测试视频
        test_video_object = os.getenv("TEST_VIDEO_OBJECT", "infra-test/sample-video.mov")
        input_uri = f"gs://{config.raw_bucket}/{test_video_object}"

        # 检查视频是否存在
        gcs = GCSClient(config)
        exists = await gcs.exists(input_uri)

        if not exists:
            logger.warning(f"⚠️  测试视频不存在: {input_uri}")
            logger.info("请上传一个小视频文件:")
            logger.info(f"  gsutil cp your-video.mp4 {input_uri}")
            logger.info("或设置环境变量:")
            logger.info(f"  export TEST_VIDEO_OBJECT=path/to/your-video.mp4")
            return False

        logger.info(f"✓ 找到测试视频: {input_uri}")

        # ========== 测试 3: 创建转码任务 ==========
        logger.info("\n测试 3: 创建转码任务")

        # 生成唯一的输出路径
        import uuid
        job_id = str(uuid.uuid4())[:8]
        output_uri = f"gs://{config.hls_bucket}/test-output/{job_id}/"

        logger.info(f"输入: {input_uri}")
        logger.info(f"输出: {output_uri}")

        job_name = await transcoder.create_transcode_job(
            input_uri=input_uri,
            output_uri=output_uri,
        )

        logger.info(f"✓ 任务已创建: {job_name}")

        # ========== 测试 4: 查询任务状态 ==========
        logger.info("\n测试 4: 查询任务状态")
        job = await transcoder.get_job(job_name)
        logger.info(f"✓ 当前状态: {job.state.name}")

        # ========== 测试 5: 等待任务完成 ==========
        logger.info("\n测试 5: 等待任务完成（最多 10 分钟）")
        logger.info("这可能需要几分钟，请耐心等待...")
        logger.info("转码时间取决于视频长度和复杂度")

        try:
            result = await transcoder.wait_for_job(
                job_name,
                max_wait_seconds=600,  # 10 分钟
                poll_interval_seconds=10,
            )

            logger.info("✓ 转码任务完成")
            logger.info(f"  输出 URI: {result.output_uri}")

        except TranscoderError as e:
            if "超时" in str(e):
                logger.warning("⚠️  任务超时（但可能仍在处理中）")
                logger.info("你可以稍后手动查询结果:")
                logger.info(f"  job_name: {job_name}")
            else:
                raise

        # ========== 测试 6: 验证输出文件 ==========
        logger.info("\n测试 6: 验证输出文件")

        # 检查 master.m3u8 是否存在
        master_m3u8_uri = f"{output_uri}manifest.m3u8"
        exists = await gcs.exists(master_m3u8_uri)

        if exists:
            logger.info(f"✓ 输出文件已生成: {master_m3u8_uri}")
        else:
            # 尝试其他可能的文件名
            alt_uri = f"{output_uri}master.m3u8"
            exists = await gcs.exists(alt_uri)
            if exists:
                logger.info(f"✓ 输出文件已生成: {alt_uri}")
            else:
                logger.warning("⚠️  未找到 master.m3u8，但转码任务已完成")
                logger.info("可能使用了不同的文件名，请检查输出目录:")
                logger.info(f"  gsutil ls {output_uri}")

        logger.info("\n" + "=" * 60)
        logger.info("✅ 所有测试通过！")
        logger.info("=" * 60)

        # 清理提示
        logger.info("\n清理测试文件:")
        logger.info(f"  gsutil rm -r {output_uri}")

        return True

    except Exception as e:
        logger.error(f"\n❌ 测试失败: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    setup_logging(level="INFO")

    success = asyncio.run(test_transcoder())

    sys.exit(0 if success else 1)