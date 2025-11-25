"""
职责：
- 编排转码流程（提交 → 等待 → 返回结果）
- 处理转码失败（重试/降级）
- 记录日志和指标

对外接口：
- async def transcode_video(
    video_uid: str,
    input_object_name: str
  ) -> str | None  # hls_path or None if failed

业务逻辑：
- 生成 output_uri
- 调用 transcoder.create_transcode_job
- 等待完成
- 失败时记录日志，返回 None（转码不致命）

依赖：infrastructure.transcoder, infrastructure.gcs

"""

import asyncio
from typing import Optional

from ingestion_worker.config import Config
from ingestion_worker.infrastructure.transcoder import TranscoderClient, TranscoderError
from ingestion_worker.infrastructure.gcs import GCSClient
from ingestion_worker.types import TranscodeResult
from ingestion_worker.errors import TranscodingError
from ingestion_worker.utils.logging import get_logger


class TranscodingService:
    """转码服务（业务层）"""

    def __init__(
        self,
        transcoder: TranscoderClient,
        gcs: GCSClient,
        config: Config
    ):
        """
        初始化转码服务

        Args:
            transcoder: Transcoder API 客户端
            gcs: GCS 客户端
            config: 系统配置
        """
        self.transcoder = transcoder
        self.gcs = gcs
        self.config = config
        self.logger = get_logger(__name__)

    async def transcode_video(
        self,
        video_uid: str,
        input_object_name: str
    ) -> TranscodeResult:
        """
        转码视频（MP4 → HLS）

        Args:
            video_uid: 视频唯一标识
            input_object_name: 输入对象名称（相对于 RAW_BUCKET）

        Returns:
            TranscodeResult: 转码结果

        Business Logic:
        - 转码是**非致命**的：失败时返回 status='failed' 而不抛异常
        - 允许重试（通过 config.max_retries 控制）
        - 失败后视频仍可用，只是没有 HLS 播放
        """
        self.logger.info(f"开始转码: video_uid={video_uid}")

        # 构建 GCS URIs
        input_uri = f"gs://{self.config.raw_bucket}/{input_object_name}"
        output_uri = f"gs://{self.config.hls_bucket}/encoded/{video_uid}/"

        # 检查输入文件是否存在
        if not await self.gcs.exists(input_uri):
            self.logger.error(f"输入文件不存在: {input_uri}")
            return TranscodeResult(
                hls_path=None,
                status="failed",
                error_message=f"Input file not found: {input_uri}"
            )

        # 重试逻辑
        for attempt in range(1, self.config.max_retries + 1):
            try:
                self.logger.info(
                    f"转码尝试 {attempt}/{self.config.max_retries}: {video_uid}"
                )

                # 1. 创建转码任务
                job_name = await self.transcoder.create_transcode_job(
                    input_uri=input_uri,
                    output_uri=output_uri,
                    template_id=self.config.transcoder_template_id
                )

                self.logger.info(f"转码任务已创建: {job_name}")

                # 2. 等待完成（最多 30 分钟）
                job = await self.transcoder.wait_for_job(
                    job_name=job_name,
                    max_wait_seconds=1800  # 30 minutes
                )

                # 3. 构建 HLS 路径
                hls_path = f"{output_uri}manifest.m3u8"

                self.logger.info(f"✓ 转码成功: {video_uid} → {hls_path}")

                return TranscodeResult(
                    hls_path=hls_path,
                    status="success",
                    error_message=None
                )

            except TranscoderError as e:
                self.logger.warning(
                    f"转码失败 (attempt {attempt}/{self.config.max_retries}): {e}"
                )

                # 如果还有重试机会，等待后重试
                if attempt < self.config.max_retries:
                    backoff = self.config.retry_backoff_seconds * (2 ** (attempt - 1))
                    self.logger.info(f"等待 {backoff}s 后重试...")
                    await asyncio.sleep(backoff)
                    continue

                # 所有重试都失败了
                self.logger.error(f"✗ 转码最终失败: {video_uid}, error: {e}")

                return TranscodeResult(
                    hls_path=None,
                    status="failed",
                    error_message=str(e)
                )

            except Exception as e:
                # 未预期错误（不重试）
                self.logger.error(f"转码出现未预期错误: {e}", exc_info=True)

                return TranscodeResult(
                    hls_path=None,
                    status="failed",
                    error_message=f"Unexpected error: {e}"
                )

        # 理论上不会到这里（重试循环会 return）
        return TranscodeResult(
            hls_path=None,
            status="failed",
            error_message="Max retries exceeded"
        )