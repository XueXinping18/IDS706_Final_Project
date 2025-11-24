"""
- 创建Google transcoder 视频转码任务（MP4 → HLS）
- 轮询任务状态直到完成
- 处理错误和重试
依赖：google-cloud-video-transcoder

对外接口：
- async def create_transcode_job(input_uri, output_uri, template_id) -> str (job_name)
- async def wait_for_job(job_name, max_wait_seconds) -> dict (job result)

注意：
- Transcoder API 是同步的，需要用 asyncio.to_thread 包装阻塞调用
"""
## TODO: I have yet to give transcoder SA the permission to modify the HLS and RAW folder but only allowed it to modify infra-test folder.
## TODO: grant permission before production use
import asyncio
from typing import Optional, Any
from datetime import datetime

from google.cloud.video import transcoder_v1
from google.api_core import exceptions as gcp_exceptions

from ingestion_worker.config import Config
from ingestion_worker.utils.logging import get_logger


class TranscoderError(Exception):
    """Transcoder API 错误"""
    pass


class TranscoderClient:
    """Google Transcoder API 客户端"""

    def __init__(self, config: Config):
        """
        初始化 Transcoder 客户端

        Args:
            config: 系统配置
        """
        self.config = config
        self.logger = get_logger(__name__)

        try:
            # 创建同步客户端（Transcoder API 是同步的）
            self.client = transcoder_v1.TranscoderServiceClient()
            self.parent = f"projects/{config.gcp_project}/locations/{config.gcp_region}"
            self.logger.info(f"✓ Transcoder 客户端初始化成功 (project: {config.gcp_project})")
        except Exception as e:
            self.logger.error(f"✗ Transcoder 客户端初始化失败: {e}")
            raise TranscoderError(f"Failed to initialize Transcoder client: {e}") from e

    async def create_transcode_job(
            self,
            input_uri: str,
            output_uri: str,
            template_id: Optional[str] = None,
    ) -> str:
        """
        创建转码任务

        Args:
            input_uri: 输入视频 URI (gs://bucket/path/video.mp4)
            output_uri: 输出目录 URI (gs://bucket/path/output/)
            template_id: 转码模板 ID（如 'preset/web-hd'），None则从config中读取默认值，归根结底来自于.env环境变量

        Returns:
            job_name: 任务名称（用于后续查询）

        Raises:
            TranscoderError: 创建任务失败
        """
        if template_id is None:
            template_id = self.config.transcoder_template_id

        self.logger.info(f"创建转码任务: {input_uri} → {output_uri}")
        self.logger.debug(f"使用模板: {template_id}")

        try:
            # 构建 Job 配置
            job = transcoder_v1.Job()
            job.input_uri = input_uri
            job.output_uri = output_uri

            # 使用预设模板
            if template_id.startswith("preset/"):
                job.template_id = template_id
            else:
                # 自定义模板（需要完整路径）
                job.template_id = f"projects/{self.config.gcp_project}/locations/{self.config.gcp_region}/jobTemplates/{template_id}"

            # 在 asyncio 中运行同步代码（不阻塞事件循环）
            response = await asyncio.to_thread(
                self.client.create_job,
                parent=self.parent,
                job=job
            )

            job_name = response.name
            self.logger.info(f"✓ 转码任务已创建: {job_name}")

            return job_name

        except gcp_exceptions.NotFound as e:
            self.logger.error(f"模板不存在: {template_id}")
            raise TranscoderError(f"Template not found: {template_id}") from e
        except gcp_exceptions.PermissionDenied as e:
            self.logger.error(f"权限不足: {e}")
            raise TranscoderError(f"Permission denied: {e}") from e
        except gcp_exceptions.InvalidArgument as e:
            self.logger.error(f"参数无效: {e}")
            raise TranscoderError(f"Invalid argument: {e}") from e
        except Exception as e:
            self.logger.error(f"创建转码任务失败: {e}")
            raise TranscoderError(f"Failed to create transcode job: {e}") from e

    async def get_job(self, job_name: str) -> transcoder_v1.Job:
        """
        获取转码任务状态

        Args:
            job_name: 任务名称

        Returns:
            Job 对象

        Raises:
            TranscoderError: 查询失败
        """
        try:
            job = await asyncio.to_thread(
                self.client.get_job,
                name=job_name
            )
            return job

        except gcp_exceptions.NotFound as e:
            raise TranscoderError(f"Job not found: {job_name}") from e
        except Exception as e:
            self.logger.error(f"查询任务失败: {e}")
            raise TranscoderError(f"Failed to get job: {e}") from e

    async def wait_for_job(
            self,
            job_name: str,
            max_wait_seconds: int = 1800,  # 30 分钟
            poll_interval_seconds: int = 10,
    ) -> transcoder_v1.Job:
        """
        轮询等待转码任务完成

        Args:
            job_name: 任务名称
            max_wait_seconds: 最大等待时间（秒）
            poll_interval_seconds: 轮询间隔（秒）

        Returns:
            完成的 Job 对象

        Raises:
            TranscoderError: 任务失败或超时
        """
        start_time = datetime.now()
        poll_count = 0

        self.logger.info(f"等待转码任务完成: {job_name} (最多 {max_wait_seconds}s)")

        while True:
            poll_count += 1
            elapsed = (datetime.now() - start_time).total_seconds()

            if elapsed > max_wait_seconds:
                raise TranscoderError(f"转码任务超时 (>{max_wait_seconds}s): {job_name}")

            # 查询状态
            job = await self.get_job(job_name)
            state = job.state

            self.logger.debug(f"轮询 #{poll_count}: state={state.name}, elapsed={elapsed:.1f}s")

            if state == transcoder_v1.Job.ProcessingState.SUCCEEDED:
                self.logger.info(f"✓ 转码任务完成: {job_name} (耗时 {elapsed:.1f}s)")
                return job

            elif state == transcoder_v1.Job.ProcessingState.FAILED:
                error_msg = job.error.message if job.error else "Unknown error"
                self.logger.error(f"✗ 转码任务失败: {error_msg}")
                raise TranscoderError(f"Transcode job failed: {error_msg}")

            elif state in (
                    transcoder_v1.Job.ProcessingState.PENDING,
                    transcoder_v1.Job.ProcessingState.RUNNING
            ):
                # 任务进行中，继续等待
                # 显示进度（如果有）
                if hasattr(job, 'progress') and job.progress:
                    progress_percent = job.progress.analyzed + job.progress.encoded
                    self.logger.debug(f"  进度: {progress_percent:.1f}%")

                await asyncio.sleep(poll_interval_seconds)

            else:
                # 未知状态
                self.logger.warning(f"未知状态: {state.name}")
                await asyncio.sleep(poll_interval_seconds)