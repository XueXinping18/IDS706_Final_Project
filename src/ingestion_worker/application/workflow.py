"""
Ingestion Workflow - 视频预处理编排器

职责：
- 编排整个 ingestion 流程（Step 0-6）
- 幂等性控制
- 状态机管理
- 错误处理与通知
"""
import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

from ingestion_worker.config import Config
from ingestion_worker.types import (
    PubSubMessage,
    IngestJob,
    Segment,
    Annotation,
    ProcessingStats,
    ASRResult,
    TranscodeResult,
    AgenticResult,
)
from ingestion_worker.errors import (
    WorkflowError,
    IdempotencyError,
    TranscodingError,
    ASRError,
    AgenticError,
    PersistenceError,
)
from ingestion_worker.utils.logging import get_logger, set_correlation_id, clear_correlation_id

# 业务配置
PROCESSING_TIMEOUT_SECONDS = 3600  # 1 hour


class IngestVideoWorkflow:
    """视频预处理工作流"""

    def __init__(
            self,
            config: Config,
            db,  # infrastructure.database.Database
            gcs,  # infrastructure.gcs.GCSClient
            lark,  # infrastructure.lark.LarkClient
            transcoder,  # infrastructure.transcoder.TranscoderClient
            replicate,  # infrastructure.replicate.ReplicateClient
            agentic,  # domain.agentic.orchestrators.AgenticOrchestrator
            persistence,  # domain.persistence.PersistenceService
    ):
        """
        初始化工作流

        Args:
            config: 系统配置
            db: 数据库客户端
            gcs: GCS 客户端
            lark: Lark 通知客户端
            transcoder: Transcoder 客户端
            replicate: Replicate 客户端
            agentic: Agentic 编排器
            persistence: 持久化服务
        """
        self.config = config
        self.db = db
        self.gcs = gcs
        self.lark = lark
        self.logger = get_logger(__name__)

        # 初始化领域服务（在 workflow 内部组装）
        from ingestion_worker.domain.transcoding import TranscodingService
        from ingestion_worker.domain.asr import ASRService

        self.transcoding_service = TranscodingService(transcoder, gcs, config)
        self.asr_service = ASRService(replicate, gcs, config)
        self.agentic_service = agentic
        self.persistence_service = persistence

    async def process_message(self, message: PubSubMessage) -> None:
        """
        处理 Pub/Sub 消息的入口

        Args:
            message: 解析后的 Pub/Sub 消息

        Raises:
            WorkflowError: 任何业务异常
        """
        start_time = time.time()

        # 设置 correlation_id（所有后续日志都会带上）
        set_correlation_id(message.video_uid)

        try:
            self.logger.info("开始处理视频")

            # Step 0: 幂等检查
            self.logger.info("Step 0: 幂等性检查")
            job = await self._check_idempotency(message)

            # Step 1-2: 并行转码与 ASR
            self.logger.info("Step 1-2: 并行转码与 ASR")
            hls_result, asr_result = await self._parallel_transcode_and_asr(
                message.video_uid, message.object_name
            )

            # Step 3: Agentic workflow
            self.logger.info("Step 3: Agentic workflow")
            agentic_result = await self._run_agentic(
                video_uid=message.video_uid,
                video_object_name=message.object_name,
                asr_result=asr_result,
            )

            # Step 4: 持久化
            self.logger.info("Step 4: 持久化数据")
            stats = await self._persist_data(
                video_uid=message.video_uid,
                job=job,
                asr_result=asr_result,
                agentic_result=agentic_result,
            )

            # Step 5-6: 更新路径与状态
            self.logger.info("Step 5-6: 更新路径与状态")
            await self._finalize(
                job=job,
                hls_result=hls_result,
                asr_result=asr_result,
                stats=stats,
            )

            elapsed = time.time() - start_time
            self.logger.info(f"✓ 处理完成，耗时 {elapsed:.1f}s")

        except WorkflowError as e:
            self.logger.error(f"工作流错误: {e.message}")
            await self._handle_error(message.video_uid, e)
            raise
        except Exception as e:
            self.logger.error(f"未预期异常: {e}", exc_info=True)
            await self._handle_error(message.video_uid, WorkflowError(str(e)))
            raise
        finally:
            clear_correlation_id()

    async def _get_or_create_video(self, video_uid: str, object_name: str) -> int:
        """
        获取或创建 video 记录

        Args:
            video_uid: 视频 UID
            object_name: GCS 对象名称

        Returns:
            video_id: 视频 ID
        """
        # 1. 尝试获取已存在的 video
        row = await self.db.fetch_one(
            "SELECT id FROM video WHERE video_uid = $1",
            video_uid
        )

        if row:
            return row["id"]

        # 2. 创建新 video
        storage_path = f"gs://{self.config.raw_bucket}/{object_name}"

        row = await self.db.fetch_one(
            """
            INSERT INTO video (video_uid, status, storage_path)
            VALUES ($1, 'PROCESSING', $2)
            RETURNING id
            """,
            video_uid,
            storage_path
        )

        return row["id"]

    async def _check_idempotency(self, message: PubSubMessage) -> IngestJob:
        """
        Step 0: 幂等性检查

        Args:
            message: Pub/Sub 消息

        Returns:
            IngestJob: 任务记录

        Raises:
            IdempotencyError: 任务已完成或正在处理中

        Business Logic:
        - 查询 ingest_jobs 表（以 object_key + etag 为主键）
        - 如果已完成 → 跳过
        - 如果正在处理且未超时 → 跳过（让原任务继续）
        - 如果超时 → 重置为 queued
        - 否则 → 创建新任务
        """
        self.logger.info(f"检查任务状态: {message.object_name}")

        # 1. 查询是否已存在
        query = """
            SELECT object_key, etag, video_uid, video_id, status, 
                   started_at, retry_count
            FROM ingest_jobs
            WHERE object_key = $1 AND etag = $2
        """

        row = await self.db.fetch_one(query, message.object_name, message.etag)

        if row:
            # 任务已存在
            status = row["status"]
            started_at = row["started_at"]

            if status == "done":
                self.logger.info(f"✓ 任务已完成，跳过: {message.video_uid}")
                raise IdempotencyError("Task already completed")

            if status == "processing":
                # 检查是否超时
                if started_at:
                    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
                    if elapsed < PROCESSING_TIMEOUT_SECONDS:
                        self.logger.info(
                            f"✓ 任务正在处理中（{elapsed:.0f}s），跳过: {message.video_uid}"
                        )
                        raise IdempotencyError("Task already processing")
                    else:
                        # 超时，重置状态
                        self.logger.warning(
                            f"任务超时（{elapsed:.0f}s），重置为 queued: {message.video_uid}"
                        )
                        await self.db.execute(
                            """
                            UPDATE ingest_jobs
                            SET status = 'queued', started_at = NULL, 
                                retry_count = retry_count + 1
                            WHERE object_key = $1 AND etag = $2
                            """,
                            message.object_name,
                            message.etag
                        )

            # 返回已存在的任务
            return IngestJob(
                object_key=row["object_key"],
                etag=row["etag"],
                video_uid=row["video_uid"],
                video_id=row["video_id"],
                status=row["status"],
                retry_count=row["retry_count"]
            )

        # 2. 获取或创建 video 记录
        video_id = await self._get_or_create_video(message.video_uid, message.object_name)

        # 3. 创建新任务
        await self.db.execute(
            """
            INSERT INTO ingest_jobs (object_key, etag, video_uid, video_id, status, started_at)
            VALUES ($1, $2, $3, $4, 'processing', NOW())
            """,
            message.object_name,
            message.etag,
            message.video_uid,
            video_id
        )

        self.logger.info(f"✓ 创建新任务: {message.video_uid} (video_id={video_id})")

        return IngestJob(
            object_key=message.object_name,
            etag=message.etag,
            video_uid=message.video_uid,
            video_id=video_id,
            status="processing",
            retry_count=0
        )

    async def _parallel_transcode_and_asr(
        self, video_uid: str, object_name: str
    ) -> tuple[TranscodeResult, ASRResult]:
        """
        Step 1 & 2: 并行执行转码与 ASR

        Args:
            video_uid: 视频唯一标识
            object_name: GCS 对象名称

        Returns:
            (TranscodeResult, ASRResult)

        Raises:
            ASRError: ASR 失败（致命）
            Note: 转码失败不抛异常，返回 failed 状态
        """
        self.logger.info("启动并行任务：转码 + ASR")

        # 并行执行两个任务
        transcode_task = asyncio.create_task(
            self.transcoding_service.transcode_video(video_uid, object_name)
        )
        asr_task = asyncio.create_task(
            self.asr_service.run_whisperx(video_uid, object_name)
        )

        # 等待两个任务都完成
        transcode_result, asr_result = await asyncio.gather(
            transcode_task,
            asr_task,
            return_exceptions=False  # ASR 失败会抛异常
        )

        # 记录结果
        if transcode_result.status == "success":
            self.logger.info(f"✓ 转码成功: {transcode_result.hls_path}")
        else:
            self.logger.warning(
                f"⚠️  转码失败（非致命）: {transcode_result.error_message}"
            )

        self.logger.info(
            f"✓ ASR 成功: {len(asr_result.segments)} segments, "
            f"{asr_result.duration_seconds:.1f}s"
        )

        return transcode_result, asr_result

    async def _run_agentic(
        self,
        video_uid: str,
        video_object_name: str,
        asr_result: ASRResult,
    ) -> AgenticResult:
        """
        Step 3: Agentic workflow

        Args:
            video_uid: 视频唯一标识
            video_object_name: 视频对象名称
            asr_result: ASR 结果

        Returns:
            AgenticResult

        Raises:
            AgenticError: Agentic workflow 失败
        """
        self.logger.info("运行 Agentic workflow...")

        # 构建视频 URI
        video_uri = f"gs://{self.config.raw_bucket}/{video_object_name}"

        # 准备 segments 数据（转换为 dict 格式）
        segments = [
            {
                "start": seg.t_start,
                "end": seg.t_end,
                "text": seg.text,
                "lang": seg.lang,
                "speaker": seg.speaker,
                "meta": seg.meta
            }
            for seg in asr_result.segments
        ]

        # 调用 Agentic Orchestrator
        annotations, method, ontology_ver = await self.agentic_service.process_video(
            video_uid=video_uid,
            video_uri=video_uri,
            segments=segments
        )

        self.logger.info(
            f"✓ Agentic workflow 完成: {len(annotations)} annotations, "
            f"method={method}"
        )

        return AgenticResult(
            annotations=annotations,
            method=method,
            ontology_ver=ontology_ver
        )

    async def _persist_data(
        self,
        video_uid: str,
        job: IngestJob,
        asr_result: ASRResult,
        agentic_result: AgenticResult,
    ) -> dict:
        """
        Step 4: 持久化数据

        Args:
            video_uid: 视频 UID
            job: 任务记录
            asr_result: ASR 结果
            agentic_result: Agentic 结果

        Returns:
            统计信息

        Raises:
            PersistenceError: 持久化失败
        """
        self.logger.info("写入数据库...")

        # 准备 segments 数据
        segments = [
            {
                "start": seg.t_start,
                "end": seg.t_end,
                "text": seg.text,
                "lang": seg.lang,
                "meta": seg.meta
            }
            for seg in asr_result.segments
        ]

        # 调用持久化服务
        stats = await self.persistence_service.save_video_analysis(
            video_id=job.video_id,
            segments=segments,
            annotations=agentic_result.annotations,
            method=agentic_result.method,
            ontology_ver=agentic_result.ontology_ver
        )

        self.logger.info(
            f"✓ 数据持久化完成: "
            f"{stats['segments_inserted']} segments, "
            f"{stats['occurrences_inserted']} occurrences "
            f"({stats['occurrences_skipped']} skipped)"
        )

        return stats

    async def _finalize(
        self,
        job: IngestJob,
        hls_result: TranscodeResult,
        asr_result: ASRResult,
        stats: dict,
    ) -> None:
        """
        Step 5-6: 更新路径与状态

        Args:
            job: 任务记录
            hls_result: 转码结果
            asr_result: ASR 结果
            stats: 持久化统计
        """
        self.logger.info("更新 video 与 ingest_job 状态...")

        # 1. 更新 video 状态
        await self.persistence_service.update_video_status(
            video_id=job.video_id,
            status="READY",
            hls_path=hls_result.hls_path,  # 可能为 None
            transcript_path=asr_result.asr_json_uri
        )

        # 2. 更新 ingest_job 状态
        await self.db.execute(
            """
            UPDATE ingest_jobs
            SET status = 'done', finished_at = NOW()
            WHERE object_key = $1 AND etag = $2
            """,
            job.object_key,
            job.etag
        )

        self.logger.info("✓ 状态更新完成")

    async def _handle_error(self, video_uid: str, error: WorkflowError) -> None:
        """
        处理工作流错误

        Args:
            video_uid: 视频 UID
            error: 工作流错误

        Actions:
        - 更新 ingest_job 状态为 error
        - 更新 video 状态为 ERROR
        - 发送 Lark 通知
        """
        self.logger.error(f"记录错误: {error.message}")

        try:
            # 1. 更新 ingest_job 状态
            await self.db.execute(
                """
                UPDATE ingest_jobs
                SET status = 'error', 
                    err = $1, 
                    finished_at = NOW()
                WHERE video_uid = $2 AND status = 'processing'
                """,
                error.message,
                video_uid
            )

            # 2. 更新 video 状态
            await self.db.execute(
                """
                UPDATE video
                SET status = 'ERROR'
                WHERE video_uid = $1
                """,
                video_uid
            )

            # 3. 发送 Lark 通知
            await self.lark.send_error(
                error_type=error.__class__.__name__,
                error_message=error.message,
                context={
                    "video_uid": video_uid,
                    "retryable": str(error.retryable)
                }
            )

        except Exception as e:
            self.logger.error(f"错误处理失败: {e}", exc_info=True)