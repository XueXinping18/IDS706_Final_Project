"""
数据持久化服务

职责：
- 批量写入 segment 和 occurrence
- 处理唯一约束冲突（去重）
- 处理外键异常（fine_id 不存在时跳过）
- 返回插入统计

依赖：infrastructure.database

对外接口：
- save_video_analysis(video_id, segments, annotations, method, ontology_ver) -> dict
"""

import json
from typing import Optional
import asyncpg

from ingestion_worker.infrastructure.database import Database, DatabaseError
from ingestion_worker.types import Segment, Annotation
from ingestion_worker.utils.logging import get_logger


class PersistenceError(Exception):
    """持久化错误"""
    pass


class PersistenceService:
    """数据持久化服务"""

    def __init__(self, db: Database):
        """
        初始化持久化服务

        Args:
            db: 数据库客户端
        """
        self.db = db
        self.logger = get_logger(__name__)

    async def save_video_analysis(
        self,
        video_id: int,
        segments: list[dict],
        annotations: list[dict],
        method: str,
        ontology_ver: str
    ) -> dict:
        """
        保存视频分析结果（批量写入）

        Args:
            video_id: 视频 ID
            segments: Segment 列表（WhisperX 输出）
            annotations: Annotation 列表（Agentic 输出）
            method: 方法标记（'gemini_video' | 'gemini_text' | 'gemini_nocache'）
            ontology_ver: 本体版本（如 'gemini-2.0-20250110'）

        Returns:
            统计信息 {
                'segments_inserted': int,
                'occurrences_inserted': int,
                'occurrences_skipped': int
            }

        Raises:
            PersistenceError: 保存失败
        """
        self.logger.info(
            f"开始保存分析结果: video_id={video_id}, "
            f"segments={len(segments)}, annotations={len(annotations)}"
        )

        try:
            # 使用事务（保证原子性）
            async with self.db.transaction() as conn:
                # 1. 批量插入 segments
                segment_ids = await self._insert_segments(conn, video_id, segments)

                # 2. 批量插入 occurrences
                occ_inserted, occ_skipped = await self._insert_occurrences(
                    conn, segment_ids, annotations, method, ontology_ver
                )

                stats = {
                    "segments_inserted": len(segment_ids),
                    "occurrences_inserted": occ_inserted,
                    "occurrences_skipped": occ_skipped
                }

                self.logger.info(
                    f"✓ 保存完成: {stats['segments_inserted']} segments, "
                    f"{stats['occurrences_inserted']} occurrences "
                    f"({stats['occurrences_skipped']} skipped)"
                )

                return stats

        except DatabaseError as e:
            self.logger.error(f"保存失败: {e}")
            raise PersistenceError(f"Failed to save video analysis: {e}") from e
        except Exception as e:
            self.logger.error(f"保存失败（未预期错误）: {e}")
            raise PersistenceError(f"Unexpected error: {e}") from e

    async def _insert_segments(
        self,
        conn,
        video_id: int,
        segments: list[dict]
    ) -> list[int]:
        """
        批量插入 segments（去重）

        去重策略：基于 (video_id, t_start, text) 唯一约束
        - 如果已存在，更新 t_end（允许修正时间）
        - 返回所有 segment_id（包括已存在的）

        Args:
            conn: 数据库连接（事务中）
            video_id: 视频 ID
            segments: Segment 列表

        Returns:
            segment_id 列表（顺序对应输入）
        """
        query = """
            INSERT INTO segment (video_id, t_start, t_end, text, lang, meta)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (video_id, t_start, text)
            DO UPDATE SET
                t_end = EXCLUDED.t_end,
                updated_at = NOW()
            RETURNING id
        """

        segment_ids = []

        for seg in segments:
            try:
                result = await conn.fetchrow(
                    query,
                    video_id,
                    seg["start"],  # t_start
                    seg["end"],    # t_end
                    seg["text"],
                    seg.get("lang", "en"),
                    json.dumps(seg.get("meta", {}))
                )

                segment_ids.append(result["id"])

            except Exception as e:
                self.logger.error(f"插入 segment 失败: {e}, segment: {seg}")
                raise

        self.logger.debug(f"插入 {len(segment_ids)} 个 segments")
        return segment_ids

    async def _insert_occurrences(
        self,
        conn,
        segment_ids: list[int],
        annotations: list[dict],
        method: str,
        ontology_ver: str
    ) -> tuple[int, int]:
        """
        批量插入 occurrences（去重 + 忽略外键错误）

        去重策略：基于 (segment_id, fine_id, span) 唯一约束
        - 如果已存在，跳过（DO NOTHING）

        错误处理：
        - 外键错误（fine_id 不存在）：跳过并记录警告
        - 其他错误：抛出异常

        Args:
            conn: 数据库连接（事务中）
            segment_ids: Segment ID 列表（对应 segments）
            annotations: Annotation 列表
            method: 方法标记
            ontology_ver: 本体版本

        Returns:
            (插入数量, 跳过数量) 元组
        """
        query = """
            INSERT INTO occurrence (
                segment_id, fine_id, reliability_score, detection_method, evidence, ontology_ver
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (segment_id, fine_id, ((evidence->>'span')::jsonb))
            DO NOTHING
        """

        inserted = 0
        skipped = 0
        first_error = None  # Track the first error to identify root cause

        for idx, ann in enumerate(annotations):
            try:
                # 获取对应的 segment_id
                segment_index = ann.get("segment_index")
                if segment_index is None or segment_index >= len(segment_ids):
                    self.logger.warning(
                        f"Invalid segment_index: {segment_index}, "
                        f"max={len(segment_ids) - 1}"
                    )
                    skipped += 1
                    continue

                segment_id = segment_ids[segment_index]

                # 构造 evidence（包含 span 和其他信息）
                evidence = {
                    "span": ann.get("span", {}),
                    "rationale": ann.get("rationale", ""),
                    "visual_comprehensibility": ann.get("visual_comprehensibility"),
                    "textual_comprehensibility": ann.get("textual_comprehensibility")
                }

                # 插入
                result = await conn.execute(
                    query,
                    segment_id,
                    ann["fine_id"],
                    ann.get("score", 0.5),
                    method,
                    json.dumps(evidence),
                    ontology_ver
                )

                # 检查是否插入（INSERT 0 0 表示冲突跳过）
                if "INSERT 0 1" in result:
                    inserted += 1

            except asyncpg.PostgresError as e:
                # Capture detailed PostgreSQL error information
                error_detail = {
                    "annotation_index": idx,
                    "segment_index": ann.get("segment_index"),
                    "fine_id": ann.get("fine_id"),
                    "error_code": e.sqlstate if hasattr(e, 'sqlstate') else None,
                    "error_message": str(e),
                    "constraint_name": e.constraint_name if hasattr(e, 'constraint_name') else None,
                    "table_name": e.table_name if hasattr(e, 'table_name') else None,
                    "column_name": e.column_name if hasattr(e, 'column_name') else None,
                }

                # Log the first error with full details
                if first_error is None:
                    first_error = error_detail
                    self.logger.error(
                        f"🔴 FIRST ERROR at annotation #{idx}:\n"
                        f"  Error Code: {error_detail['error_code']}\n"
                        f"  Message: {error_detail['error_message']}\n"
                        f"  Constraint: {error_detail['constraint_name']}\n"
                        f"  Table: {error_detail['table_name']}\n"
                        f"  Column: {error_detail['column_name']}\n"
                        f"  Segment Index: {error_detail['segment_index']}\n"
                        f"  Fine ID: {error_detail['fine_id']}\n"
                        f"  Annotation: {json.dumps(ann, indent=2)}"
                    )
                else:
                    # Subsequent errors - log concisely
                    self.logger.debug(
                        f"Cascading error at annotation #{idx} "
                        f"(transaction aborted): {error_detail['error_message'][:100]}"
                    )

                # Check if it's a foreign key error
                if isinstance(e, asyncpg.ForeignKeyViolationError):
                    self.logger.warning(
                        f"跳过无效的 fine_id: {ann.get('fine_id')} "
                        f"(违反外键约束: {error_detail['constraint_name']})"
                    )
                    skipped += 1
                else:
                    # Other database errors - don't swallow them if it's the first error
                    if first_error == error_detail:
                        # This is the root cause, raise it
                        raise
                    else:
                        # This is a cascading error, skip it
                        skipped += 1

            except DatabaseError as e:
                # Fallback for DatabaseError wrapper
                if first_error is None:
                    first_error = {"annotation_index": idx, "error": str(e)}
                    self.logger.error(
                        f"🔴 FIRST ERROR (DatabaseError) at annotation #{idx}:\n"
                        f"  Message: {e}\n"
                        f"  Annotation: {json.dumps(ann, indent=2)}"
                    )

                # Check if it's a foreign key error
                if "foreign key" in str(e).lower() or "violates foreign key" in str(e).lower():
                    self.logger.warning(
                        f"跳过无效的 fine_id: {ann.get('fine_id')} "
                        f"(可能已被删除或不存在)"
                    )
                    skipped += 1
                else:
                    # Other database errors
                    if first_error.get("error") == str(e):
                        raise
                    else:
                        skipped += 1

            except Exception as e:
                if first_error is None:
                    first_error = {"annotation_index": idx, "error": str(e)}
                    self.logger.error(
                        f"🔴 FIRST ERROR (Unexpected) at annotation #{idx}:\n"
                        f"  Type: {type(e).__name__}\n"
                        f"  Message: {e}\n"
                        f"  Annotation: {json.dumps(ann, indent=2)}"
                    )
                else:
                    self.logger.debug(f"Cascading error at annotation #{idx}: {e}")
                skipped += 1

        self.logger.debug(
            f"插入 {inserted} 个 occurrences, 跳过 {skipped} 个"
        )

        return inserted, skipped

    async def update_video_status(
        self,
        video_id: int,
        status: str,
        hls_path: Optional[str] = None,
        transcript_path: Optional[str] = None
    ):
        """
        更新视频状态

        Args:
            video_id: 视频 ID
            status: 状态（'READY' | 'ERROR'）
            hls_path: HLS 路径（可选）
            transcript_path: 转录路径（可选）
        """
        updates = ["status = $2", "updated_at = NOW()"]
        params = [video_id, status]
        param_index = 3

        if hls_path:
            updates.append(f"hls_path = ${param_index}")
            params.append(hls_path)
            param_index += 1

        if transcript_path:
            updates.append(f"structured_transcript_path = ${param_index}")
            params.append(transcript_path)
            param_index += 1

        query = f"""
            UPDATE video
            SET {', '.join(updates)}
            WHERE id = $1
        """

        try:
            await self.db.execute(query, *params)
            self.logger.info(f"✓ 更新视频状态: video_id={video_id}, status={status}")

        except DatabaseError as e:
            self.logger.error(f"更新视频状态失败: {e}")
            raise PersistenceError(f"Failed to update video status: {e}") from e