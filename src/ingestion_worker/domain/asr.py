"""
职责：
- 编排 ASR 流程（生成 URL → 调 Replicate → 从输出读取 → 上传到 GCS）
- 处理 ASR 失败（重试）
- 解析 WhisperX JSON 输出为 Segment 列表

对外接口：
- async def run_whisperx(
    video_uid: str,
    input_object_name: str
  ) -> ASRResult  # {segments, asr_json_uri, vtt_uri}

业务逻辑：
- 生成 Signed GET URL（输入）
- 提交 Replicate 任务
- 等待完成，从 prediction.output 读取
- 手动上传 JSON/VTT 到 GCS
- 失败时抛出 ASRError

依赖：infrastructure.gcs, infrastructure.replicate
"""

import asyncio
import json
import aiohttp
from typing import Optional

from ingestion_worker.config import Config
from ingestion_worker.infrastructure.gcs import GCSClient, GCSError
from ingestion_worker.infrastructure.replicate import ReplicateClient, ReplicateError
from ingestion_worker.types import ASRResult, Segment
from ingestion_worker.errors import ASRError
from ingestion_worker.utils.logging import get_logger


class ASRService:
    """ASR 服务（业务层）"""

    def __init__(
        self,
        replicate: ReplicateClient,
        gcs: GCSClient,
        config: Config
    ):
        """
        初始化 ASR 服务

        Args:
            replicate: Replicate API 客户端
            gcs: GCS 客户端
            config: 系统配置
        """
        self.replicate = replicate
        self.gcs = gcs
        self.config = config
        self.logger = get_logger(__name__)

    async def run_whisperx(
        self,
        video_uid: str,
        input_object_name: str
    ) -> ASRResult:
        """
        运行 WhisperX ASR

        新流程（无需 Replicate PUT 支持）：
        1. 生成 Signed GET URL（给 Replicate 下载视频）
        2. 提交 Replicate 任务
        3. 等待完成，从 prediction.output 读取结果
        4. 手动上传 JSON/VTT 到 GCS
        5. 解析 segments

        Args:
            video_uid: 视频唯一标识
            input_object_name: 输入对象名称（相对于 RAW_BUCKET）

        Returns:
            ASRResult: ASR 结果（包含 segments 列表）

        Raises:
            ASRError: ASR 失败（重试耗尽后）
        """
        self.logger.info(f"开始 ASR: video_uid={video_uid}")

        # 重试逻辑
        for attempt in range(1, self.config.max_retries + 1):
            try:
                self.logger.info(
                    f"ASR 尝试 {attempt}/{self.config.max_retries}: {video_uid}"
                )

                # 1. 生成输入 URL（GET）
                audio_get_url = self.gcs.generate_signed_url(
                    bucket=self.config.raw_bucket,
                    object_name=input_object_name,
                    method="GET",
                    ttl_seconds=self.config.signed_url_ttl_seconds
                )

                self.logger.debug(f"已生成 Signed GET URL")

                # 2. 提交 Replicate 任务（不传 PUT URLs）
                prediction_id = await self.replicate.submit_whisperx(
                    audio_url=audio_get_url,
                    language="en",  # TODO: 支持语言检测
                    align_output=self.config.whisperx_align_output
                )

                self.logger.info(f"WhisperX 任务已提交: {prediction_id}")

                # 3. 等待完成（最多 30 分钟）
                prediction = await self.replicate.wait_for_prediction(
                    prediction_id=prediction_id,
                    max_wait_seconds=1800
                )

                # 4. 从 output 读取结果
                output = prediction.get("output")
                if not output:
                    raise ASRError(f"Replicate 输出为空: {prediction_id}")

                self.logger.info(f"✓ WhisperX 完成，开始处理结果")

                # 5. 手动上传 JSON 到 GCS
                json_uri = f"gs://{self.config.transcript_bucket}/{video_uid}/asr.json"
                await self._upload_json_to_gcs(json_uri, output)

                # 6. 生成 VTT 并上传
                vtt_uri = f"gs://{self.config.transcript_bucket}/{video_uid}/subs.vtt"
                segments_data = output.get("segments", [])
                vtt_content = self._generate_vtt(segments_data)
                await self._upload_text_to_gcs(vtt_uri, vtt_content)

                # 7. 解析 segments
                segments = self._parse_segments(output)

                # 8. 计算视频时长
                duration_seconds = segments[-1].t_end if segments else 0.0

                self.logger.info(
                    f"✓ ASR 完成: {video_uid}, "
                    f"{len(segments)} segments, "
                    f"{duration_seconds:.1f}s"
                )

                return ASRResult(
                    segments=segments,
                    asr_json_uri=json_uri,
                    vtt_uri=vtt_uri,
                    duration_seconds=duration_seconds
                )

            except (ReplicateError, GCSError) as e:
                self.logger.warning(
                    f"ASR 失败 (attempt {attempt}/{self.config.max_retries}): {e}"
                )

                # 如果还有重试机会，等待后重试
                if attempt < self.config.max_retries:
                    backoff = self.config.retry_backoff_seconds * (2 ** (attempt - 1))
                    self.logger.info(f"等待 {backoff}s 后重试...")
                    await asyncio.sleep(backoff)
                    continue

                # 所有重试都失败了
                self.logger.error(f"✗ ASR 最终失败: {video_uid}, error: {e}")
                raise ASRError(f"ASR failed after {self.config.max_retries} retries: {e}")

            except Exception as e:
                # 未预期错误（不重试）
                self.logger.error(f"ASR 出现未预期错误: {e}", exc_info=True)
                raise ASRError(f"Unexpected ASR error: {e}")

        # 理论上不会到这里
        raise ASRError(f"Max retries exceeded: {self.config.max_retries}")

    async def _upload_json_to_gcs(self, uri: str, data: dict):
        """
        上传 JSON 到 GCS

        Args:
            uri: 目标 GCS URI (gs://bucket/path/file.json)
            data: 要上传的字典数据

        Raises:
            GCSError: 上传失败
        """
        try:
            bucket, object_name = self.gcs.parse_uri(uri)

            # 生成 PUT URL
            put_url = self.gcs.generate_signed_url(
                bucket=bucket,
                object_name=object_name,
                method="PUT",
                ttl_seconds=300,  # 5 分钟足够
                content_type="application/json"
            )

            # 上传
            json_bytes = json.dumps(data, indent=2).encode('utf-8')

            async with aiohttp.ClientSession() as session:
                async with session.put(
                    put_url,
                    data=json_bytes,
                    headers={"Content-Type": "application/json"}
                ) as resp:
                    if resp.status not in (200, 201):
                        error_text = await resp.text()
                        raise GCSError(
                            f"上传 JSON 失败: HTTP {resp.status}, {error_text}"
                        )

            self.logger.info(f"✓ 已上传 JSON: {uri}")

        except aiohttp.ClientError as e:
            self.logger.error(f"上传 JSON 网络错误: {e}")
            raise GCSError(f"Failed to upload JSON: {e}")
        except Exception as e:
            self.logger.error(f"上传 JSON 失败: {e}")
            raise GCSError(f"Failed to upload JSON: {e}")

    async def _upload_text_to_gcs(self, uri: str, content: str):
        """
        上传文本文件到 GCS

        Args:
            uri: 目标 GCS URI
            content: 文本内容

        Raises:
            GCSError: 上传失败
        """
        try:
            bucket, object_name = self.gcs.parse_uri(uri)

            # 生成 PUT URL
            put_url = self.gcs.generate_signed_url(
                bucket=bucket,
                object_name=object_name,
                method="PUT",
                ttl_seconds=300,
                content_type="text/vtt"
            )

            # 上传
            async with aiohttp.ClientSession() as session:
                async with session.put(
                    put_url,
                    data=content.encode('utf-8'),
                    headers={"Content-Type": "text/vtt"}
                ) as resp:
                    if resp.status not in (200, 201):
                        error_text = await resp.text()
                        raise GCSError(
                            f"上传 VTT 失败: HTTP {resp.status}, {error_text}"
                        )

            self.logger.info(f"✓ 已上传 VTT: {uri}")

        except aiohttp.ClientError as e:
            self.logger.error(f"上传 VTT 网络错误: {e}")
            raise GCSError(f"Failed to upload VTT: {e}")
        except Exception as e:
            self.logger.error(f"上传 VTT 失败: {e}")
            raise GCSError(f"Failed to upload VTT: {e}")

    def _generate_vtt(self, segments: list) -> str:
        """
        生成 WebVTT 字幕

        Args:
            segments: WhisperX segments 列表

        Returns:
            VTT 格式字符串
        """
        lines = ["WEBVTT\n"]

        for i, seg in enumerate(segments, 1):
            start = self._format_timestamp(seg["start"])
            end = self._format_timestamp(seg["end"])
            text = seg["text"].strip()

            lines.append(f"{i}")
            lines.append(f"{start} --> {end}")
            lines.append(text)
            lines.append("")

        return "\n".join(lines)

    def _format_timestamp(self, seconds: float) -> str:
        """
        格式化时间戳为 VTT 格式

        Args:
            seconds: 秒数

        Returns:
            格式化的时间戳（如 "00:01:23.456"）
        """
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:06.3f}"

    def _parse_segments(self, output: dict) -> list[Segment]:
        """
        解析 Replicate 输出为 Segment 列表

        Args:
            output: Replicate prediction.output

        Returns:
            Segment 列表

        Raises:
            ASRError: 解析失败

        WhisperX Output 格式：
        {
          "segments": [
            {
              "start": 0.5,
              "end": 3.2,
              "text": " Hello world",
              "words": [...],
              "speaker": "SPEAKER_00"
            }
          ]
        }
        """
        try:
            segments = []

            for seg_data in output.get("segments", []):
                segment = Segment(
                    t_start=float(seg_data["start"]),
                    t_end=float(seg_data["end"]),
                    text=seg_data["text"].strip(),
                    lang=seg_data.get("language", "en"),
                    speaker=seg_data.get("speaker"),
                    meta={
                        "words": seg_data.get("words", []),
                        "chars": seg_data.get("chars", []),
                        "confidence": seg_data.get("confidence")
                    }
                )
                segments.append(segment)

            self.logger.debug(f"✓ 解析 {len(segments)} 个 segments")

            return segments

        except KeyError as e:
            self.logger.error(f"WhisperX 输出格式无效: 缺少字段 {e}")
            raise ASRError(f"Invalid WhisperX output format: missing {e}")
        except Exception as e:
            self.logger.error(f"解析 segments 失败: {e}")
            raise ASRError(f"Failed to parse segments: {e}")