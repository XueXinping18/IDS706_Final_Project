"""
职责：
- 定义跨模块共享的数据类型
- 避免循环依赖

包含：
- PubSubMessage
- Segment
- Annotation
- ASRResult
- TranscodeResult
"""

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime


@dataclass
class PubSubMessage:
    """Pub/Sub 消息解析后的结构"""
    bucket: str
    object_name: str  # 完整路径，如 "uploads/abc123/video.mp4"
    video_uid: str  # 从 object_name 或 attributes 中提取
    etag: str  # 用于幂等
    generation: str  # GCS 对象版本
    event_time: datetime
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class Segment:
    """视频转录的时间片段"""
    t_start: float  # 开始时间（秒）
    t_end: float  # 结束时间（秒）
    text: str  # 转录文本
    lang: str = "en"
    speaker: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


## TODO: annotation seems not well-defined
@dataclass
class Annotation:
    """知识点标注"""
    fine_id: int  # 已存在的 fine_unit.id
    segment_index: int  # 对应的 segment 索引（在 segments 列表中的位置）
    score: float  # 置信度
    evidence: dict[str, Any]  # {span: [start, end], rationale: "...", tokens: [...]}

    def __post_init__(self):
        # 验证 evidence 必须包含 span
        if "span" not in self.evidence:
            raise ValueError("Annotation.evidence must contain 'span' key")


@dataclass
class ASRResult:
    """ASR 服务返回结果"""
    segments: list[Segment]
    asr_json_uri: str  # gs://bucket/path/asr.json
    vtt_uri: str  # gs://bucket/path/subs.vtt
    duration_seconds: float = 0.0


@dataclass
class TranscodeResult:
    """转码服务返回结果"""
    hls_path: str | None  # gs://bucket/path/master.m3u8 或 None（失败时）
    status: str  # 'success' | 'failed'
    error_message: str | None = None


@dataclass
class AgenticResult:
    """Agentic workflow 返回结果"""
    annotations: list[Annotation]
    method: str  # 'gemini_video' | 'gemini_text' | 'rule_fallback'
    ontology_ver: str  # 'gemini-2.0-20250110'


@dataclass
class IngestJob:
    """ingest_jobs 表的记录"""
    object_key: str
    etag: str
    video_uid: str
    video_id: int | None = None
    status: str = "queued"  # 'queued' | 'processing' | 'done' | 'error'
    retry_count: int = 0
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass
class ProcessingStats:
    """处理统计信息（用于日志/监控）"""
    video_uid: str
    processing_time_seconds: float
    asr_wall_seconds: float
    transcoder_wall_seconds: float
    agentic_wall_seconds: float
    video_duration_seconds: float
    segments_count: int
    occurrences_count: int
    fine_units_matched: int
    method: str
    ontology_ver: str