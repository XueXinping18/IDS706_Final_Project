"""
职责：
- 从环境变量加载配置
- 验证必需字段
- 提供类型安全的配置对象

输出：Config 类（dataclass）
"""
import os
import dataclasses
from dataclasses import dataclass
from typing import Optional


class ConfigError(Exception):
    """配置错误"""
    pass


@dataclass
class Config:
    """系统配置"""

    # ========================================
    # 必需字段（无默认值）
    # ========================================

    # GCP 基础
    gcp_project: str
    gcp_region: str

    # GCS Buckets
    raw_bucket: str  # 原始视频
    hls_bucket: str  # 转码后 HLS
    transcript_bucket: str  # 转录与字幕

    # Pub/Sub
    subscription_path: str  # 完整路径: projects/{project}/subscriptions/{name}

    # Transcoder API
    transcoder_template_id: str

    # Replicate (WhisperX)
    replicate_api_token: str

    # Vertex AI (Gemini)
    gemini_model: str


    # Database
    db_url: str  # postgresql://user:pass@host:port/db
    # webhook
    error_webhook_url: str # lark webhook
    # ========================================
    # 可选字段（有默认值）- 默认值只在这里定义一次
    # ========================================

    # WhisperX 配置
    whisperx_model: str = "victor-upmeet/whisperx:84d2ad2d6194fe98a17d2b60bef1c7f910c46b2f6fd38996ca457afd9c8abfcb"
    whisperx_audio_param: str = "audio_file"
    whisperx_batch_size: int = 64
    whisperx_vad_onset: float = 0.5
    whisperx_vad_offset: float = 0.363
    whisperx_temperature: int = 0
    whisperx_align_output: bool = True
    whisperx_diarization: bool = False
    whisperx_debug: bool = False
    whisperx_language_detection_min_prob: int = 0
    whisperx_language_detection_max_tries: int = 5

    # Database
    db_pool_size: int = 10

    # Retry & Timeout
    max_retries: int = 3
    retry_backoff_seconds: int = 1
    signed_url_ttl_seconds: int = 7200  # 2 hours
    processing_timeout_seconds: int = 3600  # 1 hour
    gemini_timeout_seconds: int = 180

    # Gemini 并发配置
    gemini_max_concurrency: int = 20  # 最大并发任务数
    gemini_cache_ttl_seconds: int = 3600  # Cached Content TTL (1 hour)

    # agent tool use
    mcp_endpoint: Optional[str] = None  # For future remote MCP server

    @classmethod
    def from_env(cls) -> "Config":
        """从环境变量加载配置"""

        def require(key: str) -> str:
            """获取必需的环境变量"""
            value = os.getenv(key)
            if not value:
                raise ConfigError(f"Missing required environment variable: {key}")
            return value

        def get_default(field_name: str):
            """从 dataclass 字段定义中获取默认值（单一数据源）"""
            field = cls.__dataclass_fields__[field_name]
            if field.default is not dataclasses.MISSING:
                return field.default
            elif field.default_factory is not dataclasses.MISSING:
                return field.default_factory()
            else:
                raise ConfigError(f"Field {field_name} has no default value")

        def optional(key: str, field_name: str) -> str:
            """获取可选的环境变量"""
            value = os.getenv(key)
            return value if value is not None else get_default(field_name)

        def optional_int(key: str, field_name: str) -> int:
            """获取可选的整数环境变量"""
            value = os.getenv(key)
            if value is None:
                return get_default(field_name)
            try:
                return int(value)
            except ValueError:
                raise ConfigError(f"Invalid integer value for {key}: {value}")

        def optional_float(key: str, field_name: str) -> float:
            """获取可选的浮点数环境变量"""
            value = os.getenv(key)
            if value is None:
                return get_default(field_name)
            try:
                return float(value)
            except ValueError:
                raise ConfigError(f"Invalid float value for {key}: {value}")

        def optional_bool(key: str, field_name: str) -> bool:
            """获取可选的布尔环境变量"""
            value = os.getenv(key)
            if value is None:
                return get_default(field_name)
            return value.lower() in ('true', '1', 'yes', 'on')

        # 构建 subscription_path
        project = require("GCP_PROJECT")
        subscription_name = require("PUBSUB_SUBSCRIPTION_NAME")
        subscription_path = f"projects/{project}/subscriptions/{subscription_name}"

        return cls(
            # GCP
            gcp_project=project,
            gcp_region=require("GCP_REGION"),

            # GCS
            raw_bucket=require("RAW_BUCKET"),
            hls_bucket=require("HLS_BUCKET"),
            transcript_bucket=require("TRANSCRIPT_BUCKET"),

            # Pub/Sub
            subscription_path=subscription_path,

            # Transcoder
            transcoder_template_id=optional("TRANSCODER_TEMPLATE_ID", "transcoder_template_id"),

            # Replicate
            replicate_api_token=require("REPLICATE_API_TOKEN"),

            # WhisperX 配置
            whisperx_model=optional("WHISPERX_MODEL", "whisperx_model"),
            whisperx_audio_param=optional("WHISPERX_AUDIO_PARAM", "whisperx_audio_param"),
            whisperx_batch_size=optional_int("WHISPERX_BATCH_SIZE", "whisperx_batch_size"),
            whisperx_vad_onset=optional_float("WHISPERX_VAD_ONSET", "whisperx_vad_onset"),
            whisperx_vad_offset=optional_float("WHISPERX_VAD_OFFSET", "whisperx_vad_offset"),
            whisperx_temperature=optional_int("WHISPERX_TEMPERATURE", "whisperx_temperature"),
            whisperx_align_output=optional_bool("WHISPERX_ALIGN_OUTPUT", "whisperx_align_output"),
            whisperx_diarization=optional_bool("WHISPERX_DIARIZATION", "whisperx_diarization"),
            whisperx_debug=optional_bool("WHISPERX_DEBUG", "whisperx_debug"),
            whisperx_language_detection_min_prob=optional_int("WHISPERX_LANGUAGE_DETECTION_MIN_PROB", "whisperx_language_detection_min_prob"),
            whisperx_language_detection_max_tries=optional_int("WHISPERX_LANGUAGE_DETECTION_MAX_TRIES", "whisperx_language_detection_max_tries"),

            # Vertex AI
            gemini_model=optional("GEMINI_MODEL", "gemini_model"),
            gemini_timeout_seconds=optional_int("GEMINI_TIMEOUT_SECONDS", "gemini_timeout_seconds"),
            gemini_max_concurrency=optional_int("GEMINI_MAX_CONCURRENCY", "gemini_max_concurrency"),
            gemini_cache_ttl_seconds=optional_int("GEMINI_CACHE_TTL_SECONDS", "gemini_cache_ttl_seconds"),
            mcp_endpoint=optional("MCP_ENDPOINT", "mcp_endpoint"),

            # Database
            db_url=require("DATABASE_URL"),
            db_pool_size=optional_int("DB_POOL_SIZE", "db_pool_size"),

            # Retry
            max_retries=optional_int("MAX_RETRIES", "max_retries"),
            retry_backoff_seconds=optional_int("RETRY_BACKOFF_SECONDS", "retry_backoff_seconds"),
            signed_url_ttl_seconds=optional_int("SIGNED_URL_TTL_SECONDS", "signed_url_ttl_seconds"),
            processing_timeout_seconds=optional_int("PROCESSING_TIMEOUT_SECONDS", "processing_timeout_seconds"),

            # Webhook
            error_webhook_url=require("WEBHOOK_URL"),
        )

    def validate(self) -> None:
        """验证配置的合理性"""
        if self.gemini_timeout_seconds <= 0:
            raise ConfigError("GEMINI_TIMEOUT_SECONDS must be positive")

        if self.gemini_max_concurrency <= 0:
            raise ConfigError("GEMINI_MAX_CONCURRENCY must be positive")

        if self.gemini_cache_ttl_seconds <= 0:
            raise ConfigError("GEMINI_CACHE_TTL_SECONDS must be positive")

        if self.db_pool_size <= 0:
            raise ConfigError("DB_POOL_SIZE must be positive")

        if self.max_retries < 0:
            raise ConfigError("MAX_RETRIES must be non-negative")

        if not self.db_url.startswith("postgresql://"):
            raise ConfigError("DATABASE_URL must start with 'postgresql://'")

        if self.mcp_endpoint is not None and not self.mcp_endpoint.startswith("http"):
            raise ConfigError("MCP_ENDPOINT must be a valid HTTP(S) URL")

        # WhisperX 参数验证
        if self.whisperx_batch_size <= 0:
            raise ConfigError("WHISPERX_BATCH_SIZE must be positive")

        if not (0 <= self.whisperx_vad_onset <= 1):
            raise ConfigError("WHISPERX_VAD_ONSET must be between 0 and 1")

        if not (0 <= self.whisperx_vad_offset <= 1):
            raise ConfigError("WHISPERX_VAD_OFFSET must be between 0 and 1")


# 业务配置常量（不从环境变量读取，直接硬编码）
MAX_TEXT_TOKENS = 8000