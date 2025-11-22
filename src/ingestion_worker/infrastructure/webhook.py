"""
Pub/Sub Push Webhook 工具

职责：
- 解析 Pub/Sub Push 请求
- 验证签名（可选）
"""

import base64
import json
from datetime import datetime
from typing import Optional

from ingestion_worker.types import PubSubMessage
from ingestion_worker.utils.logging import get_logger


class WebhookError(Exception):
    """Webhook 解析错误"""
    pass


def parse_pubsub_push(body: dict) -> PubSubMessage:
    """
    解析 Pub/Sub Push 请求

    Args:
        body: FastAPI request.json() 的结果

    Returns:
        PubSubMessage

    Raises:
        WebhookError: 解析失败
    """
    logger = get_logger(__name__)

    try:
        # 提取 message
        message = body.get("message")
        if not message:
            raise WebhookError("Missing 'message' field")

        # 解码 data（base64）
        data_b64 = message.get("data", "")
        data_bytes = base64.b64decode(data_b64)
        data = json.loads(data_bytes)

        # 提取字段
        bucket = data.get("bucket")
        object_name = data.get("name")

        if not bucket or not object_name:
            raise WebhookError(f"Missing bucket or name: {data}")

        # 构造 PubSubMessage
        video_uid = _extract_video_uid(object_name)

        return PubSubMessage(
            bucket=bucket,
            object_name=object_name,
            video_uid=video_uid,
            etag=data.get("etag", ""),
            generation=data.get("generation", ""),
            event_time=datetime.fromisoformat(
                data.get("timeCreated", datetime.now().isoformat()).replace("Z", "+00:00")
            ),
            attributes=dict(message.get("attributes", {}))
        )

    except json.JSONDecodeError as e:
        raise WebhookError(f"Failed to parse JSON: {e}") from e
    except Exception as e:
        raise WebhookError(f"Failed to parse message: {e}") from e


import uuid

def _extract_video_uid(object_name: str) -> str:
    """从对象名提取 video_uid，确保返回有效的 UUID"""
    parts = object_name.split("/")
    
    # 1. 尝试从路径提取 (e.g. uploads/UUID/video.mp4)
    candidate = None
    if len(parts) >= 2 and parts[0] == "uploads":
        candidate = parts[1]
    else:
        # 2. 尝试使用文件名作为 candidate
        candidate = object_name.replace("/", "-")

    # 3. 验证是否为有效 UUID
    try:
        uuid.UUID(candidate)
        return candidate
    except ValueError:
        # 4. 如果不是有效 UUID，则生成确定性 UUID (UUID5)
        # 使用 URL namespace + object_name 确保同一个文件总是生成相同的 ID
        generated_uuid = uuid.uuid5(uuid.NAMESPACE_URL, object_name)
        return str(generated_uuid)