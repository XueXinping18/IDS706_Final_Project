"""
Webhook API 端点

职责：
- 接收 Pub/Sub Push 请求
- 调用 Workflow 异步处理
"""

import asyncio
from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import JSONResponse

from ingestion_worker.infrastructure.webhook import parse_pubsub_push, WebhookError
from ingestion_worker.application.workflow import IngestVideoWorkflow
from ingestion_worker.utils.logging import get_logger, set_correlation_id

router = APIRouter()
logger = get_logger(__name__)


def get_workflow(request: Request) -> IngestVideoWorkflow:
    """Get workflow from app state (dependency injection)"""
    if not hasattr(request.app.state, 'workflow'):
        raise HTTPException(status_code=500, detail="Workflow not initialized")
    return request.app.state.workflow


@router.post("/webhooks/video-ingestion")
async def handle_video_ingestion(
    request: Request,
    workflow: IngestVideoWorkflow = Depends(get_workflow)
):
    """
    处理 Pub/Sub Push 请求

    Pub/Sub 会 POST 到这里，格式：
    {
      "message": {
        "data": "base64-encoded-json",
        "messageId": "...",
        "publishTime": "..."
      },
      "subscription": "..."
    }
    """
    try:
        # 1. 解析请求
        body = await request.json()
        message = parse_pubsub_push(body)

        # 2. 设置日志 correlation_id
        set_correlation_id(message.video_uid)

        logger.info(
            f"📥 收到 Pub/Sub Push: video_uid={message.video_uid}, "
            f"object={message.object_name}"
        )

        # 3. 异步处理（不阻塞响应）
        asyncio.create_task(_process_async(message, workflow))

        # 4. 立即返回 200（告诉 Pub/Sub 消息已接收）
        return JSONResponse(
            status_code=200,
            content={"status": "accepted", "video_uid": message.video_uid}
        )

    except WebhookError as e:
        logger.error(f"✗ Webhook 解析失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        logger.error(f"✗ Webhook 处理失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


async def _process_async(message, workflow: IngestVideoWorkflow):
    """异步处理视频（后台任务）"""
    try:
        await workflow.process_message(message)
        logger.info(f"✓ 视频处理完成: {message.video_uid}")

    except Exception as e:
        logger.error(f"✗ 视频处理失败: {e}", exc_info=True)
        # 注意：Push 模式下，失败不会自动重试
        # 需要在 Workflow 内部实现重试逻辑