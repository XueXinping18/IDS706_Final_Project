# ingestion_worker/infrastructure/lark.py

"""
职责：
- 发送 Lark Webhook 通知
- 支持多种消息类型（错误、警告、信息）

依赖：aiohttp

对外接口：
- async def send_notification(message_type, title, content, metadata)
"""

import aiohttp
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum

from ingestion_worker.config import Config
from ingestion_worker.utils.logging import get_logger


class LarkMessageType(Enum):
    """Lark 消息类型"""
    ERROR = "red"      # 红色（严重错误）
    WARNING = "orange" # 橙色（警告）
    INFO = "blue"      # 蓝色（信息）
    SUCCESS = "green"  # 绿色（成功）


class LarkError(Exception):
    """Lark 通知错误"""
    pass


class LarkClient:
    """Lark Webhook 客户端"""

    def __init__(self, config: Config):
        """
        初始化 Lark 客户端

        Args:
            config: 系统配置
        """
        self.config = config
        self.webhook_url = config.error_webhook_url
        self.logger = get_logger(__name__)

        if not self.webhook_url:
            self.logger.warning("Lark Webhook URL 未配置，通知将被跳过")

    async def send_notification(
        self,
        message_type: LarkMessageType,
        title: str,
        content: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        发送 Lark 通知

        Args:
            message_type: 消息类型（ERROR/WARNING/INFO/SUCCESS）
            title: 消息标题
            content: 消息内容（键值对）
            metadata: 可选的元数据（如时间戳）

        Returns:
            True 如果发送成功，否则 False

        Example:
            await lark.send_notification(
                LarkMessageType.WARNING,
                "短语匹配失败",
                {
                    "短语": "give up",
                    "视频": "abc-123",
                    "Segment": 5
                },
                {"timestamp": "2025-01-10T15:30:45"}
            )
        """
        if not self.webhook_url:
            self.logger.debug("Lark Webhook 未配置，跳过通知")
            return False

        try:
            # 构建消息卡片
            card = self._build_card(message_type, title, content, metadata)

            # 发送请求
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json={"msg_type": "interactive", "card": card},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        self.logger.debug(f"✓ Lark 通知已发送: {title}")
                        return True
                    else:
                        error_text = await resp.text()
                        self.logger.error(
                            f"✗ Lark 通知发送失败: HTTP {resp.status}, {error_text}"
                        )
                        return False

        except aiohttp.ClientError as e:
            self.logger.error(f"Lark Webhook 请求失败: {e}")
            return False
        except Exception as e:
            self.logger.error(f"发送 Lark 通知时出错: {e}")
            return False

    def _build_card(
        self,
        message_type: LarkMessageType,
        title: str,
        content: Dict[str, Any],
        metadata: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        构建 Lark 消息卡片

        Args:
            message_type: 消息类型
            title: 标题
            content: 内容字典
            metadata: 元数据

        Returns:
            Lark 卡片 JSON
        """
        # 图标映射
        icons = {
            LarkMessageType.ERROR: "❌",
            LarkMessageType.WARNING: "⚠️",
            LarkMessageType.INFO: "ℹ️",
            LarkMessageType.SUCCESS: "✅"
        }

        icon = icons.get(message_type, "📋")

        # 构建内容元素
        elements = []

        # 添加内容字段
        for key, value in content.items():
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**{key}**: `{value}`" if isinstance(value, str) else f"**{key}**: {value}"
                }
            })

        # 添加分隔线
        if metadata:
            elements.append({"tag": "hr"})

            # 添加元数据
            for key, value in metadata.items():
                elements.append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**{key}**: {value}"
                    }
                })

        # 添加时间戳
        elements.append({
            "tag": "note",
            "elements": [{
                "tag": "plain_text",
                "content": f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            }]
        })

        # 构建卡片
        return {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"{icon} {title}"
                },
                "template": message_type.value
            },
            "elements": elements
        }

    async def send_phrase_not_found(
        self,
        phrase: str,
        lang: str,
        video_uid: str,
        segment_index: int,
        segment_text: str,
        timestamp_range: str
    ):
        """
        发送短语未找到通知（便捷方法）

        Args:
            phrase: 短语
            lang: 语言
            video_uid: 视频 UID
            segment_index: Segment 索引
            segment_text: Segment 文本
            timestamp_range: 时间范围
        """
        await self.send_notification(
            LarkMessageType.WARNING,
            "短语匹配失败",
            {
                "短语": phrase,
                "语言": lang,
                "视频 UID": video_uid,
                "Segment #": segment_index,
                "时间范围": timestamp_range,
                "文本": f'"{segment_text}"'
            },
            {
                "建议": f"手动添加到数据库:\nINSERT INTO fine_unit (kind, label, lang, def, status) VALUES ('phrase_sense', '{phrase}', '{lang}', 'TODO', 'active');"
            }
        )

    async def send_word_not_found(
        self,
        word: str,
        pos: Optional[str],
        lang: str,
        video_uid: str,
        segment_index: int,
        segment_text: str
    ):
        """
        发送单词未找到通知（便捷方法）

        Args:
            word: 单词
            pos: 词性
            lang: 语言
            video_uid: 视频 UID
            segment_index: Segment 索引
            segment_text: Segment 文本
        """
        await self.send_notification(
            LarkMessageType.INFO,
            "单词未找到",
            {
                "单词": word,
                "词性": pos or "任意",
                "语言": lang,
                "视频 UID": video_uid,
                "Segment #": segment_index,
                "文本": f'"{segment_text}"'
            },
            {
                "说明": "单词未找到通常是正常情况（数据库不可能包含所有词）"
            }
        )

    async def send_error(
        self,
        error_type: str,
        error_message: str,
        context: Dict[str, Any]
    ):
        """
        发送错误通知（便捷方法）

        Args:
            error_type: 错误类型
            error_message: 错误消息
            context: 上下文信息
        """
        await self.send_notification(
            LarkMessageType.ERROR,
            f"系统错误: {error_type}",
            {
                "错误消息": error_message,
                **context
            }
        )