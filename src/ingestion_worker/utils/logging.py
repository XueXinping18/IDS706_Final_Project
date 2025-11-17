"""
职责：
- 配置结构化日志（JSON 格式）
- 添加 correlation_id（基于 video_uid）

输出：
- setup_logging(config: Config) -> None
- get_logger(name: str) -> logging.Logger
"""

import logging
import sys
from typing import Optional
from contextvars import ContextVar

# 使用 contextvars 存储当前请求的 correlation_id（线程安全 + asyncio 友好）
correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


class StructuredFormatter(logging.Formatter):
    """结构化日志格式化器（简化版，生产环境可用 python-json-logger）"""

    def format(self, record: logging.LogRecord) -> str:
        """
        格式化日志记录

        输出格式：
        [2025-01-10 10:30:45] [INFO] [video-123] [workflow.py:45] 开始处理视频
        """
        # 获取 correlation_id
        correlation_id = correlation_id_var.get()
        correlation_str = f"[{correlation_id}] " if correlation_id else ""

        # 获取调用位置
        location = f"[{record.filename}:{record.lineno}]"

        # 组装消息
        timestamp = self.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S")
        level = f"[{record.levelname}]"

        message = f"[{timestamp}] {level} {correlation_str}{location} {record.getMessage()}"

        # 如果有异常，添加堆栈
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)

        return message


def setup_logging(level: str = "INFO") -> None:
    """
    配置全局日志

    Args:
        level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # 获取 root logger
    root_logger = logging.getLogger()

    # 清除已有的 handlers（避免重复配置）
    root_logger.handlers.clear()

    # 设置日志级别
    log_level = getattr(logging, level.upper(), logging.INFO)
    root_logger.setLevel(log_level)

    # 创建 console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # 设置格式化器
    formatter = StructuredFormatter()
    console_handler.setFormatter(formatter)

    # 添加到 root logger
    root_logger.addHandler(console_handler)

    # 抑制第三方库的 DEBUG 日志（避免噪音）
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的 logger

    Args:
        name: logger 名称（通常是模块名 __name__）

    Returns:
        Logger 实例
    """
    return logging.getLogger(name)


def set_correlation_id(video_uid: str) -> None:
    """
    设置当前上下文的 correlation_id

    Args:
        video_uid: 视频唯一标识
    """
    correlation_id_var.set(video_uid)


def clear_correlation_id() -> None:
    """清除 correlation_id"""
    correlation_id_var.set(None)


# 便捷函数：带 correlation_id 的日志记录
def log_with_context(logger: logging.Logger, level: str, video_uid: str, message: str) -> None:
    """
    记录带 correlation_id 的日志（临时设置，不影响其他异步任务）

    Args:
        logger: Logger 实例
        level: 日志级别 (debug, info, warning, error, critical)
        video_uid: 视频唯一标识
        message: 日志消息
    """
    # 临时设置 correlation_id
    token = correlation_id_var.set(video_uid)
    try:
        log_func = getattr(logger, level.lower())
        log_func(message)
    finally:
        # 恢复原值
        correlation_id_var.reset(token)