"""
职责：
- 定义业务异常层次结构

包含：
- WorkflowError (基类)
  - TranscodingError
  - ASRError
  - AgenticError
  - PersistenceError
"""


class WorkflowError(Exception):
    """工作流基础异常（所有业务异常的父类）"""
    def __init__(self, message: str, retryable: bool = True):
        super().__init__(message)
        self.message = message
        self.retryable = retryable  # 是否可重试


class IdempotencyError(WorkflowError):
    """幂等性检查失败"""
    def __init__(self, message: str):
        super().__init__(message, retryable=False)


class TranscodingError(WorkflowError):
    """转码失败（可重试）"""
    def __init__(self, message: str):
        super().__init__(message, retryable=True)


class ASRError(WorkflowError):
    """ASR 失败（可重试）"""
    def __init__(self, message: str):
        super().__init__(message, retryable=True)


class AgenticError(WorkflowError):
    """Agentic workflow 失败（可重试）"""
    def __init__(self, message: str):
        super().__init__(message, retryable=True)


class PersistenceError(WorkflowError):
    """数据持久化失败（可重试）"""
    def __init__(self, message: str):
        super().__init__(message, retryable=True)


class MCPError(WorkflowError):
    """MCP Server 调用失败（可重试）"""
    def __init__(self, message: str):
        super().__init__(message, retryable=True)


class ConfigError(Exception):
    """配置错误（不可重试，应用启动时检查）"""
    pass