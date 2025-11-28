"""
语法标注器（grammar_rule）

职责：
- 构建语法标注的 prompt
- 识别语法规则的使用（如时态、语态、从句）
- 评估语法点的 comprehensibility

注意：当前版本暂不实现，预留接口
"""
from ingestion_worker.domain.agentic.annotators.base import BaseAnnotator
from ingestion_worker.utils.logging import get_logger


class GrammarAnnotator(BaseAnnotator):
    """语法标注器（预留，暂不实现）"""

    def __init__(self):
        self.logger = get_logger(__name__)

    def get_kind(self) -> str:
        """返回标注类型"""
        return "grammar_rule"

    def build_prompt(self, segment: dict, segment_index: int) -> str:
        """构建语法标注的 prompt（暂不实现）"""
        raise NotImplementedError(
            "Grammar annotation not yet implemented. "
            "This feature is reserved for future development."
        )

    def validate_annotation(self, ann: dict, segment: dict) -> bool:
        """验证语法标注（暂不实现）"""
        raise NotImplementedError(
            "Grammar annotation not yet implemented. "
            "This feature is reserved for future development."
        )

    def get_output_schema(self) -> dict:
        """返回输出 schema（暂不实现）"""
        raise NotImplementedError(
            "Grammar annotation not yet implemented. "
            "This feature is reserved for future development."
        )