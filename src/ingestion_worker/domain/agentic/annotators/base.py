"""
标注器抽象基类

定义标注器的统一接口
"""
from abc import ABC, abstractmethod


class BaseAnnotator(ABC):
    """标注器基类"""

    @abstractmethod
    def build_prompt(self, segment: dict, segment_index: int) -> str:
        """
        构建 prompt（子类实现）

        Args:
            segment: Segment 数据 {start, end, text, ...}
            segment_index: Segment 索引

        Returns:
            Prompt 字符串
        """
        pass

    @abstractmethod
    def validate_annotation(self, ann: dict, segment: dict) -> bool:
        """
        验证 annotation（子类实现）

        Args:
            ann: Annotation 数据
            segment: Segment 数据

        Returns:
            True 如果有效，否则 False
        """
        pass

    @abstractmethod
    def get_output_schema(self) -> dict:
        """
        返回输出的 JSON schema（子类实现）

        Returns:
            JSON schema dict
        """
        pass

    @abstractmethod
    def get_kind(self) -> str:
        """
        返回标注类型（子类实现）

        Returns:
            'sense' | 'phrase_sense' | 'grammar_rule'
        """
        pass