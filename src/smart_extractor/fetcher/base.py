"""
抓取器基类

定义网页抓取器的抽象接口和结果数据模型。
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class FetchResult(BaseModel):
    """网页抓取结果"""
    url: str = Field(description="请求的 URL")
    html: str = Field(default="", description="页面 HTML 源码")
    status_code: int = Field(default=0, description="HTTP 状态码")
    fetched_at: datetime = Field(default_factory=datetime.now, description="抓取时间")
    headers: dict[str, str] = Field(default_factory=dict, description="响应头")
    error: Optional[str] = Field(default=None, description="错误信息（如果抓取失败）")
    elapsed_ms: float = Field(default=0.0, description="耗时（毫秒）")
    is_shell_page: bool = Field(default=False, description="是否疑似前端壳页")

    @property
    def is_success(self) -> bool:
        """判断抓取是否成功"""
        return self.error is None and 200 <= self.status_code < 400


class BaseFetcher(ABC):
    """
    网页抓取器抽象基类。

    所有抓取器实现（Playwright、httpx 等）都需要继承此类。
    """

    @abstractmethod
    def fetch(self, url: str) -> FetchResult:
        """
        抓取指定 URL 的网页内容。

        Args:
            url: 目标 URL

        Returns:
            FetchResult 抓取结果
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """释放资源（如关闭浏览器实例）"""
        ...

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
