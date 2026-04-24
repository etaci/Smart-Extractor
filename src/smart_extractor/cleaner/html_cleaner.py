"""
HTML 清洗模块

将原始 HTML 转换为干净的纯文本，剔除无用标签，
保留语义结构，控制输出长度以适配 LLM token 限制。
"""

from typing import Optional

from bs4 import BeautifulSoup, Comment
from loguru import logger

from smart_extractor.config import CleanerConfig


# 精确匹配：class 名必须完全等于这些关键词才会被移除
_NOISE_CLASS_KEYWORDS = (
    "dropdown",
    "menu",
    "search-hot",
    "search-form",
    "search-box",
    "column-search-panel",
    "login-step",
    "login-form",
    "guide-download-app",
    "side-entry",
    "hot-job-box",
    "sidebar",
    "comments",
    "popup",
    "modal",
    "tooltip",
    "ad",
    "advertisement",
    "share-box",
    "cookie-banner",
)

# 前缀/子串匹配：class 名以这些前缀开头或包含这些子串就会被移除
# 针对维基百科、知乎等百科/文档类网站的提示框、导航框、信息框
_NOISE_CLASS_PREFIXES = (
    "ambox",       # 维基百科文章提示框 (ambox-notice, ambox-content 等)
    "mbox",        # 维基百科消息框
    "navbox",      # 维基百科导航框
    "hatnote",     # 维基百科顶部消跧提示
    "infobox",     # 维基百科/百度百科信息框
    "metadata",    # 元数据信息
    "noprint",     # 不打印区域
    "mw-editsection",  # 维基编辑按钮
    "mw-jump-link",    # 维基跳转链接
    "shortdescription",# 维基短描述
    "sistersitebox",   # 维基姊妹站点
    "catlinks",    # 维基分类链接
    "reflist",     # 维基参考文献列表
    "reference",   # 参考文献
    "toc",         # 目录
    "toccolours",  # 目录颜色
    "message-box", # 通用消息框
    "banner",      # 横幅
    "notice",      # 提醒
    "disclaimer",  # 免责声明
)

_PRIORITY_BLOCK_SELECTORS = (
    ".job-info",
    ".company-job-item",
    "[class*='job-card']",
    "[class*='job-list'] [class*='job']",
    "[class*='position'] [class*='job']",
)


class HTMLCleaner:
    """
    HTML → 纯文本清洗器。

    功能：
    - 剔除 script/style/nav 等无用标签
    - 移除 HTML 注释
    - 保留标题、列表、表格的语义结构
    - 智能截断控制文本长度
    - 支持 CSS 选择器过滤特定区域
    """

    def __init__(self, config: Optional[CleanerConfig] = None):
        self._config = config or CleanerConfig()

    def clean(
        self,
        html: str,
        selector: Optional[str] = None,
        max_length: Optional[int] = None,
    ) -> str:
        """
        清洗 HTML 并返回纯文本。

        Args:
            html: 原始 HTML 字符串
            selector: 可选的 CSS 选择器，仅提取匹配区域的内容
            max_length: 最大文本长度，覆盖配置中的默认值

        Returns:
            清洗后的纯文本
        """
        if not html or not html.strip():
            logger.warning("输入 HTML 为空")
            return ""

        max_len = max_length or self._config.max_text_length

        # 解析 HTML
        soup = BeautifulSoup(html, "lxml")

        # 如果指定了选择器，仅提取匹配区域
        if selector:
            target = soup.select_one(selector)
            if target:
                soup = BeautifulSoup(str(target), "lxml")
                logger.debug("使用 CSS 选择器 '{}' 过滤内容", selector)
            else:
                logger.warning("CSS 选择器 '{}' 未匹配到任何元素，使用全页面", selector)

        # 移除无用标签
        for tag_name in self._config.remove_tags:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        # 移除 HTML 注释
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # 移除隐藏元素（display:none, visibility:hidden）
        for tag in soup.find_all(style=True):
            if tag.attrs is None:
                continue
            style = tag.get("style", "").lower()
            if "display:none" in style or "display: none" in style:
                tag.decompose()
            elif "visibility:hidden" in style or "visibility: hidden" in style:
                tag.decompose()

        # 根据配置选择提取策略
        self._remove_noise_blocks(soup)

        priority_text = self._extract_priority_blocks(soup)
        if priority_text:
            text = priority_text
        elif self._config.keep_structure:
            text = self._extract_structured(soup)
        else:
            text = self._extract_plain(soup)

        # 清理空白行和多余空格
        text = self._normalize_whitespace(text)

        # 智能截断
        if len(text) > max_len:
            text = self._smart_truncate(text, max_len)
            logger.debug("文本已截断至 {} 字符", max_len)

        original_len = len(html)
        result_len = len(text)
        ratio = (result_len / original_len * 100) if original_len > 0 else 0
        logger.info(
            "HTML 清洗完成: {} → {} 字符 (压缩率 {:.1f}%)",
            original_len, result_len, ratio
        )

        return text

    def _extract_structured(self, soup: BeautifulSoup) -> str:
        """提取保留语义结构的文本"""
        lines = []

        for element in soup.find_all(True):
            tag = element.name

            # 标题标签：添加标记
            if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
                level = int(tag[1])
                prefix = "#" * level
                text = element.get_text(strip=True)
                if text:
                    lines.append(f"\n{prefix} {text}\n")

            # 段落标签
            elif tag == "p":
                text = element.get_text(strip=True)
                if text:
                    lines.append(text + "\n")

            # 列表项
            elif tag == "li":
                text = element.get_text(strip=True)
                if text:
                    lines.append(f"  • {text}")

            # 表格行
            elif tag == "tr":
                cells = [td.get_text(strip=True) for td in element.find_all(["td", "th"])]
                if any(cells):
                    lines.append(" | ".join(cells))

        if lines:
            return "\n".join(lines)

        # 回退到纯文本模式
        return self._extract_plain(soup)

    def _remove_noise_blocks(self, soup: BeautifulSoup) -> None:
        """移除噪音区块：精确匹配 + 前缀匹配双重策略。"""
        for tag in soup.find_all(True):
            if tag.attrs is None:
                continue

            # ---- 基于 class 过滤 ----
            class_names = [str(name).strip().lower() for name in tag.get("class", []) if str(name).strip()]
            if class_names:
                # 精确匹配
                if any(cn == kw for cn in class_names for kw in _NOISE_CLASS_KEYWORDS):
                    tag.decompose()
                    continue
                # 前缀匹配（针对百科类提示框）
                if any(cn.startswith(prefix) for cn in class_names for prefix in _NOISE_CLASS_PREFIXES):
                    tag.decompose()
                    continue

            # ---- 基于 id 过滤 ----
            tag_id = str(tag.get("id", "")).strip().lower()
            if tag_id and any(tag_id.startswith(prefix) for prefix in ("toc", "catlinks", "mw-panel", "mw-navigation", "footer")):
                tag.decompose()
                continue

            # ---- 基于 role 过滤 ----
            role = str(tag.get("role", "")).strip().lower()
            if role in ("navigation", "banner", "complementary"):
                tag.decompose()
                continue

    def _extract_priority_blocks(self, soup: BeautifulSoup) -> str:
        blocks: list[str] = []
        seen: set[str] = set()

        for selector in _PRIORITY_BLOCK_SELECTORS:
            try:
                candidates = soup.select(selector)
            except Exception as exc:
                logger.debug("优先块选择器 '{}' 解析失败，跳过: {}", selector, exc)
                continue

            for node in candidates:
                text = node.get_text("\n", strip=True)
                normalized = self._normalize_whitespace(text)
                if len(normalized) < 12 or normalized in seen:
                    continue
                seen.add(normalized)
                blocks.append(normalized)
                if len(blocks) >= 24:
                    break

            if len(blocks) >= 3:
                logger.debug("浣跨敤浼樺厛鍐呭鍧楅€夋嫨鍣? '{}' 鎻愬彇 {} 涓潡", selector, len(blocks))
                return "\n\n".join(blocks)

        return ""

    def _extract_plain(self, soup: BeautifulSoup) -> str:
        """提取纯文本（不保留结构）"""
        return soup.get_text(separator="\n", strip=True)

    def _normalize_whitespace(self, text: str) -> str:
        """规范化空白字符"""
        import re
        # 合并连续空白行为最多两个换行
        text = re.sub(r"\n{3,}", "\n\n", text)
        # 移除行首行尾空格
        lines = [line.strip() for line in text.split("\n")]
        # 移除连续空行
        result = []
        prev_empty = False
        for line in lines:
            if not line:
                if not prev_empty:
                    result.append("")
                    prev_empty = True
            else:
                result.append(line)
                prev_empty = False

        return "\n".join(result).strip()

    def _smart_truncate(self, text: str, max_length: int) -> str:
        """
        智能截断文本。

        优先在段落边界截断，避免截断到句子中间。
        """
        if len(text) <= max_length:
            return text

        # 在最大长度附近找段落边界
        truncated = text[:max_length]

        # 优先在段落边界（双换行）截断
        last_para = truncated.rfind("\n\n")
        if last_para > max_length * 0.7:
            return truncated[:last_para].strip() + "\n\n[... 内容已截断 ...]"

        # 其次在句号处截断
        for sep in ["。", ".", "！", "!", "？", "?"]:
            last_sentence = truncated.rfind(sep)
            if last_sentence > max_length * 0.8:
                return truncated[:last_sentence + 1].strip() + "\n\n[... 内容已截断 ...]"

        # 最后在换行处截断
        last_line = truncated.rfind("\n")
        if last_line > max_length * 0.8:
            return truncated[:last_line].strip() + "\n\n[... 内容已截断 ...]"

        return truncated.strip() + "\n\n[... 内容已截断 ...]"
