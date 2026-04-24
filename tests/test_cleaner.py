"""
HTMLCleaner 单元测试

测试 HTML 清洗的各种场景：标签移除、结构保留、截断、选择器等。
"""

import pytest
from smart_extractor.cleaner.html_cleaner import HTMLCleaner
from smart_extractor.config import CleanerConfig


class TestHTMLCleaner:
    """HTMLCleaner 核心功能测试"""

    def setup_method(self):
        """每个测试前初始化"""
        self.cleaner = HTMLCleaner()

    def test_basic_clean(self, sample_html):
        """测试基本 HTML 清洗"""
        result = self.cleaner.clean(sample_html)
        assert result  # 结果不为空
        assert "测试文章标题" in result
        assert "正文内容" in result

    def test_remove_script_tags(self, sample_html):
        """测试移除 script 标签"""
        result = self.cleaner.clean(sample_html)
        assert "var x = 1" not in result

    def test_remove_style_tags(self, sample_html):
        """测试移除 style 标签"""
        result = self.cleaner.clean(sample_html)
        assert "color: red" not in result

    def test_remove_nav_footer(self, sample_html):
        """测试移除导航和页脚"""
        result = self.cleaner.clean(sample_html)
        assert "导航栏" not in result
        assert "页脚内容" not in result

    def test_remove_aside(self, sample_html):
        """测试移除侧边栏"""
        result = self.cleaner.clean(sample_html)
        assert "侧边栏" not in result

    def test_preserve_structure(self, sample_html):
        """测试保留标题结构（Markdown 格式）"""
        config = CleanerConfig(keep_structure=True)
        cleaner = HTMLCleaner(config)
        result = cleaner.clean(sample_html)
        # 结构化模式下标题应以 # 开头
        assert "# 测试文章标题" in result

    def test_plain_text_mode(self, sample_html):
        """测试纯文本模式（不保留结构）"""
        config = CleanerConfig(keep_structure=False)
        cleaner = HTMLCleaner(config)
        result = cleaner.clean(sample_html)
        assert result
        assert "测试文章标题" in result
        # 纯文本模式下不应有 # 标记
        assert "# 测试文章标题" not in result

    def test_empty_input(self):
        """测试空输入"""
        assert self.cleaner.clean("") == ""
        assert self.cleaner.clean("   ") == ""

    def test_css_selector_valid(self, sample_html):
        """测试有效的 CSS 选择器"""
        result = self.cleaner.clean(sample_html, selector="main")
        assert "测试文章标题" in result
        assert result  # 选择器匹配到了内容

    def test_css_selector_no_match(self, sample_html):
        """测试不匹配的 CSS 选择器（回退到全页面）"""
        result = self.cleaner.clean(sample_html, selector=".nonexistent-class")
        # 未匹配时应回退全页面，仍然有内容
        assert result

    def test_max_length_truncation(self, sample_html):
        """测试文本截断"""
        result = self.cleaner.clean(sample_html, max_length=50)
        assert len(result) <= 80  # 允许截断标记的额外长度

    def test_hidden_elements_removed(self):
        """测试移除隐藏元素"""
        html = '<div><p style="display:none">隐藏内容</p><p>可见内容</p></div>'
        result = self.cleaner.clean(html)
        assert "隐藏内容" not in result
        assert "可见内容" in result

    def test_html_comments_removed(self):
        """测试移除 HTML 注释"""
        html = "<div><!-- 这是注释 -->正文</div>"
        result = self.cleaner.clean(html)
        assert "这是注释" not in result
        assert "正文" in result

    def test_list_items_preserved(self, sample_html):
        """测试列表项在结构化模式下保留"""
        result = self.cleaner.clean(sample_html)
        assert "要点一" in result
        assert "要点二" in result

    def test_table_content_preserved(self, sample_html):
        """测试表格内容"""
        result = self.cleaner.clean(sample_html)
        assert "数据A" in result

    def test_whitespace_normalization(self):
        """测试空白字符规范化"""
        html = "<p>行1</p>\n\n\n\n\n<p>行2</p>"
        result = self.cleaner.clean(html)
        # 不应有过多连续空行
        assert "\n\n\n" not in result

    def test_custom_remove_tags(self):
        """测试自定义移除标签"""
        config = CleanerConfig(remove_tags=["div"])
        cleaner = HTMLCleaner(config)
        html = "<div>被移除</div><span>被保留</span>"
        result = cleaner.clean(html)
        assert "被移除" not in result
        assert "被保留" in result


    def test_priority_job_blocks_override_navigation_noise(self):
        """职位卡片应优先于菜单噪音块"""
        html = """
        <html><body>
          <div class="home-job-menu">Python Java C++ Go Rust 前端 测试 运维 产品 销售</div>
          <div class="position-select-dropdown__list">图像算法 自然语言处理 推荐算法 搜索算法</div>
          <div class="job-info">Python工程师 南京 1-3年 本科</div>
          <div class="job-info">Java工程师 南京 3-5年 本科</div>
          <div class="job-info">算法工程师 南京 经验不限 硕士</div>
        </body></html>
        """
        result = self.cleaner.clean(html)
        assert "Python工程师" in result
        assert "Java工程师" in result
        assert "算法工程师" in result
        assert "自然语言处理" not in result


class TestHTMLCleanerEdgeCases:
    """HTMLCleaner 边界条件测试"""

    def setup_method(self):
        self.cleaner = HTMLCleaner()

    def test_only_script_page(self):
        """测试纯 script 页面"""
        html = "<html><body><script>alert(1);</script></body></html>"
        result = self.cleaner.clean(html)
        assert "alert" not in result

    def test_deeply_nested_html(self):
        """测试深度嵌套 HTML"""
        html = "<div>" * 20 + "内容" + "</div>" * 20
        result = self.cleaner.clean(html)
        assert "内容" in result

    def test_unicode_content(self):
        """测试 Unicode 内容"""
        html = "<p>Hello World ---- 你好世界</p>"
        result = self.cleaner.clean(html)
        assert "Hello World" in result
        assert "你好世界" in result

    def test_minimal_html(self, sample_html_minimal):
        """测试最小 HTML"""
        result = self.cleaner.clean(sample_html_minimal)
        assert "标题" in result
        assert "正文内容" in result
