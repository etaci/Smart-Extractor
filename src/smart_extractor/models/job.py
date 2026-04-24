"""
招聘信息数据模型

定义招聘/求职信息的提取 Schema。
"""

from pydantic import ConfigDict, Field

from smart_extractor.models.base import BaseExtractModel


class JobPosting(BaseExtractModel):
    """招聘信息提取 Schema"""

    model_config = ConfigDict(
        json_schema_extra={"description": "招聘岗位/求职信息的结构化数据模型"},
    )

    title: str = Field(
        description="职位名称"
    )
    company: str = Field(
        default="",
        description="公司名称"
    )
    location: str = Field(
        default="",
        description="工作地点"
    )
    salary_range: str = Field(
        default="",
        description="薪资范围，如 '15k-25k' 或 '面议'"
    )
    job_type: str = Field(
        default="",
        description="工作类型，如 '全职'、'兼职'、'实习'"
    )
    experience_required: str = Field(
        default="",
        description="经验要求，如 '3-5年'、'应届生'"
    )
    education_required: str = Field(
        default="",
        description="学历要求，如 '本科'、'硕士'"
    )
    description: str = Field(
        default="",
        description="职位描述"
    )
    requirements: list[str] = Field(
        default_factory=list,
        description="任职要求列表"
    )
    benefits: list[str] = Field(
        default_factory=list,
        description="福利待遇列表"
    )
    skills: list[str] = Field(
        default_factory=list,
        description="要求的技能标签"
    )
    posted_date: str = Field(
        default="",
        description="发布日期"
    )
    contact: str = Field(
        default="",
        description="联系方式"
    )
