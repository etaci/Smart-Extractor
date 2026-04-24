"""
商品详情数据模型

定义电商商品/产品详情页的提取 Schema。
"""

from typing import Optional

from pydantic import ConfigDict, Field

from smart_extractor.models.base import BaseExtractModel


class ProductDetail(BaseExtractModel):
    """商品/产品详情提取 Schema"""

    model_config = ConfigDict(
        json_schema_extra={"description": "电商商品或产品详情页的结构化数据模型"},
    )

    name: str = Field(
        description="商品名称"
    )
    price: str = Field(
        default="",
        description="商品价格（含币种符号），如 '199.00' 或 '$49.99'"
    )
    original_price: str = Field(
        default="",
        description="原价（如有折扣），如 '399.00'"
    )
    currency: str = Field(
        default="CNY",
        description="货币代码，如 CNY、USD、EUR"
    )
    description: str = Field(
        default="",
        description="商品描述/简介"
    )
    specifications: dict[str, str] = Field(
        default_factory=dict,
        description="商品规格参数，如 {'颜色': '黑色', '尺寸': 'XL', '材质': '棉'}"
    )
    rating: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=5.0,
        description="用户评分 (0-5)"
    )
    reviews_count: Optional[int] = Field(
        default=None,
        ge=0,
        description="评价数量"
    )
    availability: str = Field(
        default="",
        description="库存状态，如 '有货'、'缺货'、'预售'"
    )
    brand: str = Field(
        default="",
        description="品牌名称"
    )
    category: str = Field(
        default="",
        description="商品类目"
    )
    seller: str = Field(
        default="",
        description="卖家/店铺名称"
    )
    image_urls: list[str] = Field(
        default_factory=list,
        description="商品图片 URL 列表"
    )
