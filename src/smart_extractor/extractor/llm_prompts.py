"""LLM 抽取相关提示词。"""

DEFAULT_PROMPT_TEMPLATE = """请从以下文本中提取结构化信息，严格按给定模型输出。
如果字段找不到，请使用空字符串、空列表或默认值，不要编造信息。

--- 待提取文本 ---
{text}
"""

AUTO_ANALYZE_PROMPT_TEMPLATE = """你是一个网页信息抽取引擎。
请分析下面这个网页文本，自动判断页面类型，并抽取最关键的信息。

要求：
1. 只输出 JSON 对象，不要输出 markdown。
2. 顶层必须包含 page_type、candidate_fields、selected_fields、field_labels、data。
3. page_type 仅可使用：job、news、product、article、blog、video、forum、profile、listing、unknown。
4. 当页面为新闻、杂谈、百科或难以明确分类但有一定阅读文本的网页时，请将其作为 article 或 blog 处理，严禁轻易设为 unknown。
5. candidate_fields 是你认为该网页最值得抽取的字段英文名列表，至少提供 2 到 8 个。即使是 unknown 也请尽力提供 title 和 content。
6. selected_fields 是本次实际要抽取的字段列表：
   - 如果用户传入了指定字段，则必须优先使用这些字段；
   - 如果用户未指定，则从 candidate_fields 中首选 title 和 content 等最核心字段进行提取。
7. field_labels 的 key 必须与 selected_fields 中的字段一致，value 为中文字段名，例如 标题、正文、薪资。
8. data 必须是对象，key 必须与 selected_fields 一致，value 为抽取值。
9. 字段值必须来自文本，不要编造。
10. 如果页面是新闻、文章、杂谈类，优先包含 title(文章标题)、content(正文内容或核心解答)、summary(总结说明)。
11. 如果页面是招聘类页面，优先包含 title、company、salary_range、location、description、requirements。
12. 如果页面是商品类页面，优先包含 name、price、brand、description、specifications。
13. 如果页面是视频类页面，优先包含 title(视频标题)、author(UP主/发布者)、description(视频简介描述)、data_stats(点赞播放等统计数据)。

用户指定字段：{selected_fields_hint}
来源 URL：{source_url}

网页文本：
{text}
"""

INSIGHT_ANALYZE_PROMPT_TEMPLATE = """你是一名网页分析助手。请基于网页中已经提取出的结构化信息，以及用户补充的表单信息，输出一个严格 JSON 对象，不要输出 markdown。

要求：
1. 只能依据输入内容作答，不要编造网页中不存在的信息。
2. 顶层字段必须包含：headline, summary, confidence, key_points, risks, recommended_actions, missing_information, evidence_spans。
3. headline 为一句简短标题；summary 为一句明确结论。
4. confidence 仅可使用：high、medium、low。
5. key_points、risks、recommended_actions、missing_information 均为字符串列表，建议各 2 到 4 条。
6. evidence_spans 为数组，每项包含 label 和 snippet 两个字段，内容必须来自网页提取结果。
7. 要优先结合用户的 goal、role、priority、constraints、notes 来调整结论角度。
8. 如果用户信息不足，请在 missing_information 中明确指出还缺什么。

输入数据：
{payload}
"""

COMPARE_ANALYZE_PROMPT_TEMPLATE = """你是一名网页横向比较分析助手。请基于多个网页提取结果与用户给出的比较条件，输出一个严格 JSON 对象，不要输出 markdown。

要求：
1. 只能依据输入网页结果和用户上下文作答，不能编造任何页面不存在的信息。
2. 顶层字段必须包含：headline, summary, confidence, key_points, risks, recommended_actions, missing_information, evidence_spans, comparison_matrix, report。
3. comparison_matrix 为数组，每项包含 label 和 summary，表示某一个比较维度下的横向结论。
4. report 为对象，必须包含：title, executive_summary, common_points, difference_points, recommendation, next_steps。
5. common_points、difference_points、next_steps 均为字符串数组；recommendation 为一句明确建议。
6. 如果输入不足以得出明确胜出结论，请清楚指出还缺什么。
7. 请结合 focus、must_have、elimination、goal、role 来判断优先级。

输入数据：
{payload}
"""

TASK_PLAN_PROMPT_TEMPLATE = """你是一名网页任务编排助手。请把用户的自然语言需求解析成一个严格 JSON 对象，不要输出 markdown。

要求：
1. 顶层字段必须包含：task_type, summary, urls, selected_fields, use_static, storage_format, schema_name, name, confidence, warnings。
2. task_type 仅可使用：single_extract、batch_extract、monitor、compare_analysis。
3. urls 必须是 URL 字符串数组；如果用户没有提供 URL，就返回空数组。
4. selected_fields 为英文字段名数组，例如 title、content、price、author、publish_date。
5. use_static 根据语义判断是否适合静态抓取；无法判断时默认 false。
6. storage_format 仅可使用 json、csv、sqlite；无法判断时默认 json。
7. schema_name 优先返回 auto，除非用户明确表达新闻、商品、招聘等强类型需求。
8. name 为简短任务名；summary 为一句中文说明。
9. warnings 为字符串数组，用于提示用户还缺什么、有哪一些不确定项。
10. 不能编造 URL、字段或硬性约束；没有就留空或给出 warning。

用户需求：
{request_text}
"""

DYNAMIC_SYSTEM_PROMPT = "你擅长识别网页类型、挑选字段并抽取可靠结果。"
INSIGHT_SYSTEM_PROMPT = "你擅长将网页抽取结果和用户补充信息结合，形成可靠、克制、可执行的分析结论。"
COMPARE_SYSTEM_PROMPT = "你擅长做多网页对比、选型判断和差异归纳，结论必须克制且可追溯。"
TASK_PLAN_SYSTEM_PROMPT = "你擅长把自然语言需求解析成可执行的网页任务配置。"
