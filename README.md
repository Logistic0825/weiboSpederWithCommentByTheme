# 微博评论数据清洗方案

本仓库提供针对 HuggingFace 数据集 Logistic12/weiboDataWithCommentByTheme 的评论清洗脚本与方法说明，覆盖基础过滤、内容质量清洗、文本标准化、以及业务相关规则。脚本位于 [clean_weibo.py](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py)。

## 数据结构
- 每条样本包含 `keyword`、`weibo_details`、`top_comments` 列；清洗后将新增 `cleaned_top_comments`，仅保留通过规则的评论。
- 评论字段示例：
  ```json
  {
    "original_post_id": "5273859146388964",
    "comment_id": "5274060460392990",
    "user": "顽童游玩",
    "content": "简单的事情复杂化就是想坑人害人",
    "likes": 0
  }
  ```

## 指标与删除原因解释大全
- 关键计数含义
  - 原始样本数：数据集中原始文章条数
  - 文章去重后样本数：按微博ID严格去重后的文章条数
  - 最终样本数：清洗流程结束后保留的文章条数
  - 原始评论数：所有文章的原始评论总数
  - 评论精确去重后：清洗后（近似去重前）的评论条数
  - 清洗后评论数(去重前)：规则清洗完成后、近似去重之前的评论条数
  - 清洗后评论数(最终)：规则清洗 + 近似去重后的评论条数
  - 文章重复删除数：重复文章（ID相同）删除的数量
  - 文章时间不在范围删除数：发布时间不在设定范围内的文章删除数量（默认 2025-01-01 至 2026-03-01）
  - 文章空评论删除数：清洗后没有有效评论的文章删除数量（仅在开启该选项时）
  - 评论ID重复删除数：同一文章内，评论ID重复删除的数量
  - 评论精确重复删除数：同一文章内，文本完全相同（规范化后）评论删除数量
  - 评论近似重复删除数：同一文章内，相似度≥0.9 的评论删除数量
  - 截断评论数：长度超过截断阈值（默认500）被截断的评论数量
  - AI判别调用次数：清洗过程中触发 AI 判别的次数

- 删除原因键说明（dropped_reasons）
  - exact_dup：精确重复（规范化后文本完全相同）删除
  - too_short：过短删除（原始或清洗后有效长度不足 5）
  - empty：空文本删除（规范化后为空）
  - meaningless_rule：无意义文本删除（纯数字/重复“哈哈”等/低字符多样性）
  - pure_punct_emoji：纯标点或纯表情删除
  - ad_spam_rule：广告垃圾（手机号/QQ/微信/URL/淘口令等）命中删除
  - too_long_drop：超长删除（超过1000字）
  - near_dup：近似重复删除（相似度≥0.9）
  - ai_ad_spam：AI 判定为广告后删除
  - ai_meaningless：AI 判定为无意义后删除
  - ai_sensitive：AI 判定为敏感后删除（在允许删除敏感内容时）

- 删除样本明细（dropped_examples）
  - 根层 dropped_examples：聚合所有分片的删除样本明细，按原因分组
  - 分片层 metrics.splits.<split>.dropped_examples：该分片内的删除样本明细
  - 每条明细包含：original_post_id、comment_id、user、likes、content_orig、content_norm、reason，还可能包含 sensitive_hits

- 两处 dropped_examples 的原因
  - 为了同时支持“全局分析”和“分片对比”，指标 JSON 在根层和分片层都保留删除样本明细对象；两者位于不同层级，不是重复键

## 导出与对比（内置于 clean_weibo.py）
- 清洗入口会自动导出两份 Excel 到 output：
  - cleaned_weibo.xlsx（清洗后）
    - articles：关键词、微博ID、微博正文、完整正文、作者、点赞数、转发数、评论数、原评论条数、清洗后评论条数、发布时间
    - comments：每行一条“清洗后保留”的评论，含文章字段与清洗标记
      - 评论字段：原微博ID、评论ID、评论用户、评论内容、评论点赞
      - 清洗标记：AI广告、AI无意义、AI敏感、机器水军、是否截断、敏感命中
      - 说明：清洗后保留的评论通常不再命中删除原因，因此这些标记大多为 False
  - raw_weibo.xlsx（清洗前）
    - articles 同上
    - comments：每行一条“原始”评论，含文章字段与下列标注
      - 评论字段同上
      - 清洗标记（来自清洗过程的映射）：AI广告、AI无意义、AI敏感、机器水军、是否截断、敏感命中
      - 新增标注：是否被清洗、被清洗原因（多原因逗号分隔，如 exact_dup, near_dup, too_short, ad_spam_rule, ai_sensitive, article_time, article_dup）
        - article_time：文章时间不在设定范围，整篇文章被删除
        - article_dup：文章ID重复，整篇文章被删除
    - 作用：用于“事后复核”，哪条原始评论在清洗后被删除、因何原因删除，一目了然
  - 覆盖流式与非流式模式；可用下列参数定制导出路径：
    - --export-clean-xlsx output/cleaned_weibo.xlsx
    - --export-raw-xlsx output/raw_weibo.xlsx

## 清洗规则与实现映射
### 一、基础过滤
- 空值与重复：
  - 删除内容为空（NULL、""）。
  - 去重：同一用户+相同内容直接视为重复。
  - 近似重复：对同一帖子范围内采用序列相似度，阈值默认 0.9，仅与最近 50 条比较以保证性能。
- 长度过滤：
  - 过短：有效长度（去除表情、标点）不足 3 个字符删除。
  - 过长：超过 1000 字直接删除；500–1000 字截断到 500 字并追加省略号。
  - 纯标点/表情：删除。

### 二、内容质量清洗
- 特殊内容识别（正则规则）：
  - 垃圾广告：手机号、QQ号、微信号、外链、淘宝口令等命中删除。
  - 敏感词：维护小型词表（政治/色情/暴力/违法/侮辱）；仅用于标注，不做删除。删除由 AI 判别触发。
  - 无意义文本：纯数字、重复“哈哈”等、低字符多样性文本删除。
- 机器水军（可选 AI 辅助）：
  - 基于文本模板化特征的判别由 AI 兜底提升准确率；失败时仅用规则集。

### 三、文本标准化
- 格式统一：
  - HTML 标签移除，HTML 实体反解。
  - 全角/半角统一、不可见字符清除、空白折叠。
  - 话题与 @ 默认保留（作为内容的一部分），如需删除可在脚本中扩展。
- 语义预处理（可选）：
  - 分词、停用词、词干标准化需第三方库（如 jieba）；当前脚本未强制依赖，后续可按需扩展。

### 四、业务相关
- 时间有效性：
  - 若数据集中包含评论时间字段，可在 `process_example` 中加入时间范围过滤；当前示例数据未提供时间字段，默认跳过。

## 使用方法
### 1. 环境准备
- Python ≥ 3.9
- 安装依赖：
  ```bash
  pip install datasets "openai>=1.0.0"
  ```
  如需中文分词：`pip install jieba`

### 2. 运行清洗
- 直接运行脚本：
  ```bash
  python /Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py
  ```
- 输出：默认生成 `output/cleaned_weibo.jsonl`，每行一条样本（含 `cleaned_top_comments`）。
- 可通过环境变量指定输出路径：
  ```bash
  OUTPUT_PATH=output/weibo_clean.jsonl python clean_weibo.py
  ```
- 离线/网络不稳定（本地JSONL回退）：
  - 若访问 HuggingFace 失败，可指定本地原始 JSONL：
    ```bash
    python clean_weibo.py --input-jsonl output/raw_weibo.jsonl
    ```
  - 流式模式也会实时写出原始 JSONL：`--raw-output output/raw_weibo.jsonl`

### 3. 启用 AI 判别（必要配置）
- 在项目根目录创建 .env 文件（不提交到仓库），内容例如：
  ```bash
  OPENAI_API_KEY=sk-xxxxxxxx
  # 如使用官方 OpenAI：可不设置 GPTS_BASE_URL（默认 https://api.openai.com/v1）
  # 如使用第三方网关：设置 GPTS_BASE_URL=https://api.gptsapi.net/v1
  GPTS_MODEL=gpt-4o-mini
  AI_ENABLE=true
  AI_SAMPLE_RATE=1.0
  ```
- 运行时 run_clean.sh 会自动加载 .env；若未检测到 API Key，会输出警告并跳过 AI 判别
- AI 触发条件（默认）：
  - 仅在命中敏感词时调用（`--ai-on-sensitive-only true`）
  - 删除敏感仅在 AI 判定为敏感且允许删除时（`--drop-sensitive true`）

### 3. AI 判别
- 脚本会在检测到环境变量后启用 AI 兜底判别：
  - `GPTS_API_KEY` 或 `OPENAI_API_KEY`
  - `GPTS_BASE_URL`（默认 `https://api.gptsapi.net/v1`）
  - `GPTS_MODEL`（默认 `gpt-4o-mini`）
- 调用失败时自动回退到纯规则处理，不影响整体流程。
- 如使用自建/代理网关，请确保 Base URL 可用；若网关不可达，请改用有效的服务地址并设置到 `GPTS_BASE_URL`。
- 违禁/敏感内容的删除仅依据 AI 判别（`ai_sensitive`），规则词表只用于辅助标注与统计，不会造成误删。
- AI 调用范围（可控）：
  - `ai_enable`：是否启用 AI 调用（默认 True）
  - `ai_on_sensitive_only`：仅在规则敏感词命中时才调用 AI（默认 False）
  - `ai_sample_rate`：抽样比例（0.0–1.0，默认 1.0），用于控制调用频率
  - 以上参数位于 `DEFAULT_SETTINGS`，也可在脚本内按需修改

### 4. 流式清洗与进度
- 默认启用流式模式，边处理边写入，实时打印进度：
  - `STREAMING=1` 开启（默认），`STREAMING=0` 关闭
  - `BATCH_SIZE` 控制指标滚动写入频率（默认 200 样本）
  - `PRINT_EVERY` 控制进度打印频率（默认 100 样本）
- 运行示例：
  ```bash
  ./run_clean.sh
  ```
 - 实时输出：
   - 数据逐行写入到 `OUTPUT_PATH`
   - 指标滚动写入到 `METRICS_PATH`
   - 控制台打印：样本原始/去重后/最终、评论计数、近似重复移除、截断数、AI 调用次数
   - 安装 `tqdm` 会显示进度条与关键指标后缀
   - 结束时打印三张表（Markdown）：`summary_table_markdown`、`splits_table_markdown`、`reason_table_markdown`

### 5. 时间过滤参数
- 通过 CLI 控制文章时间范围（含边界，按 +08:00 计算）：
  ```bash
  python clean_weibo.py \
    --time-filter-enable true \
    --start-date 2025-01-01 \
    --end-date 2026-03-31
  ```
- 若不需要时间过滤：`--time-filter-enable false`
- 默认为 `2025-01-01` 到 `2027-06-01`

### 4. 可配置项
- 在 `DEFAULT_SETTINGS` 中可调整：
  - `min_effective_len`：有效长度下限，默认 3。
  - `max_len_truncate`：截断上限，默认 500。
  - `max_len_drop`：过滤阈值，默认 1000。
  - `near_dup_sim`：近似重复相似度阈值，默认 0.9。
  - `drop_sensitive`：命中敏感词是否删除，默认 True。

## 扩展建议
- 更大规模敏感词库可通过外部文件加载。
- 用户质量过滤需用户画像数据（粉丝、注册时间、认证等）；待数据可用时在 `clean_comment` 中增加该维度规则。
- 近似重复检测可用局部敏感哈希（LSH）替代序列匹配以提升性能。

## 说明
- 不在代码中写入任何明文密钥；请使用环境变量注入。
- 针对部分代理网关，Base URL 若返回 404，请确认路由是否需要 `.../v1/chat/completions` 等具体路径或更换为有效地址。

## 技术细节
- 标准化处理
  - HTML 反解与清理：[normalize_text](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L52-L60)
  - 全角/半角统一：[to_half_width](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L40-L50)
  - 有效长度计算（去表情与标点）：[effective_length](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L62-L66)
- 规则过滤
  - 纯标点/表情检测（90% 非字/数/汉字）：[is_pure_punct_or_emoji](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L68-L74)
  - 无意义文本（纯数字/重复“哈哈”/低多样性）：[is_meaningless_text](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L76-L87)
  - 广告识别（手机号/QQ/微信/URL/淘口令）：[contains_ad_spam](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L90-L101)
  - 敏感词命中仅标注不删：[contains_sensitive](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L104-L111)
- AI 判别
  - 封装调用与 JSON 返回解析：[ai_classify](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L133-L163)
  - 删除策略：仅当 AI 判定为敏感时删除（`ai_sensitive`），词表仅做标注：[clean_comment](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L166-L227)
  - 环境变量：`GPTS_API_KEY`/`OPENAI_API_KEY`、`GPTS_BASE_URL`（默认 `https://api.gptsapi.net/v1`）、`GPTS_MODEL`（默认 `gpt-4o-mini`）
- 导出
  - JSONL→XLSX 导出内置于入口：[export_jsonl_to_xlsx](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L180-L280)
  - raw_weibo.xlsx 注入清洗映射（AI标记与“是否被清洗/原因”）；cleaned_weibo.xlsx 展示清洗后保留评论
- 去重算法
  - 近似重复：`difflib.SequenceMatcher` 相似度≥0.9 判为重复；仅比较最近 50 条以兼顾性能：[dedupe_comments](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L230-L253)
- 指标统计
  - 每样本统计与汇总：[process_example](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L256-L289) 与 [clean_dataset](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L301-L355)
  - 删除原因与 case 记录：`dropped_reasons` 为计数，`dropped_examples` 为明细列表（含原始/规范化内容与用户/ID）
- 流式写入与进度打印
  - 流式处理与滚动更新指标：[run_streaming_clean](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py)
  - 主入口根据 `STREAMING` 切换执行路径：[main](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py)
 - 命令行参数（argparse）
   - 支持通过参数控制：`--dataset`、`--output`、`--metrics`、`--streaming`（1/0）、`--batch-size`、`--print-every`、`--min-effective-len`、`--max-len-truncate`、`--max-len-drop`、`--near-dup-sim`、`--drop-sensitive`（true/false）、`--ai-enable`（true/false）、`--ai-on-sensitive-only`（true/false）、`--ai-sample-rate`（0.0–1.0）
   - 示例：
     ```bash
     python clean_weibo.py \
       --dataset Logistic12/weiboDataWithCommentByTheme \
       --output output/cleaned_weibo.jsonl \
       --metrics output/clean_metrics.json \
       --streaming 1 \
       --batch-size 200 \
       --print-every 100 \
       --drop-sensitive true \
       --ai-enable true \
       --ai-on-sensitive-only false \
       --ai-sample-rate 1.0
     ```
- 输出结构
  - 清洗结果：`output/cleaned_weibo.jsonl` 每行一个样本，含 `cleaned_top_comments`
  - 指标 JSON（简化示例）：
    ```json
    {
    {
      "total_examples": 123,
      "total_comments_before": 4567,
      "total_comments_after_before_dedupe": 3210,
      "total_comments_after": 2980,
      "near_dup_removed": 230,
      "truncated_count": 45,
      "ai_invocations": 1800,
      "dropped_reasons": {
        "empty": 12,
        "too_short": 210,
        "ad_spam_rule": 98,
        "ai_sensitive": 34
      },
      "dropped_examples": {
        "ai_sensitive": [
          {
            "original_post_id": "xxx",
            "comment_id": "yyy",
            "user": "abc",
            "likes": 0,
            "content_orig": "原文……",
            "content_norm": "规范化后……",
            "reason": "ai_sensitive",
            "sensitive_hits": ["政治:颠覆"]
          }
        ]
      },
      "splits": {
        "train": { "comments_before": 1000, "dropped_reasons": { "too_short": 10 }, "dropped_examples": {} }
      }
    }
    ```
- 可配置项
  - 默认参数：[DEFAULT_SETTINGS](file:///Users/logistic/Documents/AI/LLM/project/weibo_dataset_clean/clean_weibo.py#L292-L298)
  - 通过环境变量控制输出路径：`OUTPUT_PATH`、`METRICS_PATH`
  - 是否删除敏感：`drop_sensitive=True` 时仅 AI 判定删除；设为 `False` 则只做标注
  - AI 调用范围：`ai_enable` / `ai_on_sensitive_only` / `ai_sample_rate`
  - 输出控制：`replace_comments`（默认 True，替换 `top_comments`）、`drop_articles_no_comments`（默认 False，删除无有效评论的文章）
- 性能与限制
  - 相似度比较窗口为 50 条；大数据量建议改为 LSH 或分桶
  - AI 判别超时设为 60s，失败自动回退规则；网络不通或 Base URL 404 时请更换有效地址
  - 用户质量过滤需补充画像数据方可生效（当前未包含）
