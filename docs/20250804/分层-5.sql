create database db;-- 工单编号:大数据-电商数仓-05-商品主题连带分析看板
-- 功能：存储商品的访问、收藏加购、支付等原始行为数据，用于后续关联关系分析
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_behavior (
                                                             user_id STRING COMMENT '用户唯一标识',
                                                             main_product_id STRING COMMENT '主商品ID（引流款/热销款等主商品）',
                                                             related_product_id STRING COMMENT '关联商品ID（与主商品产生关联行为的商品）',
                                                             behavior_type STRING COMMENT '行为类型：同时访问、同时收藏加购、同时段支付',
                                                             behavior_time TIMESTAMP COMMENT '行为发生时间',
                                                             store_id STRING COMMENT '店铺ID'
)
    COMMENT '商品行为原始数据表，存储近7天商品的关联行为数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式yyyy-MM-dd')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ods_db/ods_product_behavior/'
;

-- 插入500条随机数据（2025-07-16至2025-07-22）
INSERT INTO TABLE ods_product_behavior
SELECT
    concat('user_', lpad(cast(rand() * 1000 as int), 3, '0')) as user_id,
    concat('prod_', lpad(cast(rand() * 50 as int), 3, '0')) as main_product_id,
    concat('prod_', lpad(cast(rand() * 300 + 100 as int), 3, '0')) as related_product_id,
    case cast(rand() * 3 as int)
        when 0 then '同时访问'
        when 1 then '同时收藏加购'
        else '同时段支付'
        end as behavior_type,
    from_unixtime(unix_timestamp('2025-07-16', 'yyyy-MM-dd') + cast(rand() * 7 * 86400 as int), 'yyyy-MM-dd HH:mm:ss') as behavior_time,
    concat('store_', lpad(cast(rand() * 10 as int), 2, '0')) as store_id,
    date(from_unixtime(unix_timestamp('2025-07-16', 'yyyy-MM-dd') + cast(rand() * 7 * 86400 as int))) as dt
FROM
    (select 1 from (select explode(split(space(500), ' ')) ) t) t;

select *
from ods_product_behavior;




-- 工单编号:大数据-电商数仓-05-商品主题连带分析看板
-- 功能：存储商品详情页引导至其他商品的原始行为数据，用于连带效果分析
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_guide (
                                                          guide_product_id STRING COMMENT '引导商品ID（详情页所在商品）',
                                                          target_product_id STRING COMMENT '被引导商品ID（通过详情页跳转的商品）',
                                                          visitor_id STRING COMMENT '访客唯一标识',
                                                          guide_time TIMESTAMP COMMENT '引导发生时间（从引导商品跳转至目标商品的时间）',
                                                          conversion_flag INT COMMENT '转化标识：1-已转化（支付），0-未转化',
                                                          store_id STRING COMMENT '店铺ID'
)
    COMMENT '商品详情页引导行为表，存储商品详情页的引流及转化原始数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期，格式yyyy-MM-dd')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/ods_db/ods_product_guide/'
;


-- 插入500条随机数据（2025-07-16至2025-07-22）
INSERT INTO TABLE ods_product_guide
SELECT
    concat('prod_', lpad(cast(rand() * 50 as int), 3, '0')) as guide_product_id,
    concat('prod_', lpad(cast(rand() * 300 + 100 as int), 3, '0')) as target_product_id,
    concat('visitor_', lpad(cast(rand() * 1000 as int), 3, '0')) as visitor_id,
    from_unixtime(unix_timestamp('2025-07-16', 'yyyy-MM-dd') + cast(rand() * 7 * 86400 as int), 'yyyy-MM-dd HH:mm:ss') as guide_time,
    cast(rand() * 2 as int) as conversion_flag,
    concat('store_', lpad(cast(rand() * 10 as int), 2, '0')) as store_id,
    date(from_unixtime(unix_timestamp('2025-07-16', 'yyyy-MM-dd') + cast(rand() * 7 * 86400 as int))) as dt
FROM
    (select 1 from (select explode(split(space(500), ' ')) ) t) t;

select *
from ods_product_guide;

use db;




-- 功能：清洗商品关联行为数据，补充商品名称、类型（无 Dim 层，硬编码规则）商品关联行为明细
CREATE TABLE IF NOT EXISTS dwd_product_relation_detail (
     user_id STRING COMMENT '用户ID',
     main_product_id STRING COMMENT '主商品ID',
     main_product_name STRING COMMENT '主商品名称（硬编码生成）',
     related_product_id STRING COMMENT '关联商品ID',
     related_product_name STRING COMMENT '关联商品名称（硬编码生成）',
     behavior_type STRING COMMENT '行为类型：同时访问、同时收藏加购、同时段支付',
     behavior_time TIMESTAMP COMMENT '行为时间（转换为标准格式）',
     store_id STRING COMMENT '店铺ID',
     main_product_type STRING COMMENT '主商品类型（业务规则判断）'
)
    COMMENT '商品关联行为明细（无 Dim 层，通过规则补维度）'
    PARTITIONED BY (dt STRING COMMENT '分区日期：yyyy-MM-dd')
    STORED AS PARQUET;

-- 数据清洗 & 插入（补商品名称、类型）
INSERT OVERWRITE TABLE dwd_product_relation_detail
SELECT
    user_id,
    main_product_id,
    -- 硬编码生成商品名称（示例：prod_001 → 商品_prod_001）
    concat('商品_', main_product_id) AS main_product_name,
    related_product_id,
    concat('商品_', related_product_id) AS related_product_name,
    behavior_type,
    -- 时间字符串转 TIMESTAMP
    from_unixtime(unix_timestamp(behavior_time, 'yyyy-MM-dd HH:mm:ss')) AS behavior_time,
    store_id,
    -- 业务规则判断商品类型（示例：prod_001~prod_020 为引流款，prod_021~prod_050 为热销款）
    CASE
        WHEN substr(main_product_id, 6, 3) <= '020' THEN '引流款'
        ELSE '热销款'
        END AS main_product_type,
    dt -- 分区字段
FROM
    ods_product_behavior
WHERE
    dt BETWEEN '2025-07-16' AND '2025-07-22' -- 近7天数据
  AND behavior_type IN ('同时访问', '同时收藏加购', '同时段支付'); -- 过滤有效行为



select * from dwd_product_relation_detail;




-- 功能：清洗商品引导行为数据，补充商品名称（硬编码）商品引导行为明细
CREATE TABLE IF NOT EXISTS dwd_product_guide_detail (
     guide_product_id STRING COMMENT '引导商品ID',
     guide_product_name STRING COMMENT '引导商品名称（硬编码生成）',
     target_product_id STRING COMMENT '被引导商品ID',
     target_product_name STRING COMMENT '被引导商品名称（硬编码生成）',
     visitor_id STRING COMMENT '访客ID',
     guide_time TIMESTAMP COMMENT '引导时间（标准格式）',
     conversion_flag INT COMMENT '转化标识：1-支付，0-未支付',
     store_id STRING COMMENT '店铺ID'
)
    COMMENT '商品引导行为明细（无 Dim 层，硬编码补名称）'
    PARTITIONED BY (dt STRING COMMENT '分区日期：yyyy-MM-dd')
    STORED AS PARQUET;

-- 数据清洗 & 插入（补商品名称）
INSERT OVERWRITE TABLE dwd_product_guide_detail
SELECT
    guide_product_id,
    concat('商品_', guide_product_id) AS guide_product_name, -- 硬编码名称
    target_product_id,
    concat('商品_', target_product_id) AS target_product_name, -- 硬编码名称
    visitor_id,
    -- 时间字符串转 TIMESTAMP
    from_unixtime(unix_timestamp(guide_time, 'yyyy-MM-dd HH:mm:ss')) AS guide_time,
    conversion_flag,
    store_id,
    dt -- 分区字段
FROM
    ods_product_guide
WHERE
    dt BETWEEN '2025-07-16' AND '2025-07-22'; -- 近7天数据

select  *from dwd_product_guide_detail;



-- 功能：按“主商品+关联商品+行为类型”聚合，统计行为次数 商品关联行为汇总）
CREATE TABLE IF NOT EXISTS dws_product_relation_sum (
  main_product_id STRING COMMENT '主商品ID',
  main_product_name STRING COMMENT '主商品名称',
  related_product_id STRING COMMENT '关联商品ID',
  related_product_name STRING COMMENT '关联商品名称',
  behavior_type STRING COMMENT '行为类型',
  behavior_count BIGINT COMMENT '行为次数',
  store_id STRING COMMENT '店铺ID',
  main_product_type STRING COMMENT '主商品类型'
)
    COMMENT '商品关联行为汇总（DWD 层聚合）'
    PARTITIONED BY (dt STRING COMMENT '分区日期：yyyy-MM-dd')
    STORED AS PARQUET;

-- 设置动态分区为非严格模式（仅当前会话生效）
SET hive.exec.dynamic.partition.mode = nonstrict;
-- 聚合 & 插入
INSERT OVERWRITE TABLE dws_product_relation_sum PARTITION (dt)
SELECT
    main_product_id,
    main_product_name,
    related_product_id,
    related_product_name,
    behavior_type,
    COUNT(1) AS behavior_count, -- 统计行为次数
    store_id,
    main_product_type,
    dt -- 分区字段
FROM
    dwd_product_relation_detail
WHERE
    dt BETWEEN '2025-07-16' AND '2025-07-22' -- 近7天
GROUP BY
    main_product_id, main_product_name, related_product_id, related_product_name, behavior_type, store_id, main_product_type,dt;


select  *from dws_product_relation_sum;



-- 功能：按“引导商品+被引导商品”聚合，统计引导、转化数据 (商品引导效果汇总）
CREATE TABLE IF NOT EXISTS dws_product_guide_sum (
  guide_product_id STRING COMMENT '引导商品ID',
  guide_product_name STRING COMMENT '引导商品名称',
  target_product_id STRING COMMENT '被引导商品ID',
  target_product_name STRING COMMENT '被引导商品名称',
  guide_visitor_count BIGINT COMMENT '引导访客数（去重）',
  conversion_count BIGINT COMMENT '转化次数（conversion_flag=1）',
  conversion_rate DECIMAL(10,2) COMMENT '转化率：转化次数/引导访客数',
  store_id STRING COMMENT '店铺ID'
)
    COMMENT '商品引导效果汇总（DWD 层聚合）'
    PARTITIONED BY (dt STRING COMMENT '分区日期：yyyy-MM-dd')
    STORED AS PARQUET;

-- 聚合 & 插入（计算转化率）
INSERT OVERWRITE TABLE dws_product_guide_sum PARTITION (dt)
SELECT
    guide_product_id,
    guide_product_name,
    target_product_id,
    target_product_name,
    COUNT(DISTINCT visitor_id) AS guide_visitor_count, -- 去重访客数
    SUM(conversion_flag) AS conversion_count, -- 转化次数
    -- 转化率：避免除数为0，用 IF 处理
    IF(COUNT(DISTINCT visitor_id) = 0, 0, ROUND(SUM(conversion_flag)/COUNT(DISTINCT visitor_id), 2)) AS conversion_rate,
    store_id,
    dt -- 分区字段
FROM
    dwd_product_guide_detail
WHERE
    dt BETWEEN '2025-07-16' AND '2025-07-22' -- 近7天
GROUP BY
    guide_product_id, guide_product_name, target_product_id, target_product_name, store_id,dt;


select  * from dws_product_guide_sum;




-- 功能：输出“主商品+行为类型”的 TOP30 关联商品 关联商品 TOP30）
CREATE TABLE IF NOT EXISTS ads_product_relation_top30 (
   main_product_id STRING COMMENT '主商品ID',
   main_product_name STRING COMMENT '主商品名称',
   related_product_id STRING COMMENT '关联商品ID',
   related_product_name STRING COMMENT '关联商品名称',
   behavior_type STRING COMMENT '行为类型',
   behavior_count BIGINT COMMENT '行为次数',
   rank INT COMMENT '排名（同主商品+行为类型下排序）',
   store_id STRING COMMENT '店铺ID',
   main_product_type STRING COMMENT '主商品类型'
)
    COMMENT '主商品关联商品 TOP30（ADS 层）'
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ads_product_relation_top30
SELECT * FROM (
                  SELECT
                      main_product_id,
                      CONCAT('商品_', main_product_id) AS main_product_name,
                      related_product_id,
                      CONCAT('商品_', related_product_id) AS related_product_name,
                      behavior_type,
                      COUNT(1) AS behavior_count,
                      -- 按主商品+行为类型分区，行为次数降序排名
                      ROW_NUMBER() OVER (
                          PARTITION BY main_product_id, behavior_type
                          ORDER BY COUNT(1) DESC
                          ) AS rank,
                      store_id,
                      CASE
                          WHEN SUBSTR(main_product_id, 6, 3) <= '020' THEN '引流款'
                          ELSE '热销款'
                          END AS main_product_type
                  FROM ods_product_behavior
                  WHERE
                      dt BETWEEN '2025-07-16' AND '2025-07-22'
                    AND behavior_type IN ('同时访问', '同时收藏加购', '同时段支付')
                  GROUP BY
                      main_product_id, related_product_id, behavior_type, store_id
              ) t
WHERE
    t.rank <= 30
  AND t.main_product_type IN ('引流款', '热销款');

    select * from ads_product_relation_top30;


-- 功能：输出引导能力 TOP10 商品及关联商品明细
CREATE TABLE IF NOT EXISTS ads_product_guide_top10 (
     guide_product_id STRING COMMENT '引导商品ID',
     guide_product_name STRING COMMENT '引导商品名称',
     total_guide_visitor_count BIGINT COMMENT '总引导访客数',
     rank INT COMMENT '引导能力排名',
     related_products ARRAY<STRUCT<
         target_product_id:STRING,
         target_product_name:STRING,
         guide_visitor_count:BIGINT,
         conversion_rate:DECIMAL(10,2)
     >> COMMENT '关联商品明细'
)
    COMMENT '引导能力 TOP10 商品（ADS 层）'
    STORED AS PARQUET;

-- 生成 TOP10 及明细
-- 子查询：计算引导商品排名
WITH ranked_products AS (
    SELECT
        guide_product_id,
        guide_product_name,
        SUM(guide_visitor_count) AS total_guide_visitor_count,
        ROW_NUMBER() OVER (ORDER BY SUM(guide_visitor_count) DESC) AS rank
    FROM dws_product_guide_sum
    WHERE dt = '2025-07-22'
    GROUP BY guide_product_id, guide_product_name
),
-- 筛选 TOP10
     top10_products AS (
         SELECT *
         FROM ranked_products
         WHERE rank <= 10
     )
-- 最终查询
INSERT OVERWRITE TABLE ads_product_guide_top10
SELECT
    g.guide_product_id,
    g.guide_product_name,
    g.total_guide_visitor_count,
    g.rank,
    COLLECT_LIST(
            NAMED_STRUCT(
                    'target_product_id', s.target_product_id,
                    'target_product_name', s.target_product_name,
                    'guide_visitor_count', s.guide_visitor_count,
                    'conversion_rate', s.conversion_rate
            )
    ) AS related_products
FROM top10_products g
         JOIN dws_product_guide_sum s
              ON g.guide_product_id = s.guide_product_id
                  AND s.dt = '2025-07-22'
GROUP BY
    g.guide_product_id,
    g.guide_product_name,
    g.total_guide_visitor_count,
    g.rank;





select * from ads_product_guide_top10;
