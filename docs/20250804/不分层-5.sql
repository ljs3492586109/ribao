use db;
-- 关联洞察：找出与主商品有同时访问、同时收藏加购和同时段支付的top30宝贝
-- 工单编号:大数据-电商数仓-05-商品主题连带分析看板
WITH behavior_summary AS (
    SELECT
        main_product_id,
        related_product_id,
        behavior_type,
        COUNT(1) AS behavior_count
    FROM
        ods_product_behavior
    WHERE
        dt BETWEEN '2025-07-16' AND '2025-07-22'
      AND main_product_id IN (
        -- 假设这些是引流款和热销款的主商品ID，实际使用时需替换为真实ID
                              'prod_001', 'prod_002', 'prod_003', 'prod_004', 'prod_005',
                              'prod_006', 'prod_007', 'prod_008', 'prod_009', 'prod_010'
        )
    GROUP BY
        main_product_id, related_product_id, behavior_type
),
     top30_behavior AS (
         SELECT
             main_product_id,
             related_product_id,
             behavior_type,
             behavior_count,
             ROW_NUMBER() OVER (PARTITION BY main_product_id, behavior_type ORDER BY behavior_count DESC) AS ranking
         FROM
             behavior_summary
     )
SELECT
    main_product_id,
    related_product_id,
    behavior_type,
    behavior_count,
    ranking
FROM
    top30_behavior
WHERE
    ranking <= 30;


-- 连带效果监控：展示商品引导及转化效果，默认展示店内最近引导访客数前10的商品及其连带商品
-- 工单编号:大数据-电商数仓-05-商品主题连带分析看板
WITH guide_summary AS (
    SELECT
        guide_product_id,
        target_product_id,
        COUNT(DISTINCT visitor_id) AS guide_visitor_count,
        SUM(conversion_flag) AS conversion_count
    FROM
        ods_product_guide
    WHERE
        dt BETWEEN '2025-07-16' AND '2025-077-22'
    GROUP BY
        guide_product_id, target_product_id
),
     top10_guide AS (
         SELECT
             guide_product_id,
             SUM(guide_visitor_count) AS total_guide_visitor_count,
             ROW_NUMBER() OVER (ORDER BY SUM(guide_visitor_count) DESC) AS ranking
         FROM
             guide_summary
         GROUP BY
             guide_product_id
     )
SELECT
    tg.guide_product_id,
    tg.total_guide_visitor_count,
    tg.ranking,
    gs.target_product_id,
    gs.guide_visitor_count,
    gs.conversion_count,
    -- 计算转化率，避免除以0
    CASE
        WHEN gs.guide_visitor_count = 0 THEN 0
        ELSE gs.conversion_count / gs.guide_visitor_count
        END AS conversion_rate
FROM
    top10_guide tg
        JOIN
    guide_summary gs ON tg.guide_product_id = gs.guide_product_id
WHERE
    tg.ranking <= 10;


