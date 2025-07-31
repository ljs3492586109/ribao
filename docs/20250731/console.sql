create database ds;

set hive.exec.mode.local.auto=True;

use ds;







drop table if exists ods_page_behavior;
CREATE TABLE ods_page_behavior (
    log_id        STRING COMMENT '日志ID',
    user_id       STRING COMMENT '用户ID',
    session_id    STRING COMMENT '会话ID',
    page_id       STRING COMMENT '页面ID',
    page_type     STRING COMMENT '页面类型（home/detail/custom）',
    module_id     STRING COMMENT '模块ID',
    module_type   STRING COMMENT '模块类型（ad/recommend/nav等）',
    click_time    TIMESTAMP COMMENT '点击时间',
    stay_duration INT COMMENT '停留时长（毫秒）',
    refer_page    STRING COMMENT '来源页面',
    device_type   STRING COMMENT '设备类型（mobile/pc/tablet）',
    os            STRING COMMENT '操作系统',
    province      STRING COMMENT '省份',
    city          STRING COMMENT '城市',
    ip            STRING COMMENT 'IP地址',
    extra_info    MAP<STRING, STRING> COMMENT '额外信息（键值对）'
);


-- 构造 500 条页面行为数据插入 ods_page_behavior
INSERT INTO TABLE ods_page_behavior
SELECT
    CONCAT('log_', LPAD(seq, 3, '0')) AS log_id,  -- 日志ID，如 log_001
    CONCAT('user_', LPAD(seq, 3, '0')) AS user_id,  -- 用户ID，如 user_001
    CONCAT('session_', LPAD(FLOOR(RAND() * 100), 3, '0')) AS session_id,  -- 会话ID，随机生成
    CONCAT('page_', LPAD(FLOOR(RAND() * 20), 2, '0')) AS page_id,  -- 页面ID，随机生成
    pt AS page_type,  -- 从 page_types 随机选页面类型
    CONCAT('module_', LPAD(FLOOR(RAND() * 20), 2, '0')) AS module_id,  -- 模块ID，随机生成
    mt AS module_type,  -- 从 module_types 随机选模块类型
    -- 点击时间：2025-07-01 ~ 2025-07-31 随机（按秒级随机）
    FROM_UNIXTIME(
            UNIX_TIMESTAMP('2025-07-01 00:00:00') + FLOOR(RAND() * (86400 * 31))
    ) AS click_time,
    FLOOR(RAND() * 20000) AS stay_duration,  -- 停留时长：0~20000 随机
    -- 来源页面随机选（用 CASE WHEN 替代易出错的 ARRAY 语法）
    CASE FLOOR(RAND() * 6)
        WHEN 0 THEN '/home'
        WHEN 1 THEN '/search'
        WHEN 2 THEN '/category'
        WHEN 3 THEN '/item/123'
        WHEN 4 THEN '/detail/456'
        WHEN 5 THEN '/ad'
        END AS refer_page,
    dt AS device_type,  -- 从 devices 随机选设备类型
    os_val AS os,  -- 从 oses 随机选操作系统
    p AS province,  -- 从 provinces 随机选省份
    c AS city,  -- 从 cities 随机选城市
    CONCAT('192.168.1.', FLOOR(RAND() * 255)) AS ip,  -- IP 随机生成
    MAP(  -- 额外信息随机构造
            CASE WHEN FLOOR(RAND() * 2) = 1 THEN 'source' ELSE 'category' END,
            CASE WHEN FLOOR(RAND() * 2) = 1 THEN 'direct' ELSE 'electronics' END
    ) AS extra_info
FROM (
         SELECT posexplode(split(space(499), ' ')) AS (seq, val)  -- 生成 0~499 序列（500 条数据）
     ) t
-- 关联基础维度表（通过 CROSS JOIN 实现随机选取）
         CROSS JOIN (SELECT 'home' AS pt UNION ALL SELECT 'detail' UNION ALL SELECT 'custom') page_types
         CROSS JOIN (SELECT 'ad' AS mt UNION ALL SELECT 'recommend' UNION ALL SELECT 'nav' UNION ALL SELECT 'content' UNION ALL SELECT 'footer') module_types
         CROSS JOIN (SELECT 'mobile' AS dt UNION ALL SELECT 'pc' UNION ALL SELECT 'tablet') devices
         CROSS JOIN (SELECT 'Android' AS os_val UNION ALL SELECT 'iOS' UNION ALL SELECT 'Windows' UNION ALL SELECT 'Linux' UNION ALL SELECT 'macOS') oses
         CROSS JOIN (SELECT '北京' AS p UNION ALL SELECT '上海' UNION ALL SELECT '广东' UNION ALL SELECT '浙江' UNION ALL SELECT '江苏'
                     UNION ALL SELECT '四川' UNION ALL SELECT '湖北' UNION ALL SELECT '湖南' UNION ALL SELECT '山东' UNION ALL SELECT '陕西') provinces
         CROSS JOIN (SELECT '北京' AS c UNION ALL SELECT '上海' UNION ALL SELECT '深圳' UNION ALL SELECT '杭州' UNION ALL SELECT '南京'
                     UNION ALL SELECT '成都' UNION ALL SELECT '武汉' UNION ALL SELECT '长沙' UNION ALL SELECT '青岛' UNION ALL SELECT '西安') cities
LIMIT 500;  -- 限制结果为 500 条

select * from ods_page_behavior;


drop table ods_product_info;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_info
(
    product_id STRING COMMENT'商品ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT'商品分类ID',
    category_name STRING COMMENT '商品分类名称',
    brand_id STRING COMMENT'品牌ID',
    brand_name STRING COMMENT'品牌名称',
    price DECIMAL(16,2) COMMENT '商品价格',
    cost DECIMAL(16, 2) COMMENT '商品成本价',
    status STRING COMMENT'商品状态',
    create_time STRING COMMENT'商品创建时间',
    update_time STRING COMMENT '商品更新时间'
);


-- 向 ods_product_info 表插入 500 条模拟商品信息数据
-- 向 ods_product_info 表插入 500 条模拟商品信息数据
INSERT INTO TABLE ods_product_info
SELECT
    product_id,
    product_name,
    category_id,
    category_name,
    brand_id,
    brand_name,
    price,
    ROUND(price * 0.8, 2) AS cost,  -- 成本价为价格的 80%
    status,
    create_time,  -- 引用子查询的创建时间
    -- 基于创建时间计算更新时间（0~30 天内随机）
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(create_time) + FLOOR(RAND() * 86400 * 30)
    ) AS update_time
FROM (
         SELECT
             CONCAT('P', LPAD(seq, 3, '0')) AS product_id,  -- 商品ID：P001 ~ P499
             -- 随机拼接商品名称（前缀 + 随机数字）
             CONCAT(
                     CASE FLOOR(RAND() * 5)
                         WHEN 0 THEN 'iPhone '
                         WHEN 1 THEN '小米 '
                         WHEN 2 THEN '华为 '
                         WHEN 3 THEN '戴尔 '
                         WHEN 4 THEN '索尼 '
                         END,
                     LPAD(FLOOR(RAND() * 20), 2, '0')
             ) AS product_name,
             CONCAT('C', LPAD(FLOOR(RAND() * 10), 2, '0')) AS category_id,  -- 分类ID：C00 ~ C09
             -- 分类名称映射
             CASE FLOOR(RAND() * 10)
                 WHEN 0 THEN '手机' WHEN 1 THEN '笔记本电脑' WHEN 2 THEN '家电'
                 WHEN 3 THEN '电脑配件' WHEN 4 THEN '音频设备' WHEN 5 THEN '摄影设备'
                 WHEN 6 THEN '电脑外设' WHEN 7 THEN '智能家居' WHEN 8 THEN '运动装备'
                 WHEN 9 THEN '时尚箱包'
                 END AS category_name,
             CONCAT('B', LPAD(FLOOR(RAND() * 10), 2, '0')) AS brand_id,  -- 品牌ID：B00 ~ B09
             -- 品牌名称映射
             CASE FLOOR(RAND() * 10)
                 WHEN 0 THEN 'Apple' WHEN 1 THEN '小米' WHEN 2 THEN '华为'
                 WHEN 3 THEN '戴尔' WHEN 4 THEN '索尼' WHEN 5 THEN '佳能'
                 WHEN 6 THEN '联想' WHEN 7 THEN '美的' WHEN 8 THEN '罗技'
                 WHEN 9 THEN '耐克'
                 END AS brand_name,
             ROUND(RAND() * 9900 + 100, 2) AS price,  -- 随机价格（100.00 ~ 10000.00）
             -- 商品状态（active/inactive 随机）
             CASE WHEN FLOOR(RAND() * 2) = 1 THEN 'active' ELSE 'inactive' END AS status,
             -- 构造创建时间（2025-07-01 ~ 2025-07-31）
             FROM_UNIXTIME(
                     UNIX_TIMESTAMP('2025-07-01 00:00:00') + FLOOR(RAND() * (86400 * 31))
             ) AS create_time
         FROM (
                  -- 生成 0~499 的序列（共 500 条数据）
                  SELECT posexplode(split(space(499), ' ')) AS (seq, val)
              ) t
     ) sub_query  -- 子查询包裹，确保列别名作用域正确
LIMIT 500;  -- 限制结果为 500 条


select  * from ods_product_info;

drop table ods_oders_detail;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_oders_detail
(
    order_id STRING COMMENT '订单ID',
    user_id STRING COMMENT '用户ID',
    shop_id STRING COMMENT '店铺ID',
    product_id STRING COMMENT '商品ID',
    content_id STRING COMMENT '内容ID(视频/直播)',
    order_time TIMESTAMP COMMENT '下单时间',
    pay_time TIMESTAMP COMMENT '支付时间',
    pay_amount DECIMAL(18, 2) COMMENT '支付金额',
    pay_method STRING COMMENT '支付方式',
    order_status TINYINT COMMENT '订单状态(0-未支付,1-已支付,2-已发货,3-已完成,4-已取消,5-已退款)',
    province STRING COMMENT '省份',
    city STRING COMMENT '城市',
    is_first_order BOOLEAN COMMENT '是否首单',
    coupon_amount DECIMAL(18, 2) COMMENT '优惠券金额',
    shipping_fee DECIMAL(18, 2) COMMENT '运费',
    create_time STRING COMMENT '记录创建时间'
);


-- 向 ods_oders_detail 表插入 500 条模拟订单详情数据
INSERT INTO TABLE ods_oders_detail
SELECT
    CONCAT('order_', LPAD(seq, 3, '0')) AS order_id,  -- 订单ID：order_000 ~ order_499
    CONCAT('user_', LPAD(FLOOR(RAND() * 200), 3, '0')) AS user_id,  -- 用户ID：user_000 ~ user_199
    CONCAT('shop_', LPAD(FLOOR(RAND() * 50), 2, '0')) AS shop_id,  -- 店铺ID：shop_00 ~ shop_49
    CONCAT('P', LPAD(FLOOR(RAND() * 200), 3, '0')) AS product_id,  -- 商品ID：P000 ~ P199
    CASE WHEN FLOOR(RAND() * 5) = 0 THEN CONCAT('content_', LPAD(FLOOR(RAND() * 20), 2, '0')) END AS content_id,  -- 内容ID（部分为空）
    -- 订单时间：2025-07-01 ~ 2025-07-31 随机
    FROM_UNIXTIME(UNIX_TIMESTAMP('2025-07-01 00:00:00') + FLOOR(RAND() * (86400 * 31))) AS order_time,
    -- 支付时间：订单时间之后0~7天内随机
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP('2025-07-01 00:00:00') + FLOOR(RAND() * (86400 * 31))))
                + FLOOR(RAND() * 86400 * 7)
    ) AS pay_time,
    ROUND(RAND() * 9900 + 100, 2) AS pay_amount,  -- 支付金额：100.00 ~ 10000.00
    CASE FLOOR(RAND() * 4)
        WHEN 0 THEN 'alipay'
        WHEN 1 THEN 'wechat'
        WHEN 2 THEN 'credit_card'
        WHEN 3 THEN 'debit_card'
        END AS pay_method,  -- 支付方式
    FLOOR(RAND() * 6) AS order_status,  -- 订单状态：0~5
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN '北京'
        WHEN 1 THEN '上海'
        WHEN 2 THEN '广东'
        WHEN 3 THEN '浙江'
        WHEN 4 THEN '江苏'
        WHEN 5 THEN '四川'
        WHEN 6 THEN '湖北'
        WHEN 7 THEN '湖南'
        WHEN 8 THEN '山东'
        WHEN 9 THEN '陕西'
        END AS province,  -- 省份
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN '北京'
        WHEN 1 THEN '上海'
        WHEN 2 THEN '深圳'
        WHEN 3 THEN '杭州'
        WHEN 4 THEN '南京'
        WHEN 5 THEN '成都'
        WHEN 6 THEN '武汉'
        WHEN 7 THEN '长沙'
        WHEN 8 THEN '青岛'
        WHEN 9 THEN '西安'
        END AS city,  -- 城市
    CASE WHEN FLOOR(RAND() * 3) = 0 THEN TRUE ELSE FALSE END AS is_first_order,  -- 是否首单
    ROUND(RAND() * 500, 2) AS coupon_amount,  -- 优惠券金额：0.00 ~ 500.00
    ROUND(RAND() * 50, 2) AS shipping_fee,  -- 运费：0.00 ~ 50.00
    FROM_UNIXTIME(UNIX_TIMESTAMP('2025-07-01 00:00:00') + FLOOR(RAND() * (86400 * 31))) AS create_time  -- 记录创建时间
FROM (
         -- 生成 0~499 的序列（共 500 条数据）
         SELECT posexplode(split(space(499), ' ')) AS (seq, val)
     ) t
LIMIT 500;  -- 限制结果为 500 条



select  * from ods_oders_detail;






-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- 功能：存储清洗后的页面行为明细数据，支持页面概览和装修诊断的基础计算 （页面行为明细）
drop table dwd_page_behavior_detail;

CREATE TABLE dwd_page_behavior_detail (
                                              log_id STRING COMMENT '日志ID',
                                              user_id STRING COMMENT '用户ID',
                                              session_id STRING COMMENT '会话ID',
                                              page_id STRING COMMENT '页面ID',
                                              page_type STRING COMMENT '页面类型（home/detail/custom）',
                                              module_id STRING COMMENT '模块ID',
                                              click_time TIMESTAMP COMMENT '点击时间',
                                              stay_duration INT COMMENT '停留时长（毫秒）',
                                              refer_page STRING COMMENT '来源页面',
                                              device_type STRING COMMENT '设备类型（mobile/pc/tablet）',
                                              os STRING COMMENT '操作系统',
                                              province STRING COMMENT '省份',
                                              city STRING COMMENT '城市'


) COMMENT '页面行为明细数据（DWD层）'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 从ODS层同步并清洗数据
INSERT OVERWRITE TABLE dwd_page_behavior_detail PARTITION (dt='20250730')
SELECT
    log_id,
    user_id,
    session_id,
    page_id,
    page_type,
    module_id,
    click_time,
    stay_duration,
    refer_page,
    device_type,
    os,
    province,
    city
FROM ods_page_behavior;


select  * from dwd_page_behavior_detail;



-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- 功能：汇总页面级别的基础指标（访客数、点击量等），支持页面概览模块  （页面概览汇总）

DROP TABLE IF EXISTS dws_page_overview_stats;

-- 重建表，增加 PARTITIONED BY (dt STRING) 定义分区
CREATE TABLE dws_page_overview_stats (
                                         page_id STRING COMMENT '页面ID',
                                         page_type STRING COMMENT '页面类型',
                                         visitor_count INT COMMENT '访客数（去重）',
                                         click_count INT COMMENT '总点击量',
                                         total_stay_duration INT COMMENT '总停留时长（毫秒）'
) COMMENT '页面概览汇总指标（DWS层）'
    PARTITIONED BY (dt STRING COMMENT '统计日期'); -- 关键：显式定义分区


-- 重新执行插入语句（此时分区逻辑匹配，可正常运行）
INSERT OVERWRITE TABLE dws_page_overview_stats PARTITION (dt='20250730')
SELECT
    page_id,
    page_type,
    COUNT(DISTINCT user_id) AS visitor_count,
    COUNT(CASE WHEN click_time IS NOT NULL THEN log_id END) AS click_count,
    SUM(stay_duration) AS total_stay_duration
FROM dwd_page_behavior_detail
WHERE dt='20250730'
GROUP BY page_id, page_type;

select  * from dws_page_overview_stats;

-- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- 功能：汇总页面模块级别的指标（点击分布、引导支付等），支持装修诊断模块  （模块明细汇总）
drop table dws_page_module_stats;
CREATE TABLE dws_page_module_stats (
       page_id STRING COMMENT '页面ID',
       module_id STRING COMMENT '模块ID',
       click_count INT COMMENT '模块点击量',
       click_user_count INT COMMENT '模块点击人数（去重）',
       guide_pay_amount DECIMAL(18,2) COMMENT '模块引导支付金额',
       dt STRING COMMENT '统计日期'
) COMMENT '页面模块汇总指标（DWS层）';

-- 数据加载：关联订单表计算引导支付金额
INSERT OVERWRITE TABLE dws_page_module_stats
SELECT
    b.page_id,  -- 从页面行为明细数据中获取页面ID
    b.module_id,  -- 从页面行为明细数据中获取模块ID
    COUNT(b.log_id) AS click_count,  -- 统计页面行为明细中该模块的日志数量，作为模块点击量
    COUNT(DISTINCT b.user_id) AS click_user_count,  -- 统计页面行为明细中点击该模块的不同用户数量，去重后作为模块点击人数
    COALESCE(SUM(o.pay_amount), 0) AS guide_pay_amount,  -- 关联订单表，计算点击模块后1天内的支付金额总和，若为NULL则填0
    '20250730' AS dt  -- 明确统计日期为2025年7月30日
FROM dwd_page_behavior_detail b  -- 页面行为明细数据来源表，存储用户对页面模块的行为信息
         LEFT JOIN ods_oders_detail o  -- 左关联订单明细表，用于获取支付金额等信息
                   ON b.user_id = o.user_id  -- 关联条件：用户ID相同
                       AND o.pay_time BETWEEN b.click_time AND DATE_ADD(b.click_time, 1)  -- 关联条件：支付时间在模块点击时间后1天内，视为该模块引导的支付
GROUP BY b.page_id, b.module_id;  -- 按页面ID和模块ID分组，以便计算每个页面模块的汇总指标

select  *from dws_page_module_stats;


--
-- -- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- -- 功能：存储页面概览的最终指标，支持首页、商品详情页、自定义承接页的数据展示    （页面概览指标）
CREATE TABLE ads_page_overview (
      page_id STRING COMMENT '页面ID',
      page_type STRING COMMENT '页面类型（home/detail/custom）',
      visitor_count INT COMMENT '访客数',
      click_count INT COMMENT '总点击量',
      avg_stay_duration DECIMAL(10,2) COMMENT '平均停留时长（秒）',
      dt STRING COMMENT '统计日期'
) COMMENT '页面概览指标（ADS层）';

-- 数据加载：从DWS层计算
INSERT OVERWRITE TABLE ads_page_overview
SELECT
    page_id,
    page_type,
    visitor_count,
    click_count,
    -- 总停留时长转换为秒，计算平均值
    (total_stay_duration / click_count) / 1000 AS avg_stay_duration,
    '20250730' AS dt
FROM dws_page_overview_stats
WHERE dt='20250730';
--
  select  * from ads_page_overview;
--
--
-- -- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- -- 功能：存储页面各模块的点击分布数据，支持查看板块点击量、引导支付金额等  （点击分布指标）
CREATE TABLE ads_page_click_distribution (
                                                 page_id STRING COMMENT '页面ID',
                                                 module_id STRING COMMENT '模块ID',
                                                 click_count INT COMMENT '模块点击量',
                                                 click_user_count INT COMMENT '模块点击人数',
                                                 guide_pay_amount DECIMAL(18,2) COMMENT '引导支付金额',
                                                 dt STRING COMMENT '统计日期'
) COMMENT '页面模块点击分布（ADS层）';

-- 数据加载：从DWS层直接同步（已聚合完成）
INSERT OVERWRITE TABLE ads_page_click_distribution
SELECT
    page_id,
    module_id,
    click_count,
    click_user_count,
    guide_pay_amount,
    '20250730' AS dt
FROM dws_page_module_stats
WHERE dt='20250730';

select  * from ads_page_click_distribution;
--
-- -- 工单编号：大数据-电商数仓-10-流量主题页面分析看板
-- -- 功能：存储近30天页面的访客数、点击人数趋势，支持数据趋势模块
CREATE TABLE ads_page_trend_30d (
                                        page_id STRING COMMENT '页面ID',
                                        stat_date STRING COMMENT '统计日期',
                                        visitor_count INT COMMENT '当日访客数',
                                        click_user_count INT COMMENT '当日点击人数',
                                        dt STRING COMMENT '分区日期'
) COMMENT '近30天页面数据趋势（ADS层）';

-- 数据加载：从DWD层按日期聚合近30天数据（近 30 天数据趋势
INSERT OVERWRITE TABLE ads_page_trend_30d
SELECT
    page_id,
    dt AS stat_date,
    COUNT(DISTINCT user_id) AS visitor_count,  -- 当日访客数
    COUNT(DISTINCT CASE WHEN click_time IS NOT NULL THEN user_id END) AS click_user_count,  -- 当日点击人数
    '20250730' AS dt
FROM dwd_page_behavior_detail
WHERE dt >= DATE_SUB('20250730', 29)  -- 近30天（含当天）
GROUP BY page_id, dt;

select *
from ads_page_trend_30d;

--
--
-- -- 工单编号：大数据-电商数仓-10-流量主题页面分析看板（引导详情指标）
-- -- 功能：存储页面引导至商品的转化数据，支持查看引导下单的商品详情
CREATE TABLE ads_page_guide_detail (
                                           page_id STRING COMMENT '页面ID',
                                           product_id STRING COMMENT '商品ID',
                                           product_name STRING COMMENT '商品名称',
                                           guide_visitor_count INT COMMENT '引导访客数（点击页面后访问商品的用户）',
                                           guide_pay_amount DECIMAL(18,2) COMMENT '引导支付金额',
                                           dt STRING COMMENT '统计日期'
) COMMENT '页面引导商品详情（ADS层）';
--
-- -- 数据加载：关联商品表和订单表，计算引导转化
INSERT OVERWRITE TABLE ads_page_guide_detail
SELECT
    b.page_id,
    -- 处理 product_id 空值：如果订单表无数据，显示 '无订单'
    COALESCE(o.product_id, '无订单') AS product_id,
    -- 处理 product_name 空值：如果商品表无数据，显示 '未知商品'
    COALESCE(p.product_name, '未知商品') AS product_name,
    COUNT(DISTINCT b.user_id) AS guide_visitor_count,
    SUM(o.pay_amount) AS guide_pay_amount,
    '20250730' AS dt
FROM dwd_page_behavior_detail b
         LEFT JOIN ods_oders_detail o
                   ON b.user_id = o.user_id
                       AND o.pay_time BETWEEN b.click_time AND DATE_ADD(b.click_time, 1)
         LEFT JOIN ods_product_info p
                   ON o.product_id = p.product_id
WHERE b.dt='20250730'
GROUP BY
    b.page_id,
    COALESCE(o.product_id, '无订单'),  -- GROUP BY 要和 SELECT 对齐
    COALESCE(p.product_name, '未知商品');



select  * from ads_page_guide_detail;
--
--
--
-- -- 工单编号：大数据-电商数仓-10-流量主题页面分析看板（分布明细指标）
-- -- 功能：存储页面各模块的详细分布数据，支持查看模块类型的占比等

drop table  ads_page_module_distribution;
CREATE TABLE ads_page_module_distribution (
     page_id STRING COMMENT '页面ID',
     module_type STRING COMMENT '模块类型（ad/recommend/nav等）',
     module_count INT COMMENT '该类型模块数量',
     total_click_count INT COMMENT '该类型模块总点击量',
     click_ratio DECIMAL(10,2) COMMENT '点击量占比（该类型点击量/页面总点击量）',
     dt STRING COMMENT '统计日期'
) COMMENT '页面模块分布明细（ADS层）';
--
-- -- 数据加载：从DWS层聚合计算
INSERT OVERWRITE TABLE ads_page_module_distribution
SELECT
    page_id,
    COUNT(DISTINCT module_id) AS module_count,
    SUM(click_count) AS total_click_count,
    (SUM(click_count) / SUM(SUM(click_count)) OVER (PARTITION BY page_id)) * 100 AS click_ratio,
    '20250730' AS dt,
    'ad' AS module_type -- 补充表中存在的、SELECT 遗漏的列，值按实际业务填
FROM dws_page_module_stats
WHERE dt='20250730'
GROUP BY page_id;

select  * from  ads_page_module_distribution;