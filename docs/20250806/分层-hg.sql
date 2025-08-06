create database hg;
use hg;
-- 用户商品行为日志表（ods_sku_behavior_log）
drop table ods_sku_behavior_log;
CREATE EXTERNAL TABLE ods_sku_behavior_log (
   user_id STRING COMMENT '用户唯一标识（匿名用户用设备ID）',
   sku_id STRING COMMENT '商品ID',
   spu_id STRING COMMENT '商品所属SPU ID',
   behavior_type STRING COMMENT '行为类型：view（访问详情页）、favorite（收藏）、add_cart（加购）',
   behavior_time STRING COMMENT '行为发生时间（精确到秒，格式yyyy-MM-dd HH:mm:ss）',
   terminal_type STRING COMMENT '终端类型：pc（电脑端）、wireless（无线端，含手机/Pad）',
   is_detail_view BOOLEAN COMMENT '是否访问商品详情页（true/false，用于区分店铺页访客）',
   stay_seconds BIGINT COMMENT '详情页停留时长（秒，仅behavior_type=view时有效）',
   is_click_in_detail BOOLEAN COMMENT '详情页内是否有点击行为（用于计算跳出率）',
   add_cart_num BIGINT COMMENT '加购件数（仅behavior_type=add_cart时有效，默认1）'
) COMMENT '用户商品行为原始日志（含访问、收藏、加购）'
    PARTITIONED BY (dt STRING, hour STRING)
    STORED AS ORC
    LOCATION '/warehouse/ods/sku_behavior_log'
    TBLPROPERTIES (
        'orc.compress'='SNAPPY',
        'comment'='每日增量同步，保留90天原始日志'
        );


-- 强制本地执行（避免集群资源问题）
set hive.exec.mode.local.auto=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 插入100条确保有数据的记录
INSERT INTO TABLE ods_sku_behavior_log
    PARTITION (dt='2023-10-01', hour)
SELECT
    -- 确保用户ID非空且格式正确
    concat('user_', cast(floor(rand() * 1000) as int)) as user_id,
    -- 确保商品ID非空
    concat('sku_', cast(floor(rand() * 500) as int)) as sku_id,
    -- 确保SPU ID非空
    concat('spu_', cast(floor(rand() * 100) as int)) as spu_id,
    -- 行为类型固定三选一，避免null
    CASE floor(rand() * 3)
        WHEN 0 THEN 'view'
        WHEN 1 THEN 'favorite'
        ELSE 'add_cart'
        END as behavior_type,
    -- 时间字段强制生成有效格式（非null）
    date_format(
            from_unixtime(
                    unix_timestamp('2023-10-01', 'yyyy-MM-dd') +
                    cast(floor(rand() * 86400) as bigint)
            ),
            'yyyy-MM-dd HH:mm:ss'
    ) as behavior_time,
    -- 终端类型非null
    CASE floor(rand() * 2)
        WHEN 0 THEN 'pc'
        ELSE 'wireless'
        END as terminal_type,
    -- 布尔值非null
    rand() > 0.2 as is_detail_view,
    -- 停留时长：仅view时有值，否则null（符合业务逻辑）
    CASE
        WHEN CASE floor(rand() * 3)
                 WHEN 0 THEN 'view'
                 WHEN 1 THEN 'favorite'
                 ELSE 'add_cart'
                 END = 'view'
            THEN cast(floor(rand() * 290 + 10) as bigint)
        ELSE NULL
        END as stay_seconds,
    -- 详情页点击：仅详情页访问时有值
    CASE
        WHEN rand() > 0.2  -- 与is_detail_view逻辑一致
            THEN rand() > 0.4
        ELSE NULL
        END as is_click_in_detail,
    -- 加购数量：仅add_cart时有值
    CASE
        WHEN CASE floor(rand() * 3)
                 WHEN 0 THEN 'view'
                 WHEN 1 THEN 'favorite'
                 ELSE 'add_cart'
                 END = 'add_cart'
            THEN cast(floor(rand() * 5 + 1) as bigint)
        ELSE NULL
        END as add_cart_num,
    -- 小时分区：从时间字段提取，确保非null
    substr(
            date_format(
                    from_unixtime(
                            unix_timestamp('2023-10-01', 'yyyy-MM-dd') +
                            cast(floor(rand() * 86400) as bigint)
                    ),
                    'yyyy-MM-dd HH:mm:ss'
            ),
            12, 2
    ) as hour
FROM (
         -- 生成100条记录（确保数量准确）
         SELECT pos
         FROM (
                  SELECT stack(100
                             ,1,2,3,4,5,6,7,8,9,10
                             ,11,12,13,14,15,16,17,18,19,20
                             ,21,22,23,24,25,26,27,28,29,30
                             ,31,32,33,34,35,36,37,38,39,40
                             ,41,42,43,44,45,46,47,48,49,50
                             ,51,52,53,54,55,56,57,58,59,60
                             ,61,62,63,64,65,66,67,68,69,70
                             ,71,72,73,74,75,76,77,78,79,80
                             ,81,82,83,84,85,86,87,88,89,90
                             ,91,92,93,94,95,96,97,98,99,100
                         ) as pos
              ) t
     ) gen_rows;




set hive.exec.mode.local.auto=True;

select * from ods_sku_behavior_log;




-- 商品基础信息全量表（ods_sku_base_full）
CREATE EXTERNAL TABLE ods_sku_base_full (
                                            sku_id STRING COMMENT '商品ID',
                                            sku_name STRING COMMENT '商品名称',
                                            spu_id STRING COMMENT 'SPU ID',
                                            category1_id STRING COMMENT '一级类目ID',
                                            category2_id STRING COMMENT '二级类目ID',
                                            category3_id STRING COMMENT '叶子类目ID（最细分类目）',
                                            brand_id STRING COMMENT '品牌ID',
                                            price DECIMAL(10,2) COMMENT '商品定价（原价）',
                                            sale_price DECIMAL(10,2) COMMENT '当前售价',
                                            is_online BOOLEAN COMMENT '是否上架（true=在售，false=下架）',
                                            create_time STRING COMMENT '商品创建时间（上架时间）',
                                            update_time STRING COMMENT '信息最后更新时间'
) COMMENT '商品基础信息每日全量快照'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS RCFILE
    LOCATION '/warehouse/ods/sku_base_full'
    TBLPROPERTIES (
        'comment'='每日0点同步前一天全量数据，保留30天快照'
        );

-- 插入100条商品基础信息（全量快照）
INSERT INTO TABLE ods_sku_base_full PARTITION (dt='2023-10-01')
SELECT
    concat('sku_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as sku_id,
    concat('商品', cast(floor(rand() * 10000) as int), '-', floor(rand() * 100)) as sku_name,
    concat('spu_', lpad(cast(floor(rand() * 1000) as int), 4, '0')) as spu_id,
    concat('c1_', floor(rand() * 10 + 1)) as category1_id,  -- 一级类目1-10
    concat('c2_', floor(rand() * 50 + 1)) as category2_id,  -- 二级类目1-50
    concat('c3_', floor(rand() * 200 + 1)) as category3_id, -- 叶子类目1-200
    concat('brand_', floor(rand() * 100 + 1)) as brand_id,   -- 品牌1-100
    cast(floor(rand() * 9900 + 100) / 100 as decimal(10,2)) as price,  -- 1.00-99.99
    cast(floor(rand() * 9900 + 100) / 100 as decimal(10,2)) as sale_price,
    rand() > 0.2 as is_online,  -- 80%在售
    concat('2023-', lpad(cast(floor(rand() * 9 + 1) as int), 2, '0'), '-',
           lpad(cast(floor(rand() * 28 + 1) as int), 2, '0')) as create_time,  -- 2023-01至09月
    concat('2023-10-01') as update_time  -- 全量快照更新时间为分区日
FROM (
         SELECT explode(split(space(99), ' ')) as dummy  -- 生成100条
     ) t;

select  *from ods_sku_base_full;



-- 三、订单明细表（ods_order_detail_inc）
CREATE EXTERNAL TABLE IF NOT EXISTS ods_order_detail_inc (
                                                             order_id STRING COMMENT '订单ID',
                                                             order_detail_id STRING COMMENT '订单项ID（一个订单含多个商品时唯一标识）',
                                                             user_id STRING COMMENT '下单用户ID',
                                                             sku_id STRING COMMENT '商品ID',
                                                             order_num BIGINT COMMENT '下单件数',
                                                             order_price DECIMAL(10,2) COMMENT '下单单价（商品实际成交价）',
                                                             order_amount DECIMAL(10,2) COMMENT '订单项金额（order_num * order_price）',
                                                             terminal_type STRING COMMENT '下单终端：pc/wireless',
                                                             create_time STRING COMMENT '下单时间（格式yyyy-MM-dd HH:mm:ss）',
                                                             order_status STRING COMMENT '订单状态：pending/paid/cancelled',
                                                             is_pre_sale BOOLEAN COMMENT '是否预售订单（true=预售，false=普通）'
)
    COMMENT '商品订单明细增量数据（实时同步变更，按下单日期分区）'
    PARTITIONED BY (
        dt STRING COMMENT '下单日期分区，格式yyyy-MM-dd'
        );

-- 插入100条订单明细（增量数据）
INSERT INTO TABLE ods_order_detail_inc PARTITION (dt='2023-10-01')
SELECT
    concat('order_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as order_id,
    concat('od_', lpad(cast(floor(rand() * 100000) as int), 6, '0')) as order_detail_id,
    concat('user_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as user_id,
    concat('sku_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as sku_id,
    cast(floor(rand() * 10 + 1) as bigint) as order_num,  -- 1-10件
    cast(floor(rand() * 9900 + 100) / 100 as decimal(10,2)) as order_price,  -- 1.00-99.99
    cast((floor(rand() * 10 + 1) * floor(rand() * 9900 + 100) / 100) as decimal(10,2)) as order_amount,
    CASE WHEN rand() < 0.7 THEN 'wireless' ELSE 'pc' END as terminal_type,  -- 70%无线端
    concat('2023-10-01 ', lpad(cast(floor(rand() * 24) as int), 2, '0'), ':',
           lpad(cast(floor(rand() * 60) as int), 2, '0'), ':',
           lpad(cast(floor(rand() * 60) as int), 2, '0')) as create_time,
    CASE
        WHEN rand() < 0.6 THEN 'paid'
        WHEN rand() < 0.8 THEN 'pending'
        ELSE 'cancelled'
        END as order_status,  -- 60%已支付，20%待支付，20%取消
    rand() < 0.1 as is_pre_sale  -- 10%预售
FROM (
         SELECT explode(split(space(99), ' ')) as dummy  -- 生成100条
     ) t;
select  * from ods_order_detail_inc;


-- 四、支付明细表（ods_payment_detail_inc）
drop table ods_payment_detail_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_payment_detail_inc (
                                                               payment_id STRING COMMENT '支付流水ID',
                                                               order_id STRING COMMENT '关联订单ID',
                                                               order_detail_id STRING COMMENT '关联订单项ID',
                                                               user_id STRING COMMENT '支付用户ID',
                                                               sku_id STRING COMMENT '商品ID',
                                                               pay_num BIGINT COMMENT '支付件数（对应订单项实际支付数量）',
                                                               pay_amount DECIMAL(10,2) COMMENT '支付金额（未剔除退款，含预售尾款）',
                                                               pay_time STRING COMMENT '支付完成时间（预售按尾款付清时间，格式yyyy-MM-dd HH:mm:ss）',
                                                               terminal_type STRING COMMENT '下单终端（pc/wireless，区分PC/无线支付）',
                                                               pay_type STRING COMMENT '支付方式：alipay/wechat/cod',
                                                               is_juhuasuan BOOLEAN COMMENT '是否聚划算活动订单',
                                                               pre_sale_pay_stage STRING COMMENT '预售阶段：deposit/balance/null'
)
    COMMENT '商品支付明细增量数据（支付完成后同步，按日期分区）'
    PARTITIONED BY (dt STRING COMMENT '支付日期分区，格式yyyy-MM-dd')
-- 替换为 Hive 原生 JSON SerDe（无需额外依赖，兼容性最佳）
    STORED AS PARQUET
    LOCATION '/warehouse/ods/payment_detail_inc'
    TBLPROPERTIES (
        'parquet.compression'='SNAPPY',
        'transient_lastDdlTime'='${hiveconf:current_timestamp}',
        'comment'='支付完成后同步数据，按支付日期分区'
        );

-- 插入100条支付明细（增量数据）
INSERT INTO TABLE ods_payment_detail_inc PARTITION (dt='2023-10-01')
SELECT
    concat('pay_', lpad(cast(floor(rand() * 100000) as int), 6, '0')) as payment_id,
    concat('order_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as order_id,
    concat('od_', lpad(cast(floor(rand() * 100000) as int), 6, '0')) as order_detail_id,
    concat('user_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as user_id,
    concat('sku_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as sku_id,
    cast(floor(rand() * 10 + 1) as bigint) as pay_num,  -- 1-10件
    cast((floor(rand() * 10 + 1) * floor(rand() * 9900 + 100) / 100) as decimal(10,2)) as pay_amount,
    concat('2023-10-01 ', lpad(cast(floor(rand() * 24) as int), 2, '0'), ':',
           lpad(cast(floor(rand() * 60) as int), 2, '0'), ':',
           lpad(cast(floor(rand() * 60) as int), 2, '0')) as pay_time,
    CASE WHEN rand() < 0.7 THEN 'wireless' ELSE 'pc' END as terminal_type,
    CASE
        WHEN rand() < 0.6 THEN 'alipay'
        WHEN rand() < 0.9 THEN 'wechat'
        ELSE 'cod'
        END as pay_type,  -- 60%支付宝，30%微信，10%货到付款
    rand() < 0.2 as is_juhuasuan,  -- 20%聚划算
    CASE
        WHEN rand() < 0.05 THEN 'deposit'
        WHEN rand() < 0.1 THEN 'balance'
        ELSE null
        END as pre_sale_pay_stage  -- 5%定金，5%尾款，90%非预售
FROM (
         SELECT explode(split(space(99), ' ')) as dummy  -- 生成100条
     ) t;

select * from ods_payment_detail_inc;


-- 退款明细表（ods_refund_detail_inc）
CREATE EXTERNAL TABLE ods_refund_detail_inc (
                                                refund_id STRING COMMENT '退款单ID',
                                                order_id STRING COMMENT '关联订单ID',
                                                order_detail_id STRING COMMENT '关联订单项ID',
                                                sku_id STRING COMMENT '商品ID',
                                                user_id STRING COMMENT '退款用户ID',
                                                refund_amount DECIMAL(10,2) COMMENT '成功退款金额（含仅退款/退货退款）',
                                                refund_type STRING COMMENT '退款类型：only_refund（仅退款）、return_refund（退货退款）',
                                                refund_time STRING COMMENT '退款成功时间（格式yyyy-MM-dd HH:mm:ss）',
                                                is_cod_refund BOOLEAN COMMENT '是否货到付款退款（true=是，不计入成功退款金额；false=否，计入）'
) COMMENT '商品退款明细增量数据'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/ods/refund_detail_inc'
    TBLPROPERTIES (
        'comment'='退款成功后同步数据，按退款日期分区'
        );

-- 插入100条退款明细（增量数据）
INSERT INTO TABLE ods_refund_detail_inc PARTITION (dt='2023-10-01')
SELECT
    concat('refund_', lpad(cast(floor(rand() * 100000) as int), 6, '0')) as refund_id,
    concat('order_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as order_id,
    concat('od_', lpad(cast(floor(rand() * 100000) as int), 6, '0')) as order_detail_id,
    concat('sku_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as sku_id,
    concat('user_', lpad(cast(floor(rand() * 10000) as int), 5, '0')) as user_id,
    cast(floor(rand() * 9900 + 100) / 100 as decimal(10,2)) as refund_amount,  -- 1.00-99.99
    CASE WHEN rand() < 0.7 THEN 'only_refund' ELSE 'return_refund' END as refund_type,  -- 70%仅退款
    concat('2023-10-01 ', lpad(cast(floor(rand() * 24) as int), 2, '0'), ':',
           lpad(cast(floor(rand() * 60) as int), 2, '0'), ':',
           lpad(cast(floor(rand() * 60) as int), 2, '0')) as refund_time,
    rand() < 0.1 as is_cod_refund  -- 10%货到付款退款
FROM (
         SELECT explode(split(space(99), ' ')) as dummy  -- 生成100条
     ) t;

select *
from ods_refund_detail_inc;


-- 商品区间配置表（ods_sku_range_config）
CREATE EXTERNAL TABLE ods_sku_range_config (
                                               range_type STRING COMMENT '区间类型：price_band（价格带）、pay_num（支付件数）、pay_amount（支付金额）',
                                               range_id STRING COMMENT '区间ID（如price_band_1、pay_num_2）',
                                               min_val DECIMAL(16,2) COMMENT '区间最小值（含）',
                                               max_val DECIMAL(16,2) COMMENT '区间最大值（含，无上界时为999999999）',
                                               range_name STRING COMMENT '区间名称（如0~50、51~100）',
                                               create_time STRING COMMENT '配置创建时间'
) COMMENT '商品区间分析固定分档配置表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/warehouse/ods/sku_range_config'
    TBLPROPERTIES (
        'comment'='手动维护的区间分档配置，如支付件数0~50、51~100等'
        );




-- 插入100条区间配置数据（固定分档）
INSERT INTO TABLE ods_sku_range_config
SELECT
    -- 按类型平均分布：价格带、支付件数、支付金额
    CASE
        WHEN mod(id, 3) = 0 THEN 'price_band'
        WHEN mod(id, 3) = 1 THEN 'pay_num'
        ELSE 'pay_amount'
        END as range_type,
    concat(
            CASE
                WHEN mod(id, 3) = 0 THEN 'price_band_'
                WHEN mod(id, 3) = 1 THEN 'pay_num_'
                ELSE 'pay_amount_'
                END,
            cast(id as string)
    ) as range_id,
    -- 区间最小值（按类型区分）
    CASE
        WHEN mod(id, 3) = 0 THEN cast((id % 10) * 50 as decimal(16,2))  -- 价格带：0,50,100...
        WHEN mod(id, 3) = 1 THEN cast((id % 10) * 10 as decimal(16,2))   -- 支付件数：0,10,20...
        ELSE cast((id % 10) * 1000 as decimal(16,2))                    -- 支付金额：0,1000,2000...
        END as min_val,
    -- 区间最大值（按类型区分）
    CASE
        WHEN mod(id, 3) = 0 THEN cast(((id % 10) + 1) * 50 as decimal(16,2))
        WHEN mod(id, 3) = 1 THEN cast(((id % 10) + 1) * 10 as decimal(16,2))
        ELSE cast(((id % 10) + 1) * 1000 as decimal(16,2))
        END as max_val,
    -- 区间名称
    concat(
            cast(CASE
                     WHEN mod(id, 3) = 0 THEN (id % 10) * 50
                     WHEN mod(id, 3) = 1 THEN (id % 10) * 10
                     ELSE (id % 10) * 1000
                END as string),
            '~',
            cast(CASE
                     WHEN mod(id, 3) = 0 THEN ((id % 10) + 1) * 50
                     WHEN mod(id, 3) = 1 THEN ((id % 10) + 1) * 10
                     ELSE ((id % 10) + 1) * 1000
                END as string)
    ) as range_name,
    '2023-01-01' as create_time  -- 配置创建时间固定
FROM (
         SELECT row_number() over () as id  -- 生成1-100序号
         FROM (SELECT explode(split(space(99), ' ')) as dummy) t
     ) t;
select  * from ods_sku_range_config;