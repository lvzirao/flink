
# **达摩盘**
任务标签：年龄、性别、年代、身高、体重、星座 6 类标签
数据来源：淘宝平台用户行为日志数据（包括但不限于点击、浏览、搜索、
收藏、加购、下单、支付、商品评价等）、商品类目属性数据
表结构设计
1. 用户基础信息表 (user_basic_info)
CREATE TABLE user_basic_info (
user_id BIGINT PRIMARY KEY COMMENT '用户唯一标识',
username VARCHAR(100) COMMENT '用户名',
birth_date DATE COMMENT '出生日期(用于计算年龄和星座)',
register_time DATETIME COMMENT '注册时间',
last_update_time DATETIME COMMENT '最后更新时间',
data_source VARCHAR(20) COMMENT '数据来源(如:淘宝、天猫等)',
is_valid TINYINT DEFAULT 1 COMMENT '是否有效用户(1有效,0无效)',
create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
INDEX idx_update_time (update_time)
) COMMENT '用户基础信息表';
 用途：存储用户的基本注册信息
2. 用户行为日志表 (user_behavior_log)
CREATE TABLE user_behavior_log (
log_id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '日志ID',
user_id BIGINT NOT NULL COMMENT '用户ID',
behavior_type VARCHAR(20) NOT NULL COMMENT '行为类型(浏览/收藏/加购/购买等)',
item_id BIGINT COMMENT '商品ID',
category_id BIGINT COMMENT '商品类目ID',
brand_id BIGINT COMMENT '品牌ID',
price DECIMAL(10,2) COMMENT '商品价格',
behavior_time DATETIME NOT NULL COMMENT '行为时间',
device_type VARCHAR(20) COMMENT '设备类型(iOS/Android/PC)',
os_version VARCHAR(50) COMMENT '操作系统版本',
session_id VARCHAR(100) COMMENT '会话ID',
ip_address VARCHAR(50) COMMENT 'IP地址',
create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
INDEX idx_user_id (user_id),
INDEX idx_behavior_time (behavior_time),
INDEX idx_category_id (category_id),
INDEX idx_behavior_type (behavior_type)
) COMMENT '用户行为日志表';
   用途：记录用户的各种行为数据
3. 用户标签主表 (user_tags_main)
CREATE TABLE user_tags_main (
id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
user_id BIGINT NOT NULL COMMENT '用户ID',
tag_type VARCHAR(50) NOT NULL COMMENT '标签类型(age/gender/weight/height/constellation)',
tag_value VARCHAR(100) COMMENT '标签值',
confidence_score DECIMAL(5,2) COMMENT '置信度分数(0-1)',
data_source VARCHAR(50) COMMENT '数据来源',
effective_date DATE COMMENT '生效日期',
expiry_date DATE COMMENT '失效日期',
is_current TINYINT DEFAULT 1 COMMENT '是否当前有效标签(1是,0否)',
create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
UNIQUE KEY uk_user_tag (user_id, tag_type, effective_date),
INDEX idx_tag_type (tag_type),
INDEX idx_user_tag (user_id, tag_type, is_current)
) COMMENT '用户标签主表';
   用途：存储用户的各种标签信息
4. 年龄标签计算中间表 (user_age_calculation)
   CREATE TABLE user_age_calculation (
   id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
   user_id BIGINT NOT NULL COMMENT '用户ID',
   calculation_date DATE NOT NULL COMMENT '计算日期',
   age_range VARCHAR(20) COMMENT '年龄段(18-24/25-29等)',
   category_score DECIMAL(5,2) COMMENT '类目偏好得分',
   brand_score DECIMAL(5,2) COMMENT '品牌偏好得分',
   price_score DECIMAL(5,2) COMMENT '价格敏感度得分',
   time_score DECIMAL(5,2) COMMENT '时间行为得分',
   search_score DECIMAL(5,2) COMMENT '搜索词分析得分',
   social_score DECIMAL(5,2) COMMENT '社交互动行为得分',
   device_score DECIMAL(5,2) COMMENT '设备信息得分',
   total_score DECIMAL(5,2) COMMENT '总分',
   final_age_range VARCHAR(20) COMMENT '最终确定的年龄段',
   create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   INDEX idx_user_id (user_id),
   INDEX idx_calculation_date (calculation_date)
   ) COMMENT '年龄标签计算中间表';
   用途：存储年龄标签计算的中间结果
5. 性别标签计算中间表 (user_gender_calculation)
CREATE TABLE user_gender_calculation (
id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
user_id BIGINT NOT NULL COMMENT '用户ID',
calculation_date DATE NOT NULL COMMENT '计算日期',
female_score DECIMAL(5,2) COMMENT '女性相关得分',
male_score DECIMAL(5,2) COMMENT '男性相关得分',
family_score DECIMAL(5,2) COMMENT '家庭相关得分',
purchase_count INT COMMENT '购买行为次数',
cart_favorite_count INT COMMENT '加购/收藏行为次数',
browse_count INT COMMENT '浏览行为次数',
final_gender VARCHAR(20) COMMENT '最终性别标签',
create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
INDEX idx_user_id (user_id),
INDEX idx_calculation_date (calculation_date)
) COMMENT '性别标签计算中间表';
   用途：存储性别标签计算的中间结果
6. 身高体重数据源表 (user_body_measurement_source)
   CREATE TABLE user_body_measurement_source (
   id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
   user_id BIGINT NOT NULL COMMENT '用户ID',
   measurement_type VARCHAR(10) NOT NULL COMMENT '测量类型(height/weight)',
   original_value VARCHAR(50) COMMENT '原始值',
   standardized_value DECIMAL(6,1) COMMENT '标准化后的值',
   unit VARCHAR(10) COMMENT '单位',
   source_type VARCHAR(50) NOT NULL COMMENT '来源类型(订单/会员资料/健康设备/活动表单)',
   source_weight DECIMAL(3,1) COMMENT '来源权重',
   record_time DATETIME COMMENT '记录时间',
   is_valid TINYINT DEFAULT 1 COMMENT '是否有效(1有效,0无效)',
   create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   INDEX idx_user_measurement (user_id, measurement_type),
   INDEX idx_source_type (source_type)
   ) COMMENT '身高体重数据源表';
   用途：存储用户身高体重的原始数据
7. 星座标签表 (user_constellation)
   CREATE TABLE user_constellation (
   id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
   user_id BIGINT NOT NULL UNIQUE COMMENT '用户ID',
   birth_date DATE COMMENT '出生日期',
   constellation VARCHAR(20) COMMENT '星座',
   is_verified TINYINT DEFAULT 0 COMMENT '是否已验证(1是,0否)',
   last_update_time DATETIME COMMENT '最后更新时间',
   create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   INDEX idx_constellation (constellation)
   ) COMMENT '用户星座标签表';
   用途：存储用户的星座信息
8.  标签类目权重配置表 (tag_category_weight_config)
    CREATE TABLE tag_category_weight_config (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    tag_type VARCHAR(50) NOT NULL COMMENT '标签类型',
    dimension VARCHAR(50) NOT NULL COMMENT '维度名称',
    weight DECIMAL(5,2) NOT NULL COMMENT '权重(0-1)',
    effective_date DATE NOT NULL COMMENT '生效日期',
    expiry_date DATE COMMENT '失效日期',
    description VARCHAR(200) COMMENT '描述',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_tag_type (tag_type),
    UNIQUE KEY uk_tag_dimension (tag_type, dimension, effective_date)
    ) COMMENT '标签类目权重配置表';
    用途：存储标签计算所需的权重配置
# 数据关系
user_basic_info表是所有其他表的基准，通过user_id关联
user_behavior_log提供行为数据用于标签计算
各类计算中间表存储计算结果
user_tags_main汇总最终标签结果
# 数据流向：
基础信息 → 行为日志 → 计算中间表 → 标签主表
配置表为计算过程提供权重参数
# 设计特点
原始数据层（基础信息、行为日志）  
计算中间层（各类计算表）
结果汇总层（标签主表）
# 历史记录：
通过effective_date和expiry_date支持标签历史版本
is_current字段标识当前有效标签
# 可配置性：
通过tag_category_weight_config表实现计算权重的灵活配置
# 数据质量：
包含置信度分数、验证状态等字段
支持数据来源追踪
# 使用建议
高频查询应使用已建立的索引
历史数据分析可基于calculation_date或behavior_time
# 数据更新：
标签更新时需同时更新is_current状态
重要变更应记录历史版本
# 扩展性：
新增标签类型可通过tag_type扩展
新增计算维度可参考现有中间表结构





















