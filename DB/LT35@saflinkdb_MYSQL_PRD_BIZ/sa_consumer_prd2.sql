USE `logquery`;

drop table if exists project_user_number;
create table project_user_number
(
    id           int(11) primary key not null auto_increment,
    day_uv       bigint(20)          not null,
    month_uv     bigint(20)          not null,
    one_day      varchar(255),
    project_name varchar(255)        not null
);

drop table if exists root_node_info;
create table root_node_info
(
    id int(20) primary key not null auto_increment,
    code varchar(255) not null
);

drop table if exists sa_kafka_consumption_info;
create table sa_kafka_consumption_info
(
    id int(11) primary key not null auto_increment,
    hdfs_path varchar(255) not null,
    kafka_group_id varchar(255) not null,
    kafka_server_prot varchar(255) not null,
    kafka_topic varchar(100) not null
);

drop table if exists sa_output_type;
create table sa_output_type
(
    id bigint(20) primary key not null auto_increment,
    label varchar(255),
    type int(11)
);

drop table if exists sa_project_ext;
create table sa_project_ext
(
    id bigint(20) primary key not null auto_increment,
    data_role_id varchar(255),
    project_name varchar(255),
    trans_rule_flag int(11)
);

drop table if exists sa_project_task;
create table sa_project_task
(
    id bigint(20) primary key not null auto_increment,
    cname varchar(255),
    create_time datetime(3),
    name varchar(255),
    pass_word varchar(255),
    project_status int(11),
    remark text,
    status int(11),
    type int(11),
    update_time datetime(3),
    open_stream bit(1)
);

alter table sa_source_sink add source_insert_type int(11);
alter table sa_source_sink add create_time datetime(0);
alter table sa_source_sink add create_by varchar(255);
alter table sa_source_sink add update_time datetime(0);
alter table sa_source_sink add update_by varchar(255);

alter table sa_trans_rule change out_put_field output_field varchar(255);
alter table sa_trans_rule add filter int(11);
alter table sa_trans_rule add sink_projectcname varchar(255);
alter table sa_trans_rule add create_time datetime(0);
alter table sa_trans_rule add create_by varchar(255);
alter table sa_trans_rule add update_time datetime(0);
alter table sa_trans_rule add update_by varchar(255);

drop table if exists source_sink_flag;
create table source_sink_flag
(
    id bigint(20) primary key not null auto_increment,
    flag int(11) not null
);

drop table if exists `sql_lock`;
create table `sql_lock`
(
  `id` bigint(20) primary key not null auto_increment,
  `name` varchar(255) not null ,
  `status` int(11) not null ,
  `update_time` datetime
);

drop table if exists `data_import_config`;
create table `data_import_config`
(
    `id` bigint(20) primary key NOT NULL AUTO_INCREMENT,
    `a_distinct_id` varchar(255),
    `a_original_id` varchar(255),
    `a_time` varchar(255),
    `e_id` varchar(255),
    `e_is_login` bit(1),
    `e_name` varchar(255) ,
    `e_time` varchar(255) ,
    `name` varchar(255) ,
    `num` varchar(255) ,
    `project_name` varchar(255) ,
    `type` int(11),
    `u_id` varchar(255),
    `u_is_login` bit(1) ,
    `u_time` varchar(255) ,
    `update_by` varchar(255) ,
    `update_time` datetime(0) DEFAULT NULL
);

-- 计数记录表
drop table if exists sa_record_count;
CREATE TABLE sa_record_count (
	id int(11) AUTO_INCREMENT primary key,
	source_server_port varchar(1024) not null comment '输入流 Kafka 集群服务器及端口',
	source_group_id varchar(255) not null comment '输入流 Kafka 消费组',
	source_topic varchar(100) not null comment '输入流 Topic',
	sink_project_id varchar(1024) not null comment '输出流目标神策项目 ID',
	stage varchar(100) comment '本条记录表示的计数阶段，含 SOURCE、PARSED、SINK、SIGNUP、INVALID、NEED_SPLIT、SPLIT',
	cnt bigint not null comment '计数',
    update_time datetime comment '录入时间'
) comment='输入输出 Kafka 信息对应表';

ALTER TABLE sa_record_count
  MODIFY update_time varchar(50);


-- Topic拆分规则对应表：sa_topic_split drop table if exists sa_topic_split;
CREATE TABLE sa_topic_split (
        id int(11) AUTO_INCREMENT primary key,
        sink_topic varchar(100) not null comment '输出Kafka主题',
        sink_project_id varchar(1024) not null comment '输出目标神策项目 ID',
        source_field_1 varchar(100) not null comment '识别字段名1',
        source_field_value_1 varchar(100) not null comment '识别字段值1',
        source_field_2 varchar(100) comment '识别字段名2',
        source_field_value_2 varchar(100) comment '识别字段值2',
        source_field_3 varchar(100)  comment '识别字段名3',
        source_field_value_3 varchar(100) comment '识别字段值3',
        valid_flag int(1) default 1 comment '该条规则是否有效 ，默认为1',
        create_time datetime not null comment '记录创建时间',
        create_by varchar(255) not null comment '记录创建人',
        update_time datetime comment '记录更新时间',
        update_by varchar(255) comment '记录更新人'
) comment='拆分 topic 分割表';

-- Topic拆分信息对应表：sa_topic_split_info
drop table if exists sa_topic_split_info;
CREATE TABLE sa_topic_split_info (
        id int(11) AUTO_INCREMENT primary key,
        sink_topic varchar(100) not null comment '输出Kafka主题',
        source_field_list varchar(255) not null comment '识别字段列表'
) comment= '拆分topic信息表';