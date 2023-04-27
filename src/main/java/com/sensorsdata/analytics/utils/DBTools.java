package com.sensorsdata.analytics.utils;

import com.sensorsdata.analytics.bean.mysql.CounterBean;
import com.sensorsdata.analytics.bean.mysql.PropertyMappingBean;
import com.sensorsdata.analytics.bean.mysql.SinkProjectBean;
import com.sensorsdata.analytics.bean.mysql.SourceSinkBean;
import com.sensorsdata.analytics.constants.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author igarashi233
 **/
@Slf4j
public class DBTools {

    DBTools() {
    }

    public static List<SinkProjectBean> querySinkProjectMapping(String querySql, ParameterTool parameters) {
        List<SinkProjectBean> result = Lists.newArrayList();
        try (Connection connection = getConnection(parameters.toMap())) {
            QueryRunner runner = new QueryRunner();
            result = runner.query(connection, querySql, new BeanListHandler<>(SinkProjectBean.class));
            log.info("querySinkProjectMapping: {}", result.size());
        } catch (Exception e) {
            log.error("querySinkProjectMapping error", e);
        }
        return result;
    }

    public static List<CounterBean> queryCounterBeanList(String querySql, ParameterTool parameters) {
        List<CounterBean> result = Lists.newArrayList();
        try (Connection connection = getConnection(parameters.toMap())) {
            QueryRunner runner = new QueryRunner();
            result = runner.query(connection, querySql, new BeanListHandler<>(CounterBean.class));
            log.info("queryCounterBeanList: {}", result.size());
        } catch (Exception e) {
            log.error("queryCounterBeanList error", e);
        }
        return result;
    }

    public static List<SourceSinkBean> queryProjectTopicMapping(String querySql, ParameterTool parameters) {
        List<SourceSinkBean> result = Lists.newArrayList();
        try (Connection connection = getConnection(parameters.toMap())) {
            QueryRunner runner = new QueryRunner();
            result = runner.query(connection, querySql, new BeanListHandler<>(SourceSinkBean.class));
            log.info("queryProjectTopicMapping: {}", result.size());
        } catch (Exception e) {
            log.error("queryProjectTopicMapping error", e);
        }
        return result;
    }

    /**
     * 查询属性映射表，查询出全量数据
     *
     * @param querySql - 查询所使用的 SQL
     * @return - 所有数据的 List 集合
     */
    public static List<PropertyMappingBean> queryPropertyMapping(String querySql, ParameterTool parameters) {
        List<PropertyMappingBean> result = Lists.newArrayList();
        try (Connection connection = getConnection(parameters.toMap())) {
            QueryRunner runner = new QueryRunner();
            result = runner.query(connection, querySql, new BeanListHandler<>(PropertyMappingBean.class));
            log.info("queryPropertyMappingList:" + result.size());
        } catch (Exception e) {
            log.error("queryPropertyMappingList error", e);
        }
        return result;
    }

    private static Connection getConnection(Map<String, String> parameterMap) {
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String pwd = parameterMap.get(PropertiesConstants.MYSQL_PASSWORD);
            if (StringUtils.isBlank(pwd)) {
                log.error("get mysql password failed. parameters: {}", parameterMap);
            }
            con = DriverManager.getConnection(parameterMap.get(PropertiesConstants.MYSQL_JDBC_URL),
                    parameterMap.get(PropertiesConstants.MYSQL_USERNAME), pwd);
        } catch (Exception e) {
            log.error("mysql get connection has error .[ detail = {} , stacktrace = {} ]",
                    e.getMessage(), e.getStackTrace());
        }
        return con;
    }

    /**
     * #记录kafak偏移量的表
     * create table ua_project_kafka_offset(
     * `date``insert_time` datetime not null comment '日期记录插入时间', #yyyy-MM-dd HH:mm:ss
     * `topicname` varchar(256) not null comment '表名称',
     * `partitionid` int default -1 comment '分区号',
     * `offset` bigint default -1 comment '分区偏移量',
     * `group` varchar(256) not null comment '组名',
     * `jobid` varchar(256) not null comment 'jobid'
     * primary key (`jobid``insert_time`,`topicname`,)
     * ) comment='kafka偏移量信息';
     *
     * @param parameters
     */

    public static void batchInsertCheckpointInfo(ParameterTool parameters, Object[][] values) {
        //批量插入
        String sql = parameters.get("insert.checkpoint");
        try (Connection connection = getConnection(parameters.toMap())) {
            QueryRunner queryRunner = new QueryRunner();
            queryRunner.batch(connection, sql, values);
        } catch (Exception e) {
            log.error("insertCheckpointInfo error", e);
        }
        log.info("[C:DBTools] - [M:batchInsertCheckpointInfo] sql is {},-values arrays is {} .", sql, values);
    }

    public static void batchInsertCounterInfo(ParameterTool parameters, Object[][] values) {
        //批量插入
        String sql = parameters.get("insert.counter");
        try (Connection connection = getConnection(parameters.toMap())) {
            QueryRunner queryRunner = new QueryRunner();
            queryRunner.batch(connection, sql, values);
        } catch (Exception e) {
            log.error("insertCounterInfo error", e);
        }
        log.info("[C:DBTools] - [M:batchInsertCounterInfo] sql is {},-values arrays is {} .", sql, values);
    }

    public static void batchUpdateCounterInfo(ParameterTool parameters, Object[][] values) {
        //批量插入
        String sql = parameters.get("update.counter");
        try (Connection connection = getConnection(parameters.toMap())) {
            QueryRunner queryRunner = new QueryRunner();
            queryRunner.batch(connection, sql, values);
        } catch (Exception e) {
            log.error("updateCounterInfo error", e);
        }
        log.info("[C:DBTools] - [M:batchUpdateCounterInfo] sql is {},-values arrays is {} .", sql, values);
    }

    /**
     * 查询checkpoint表，获取最近一次checkpoint信息
     *
     * @param
     * @return
     */
    public static List<Map<String, Object>> getLatestCheckpointInfo(ParameterTool parameters,
                                                                    String topicName,
                                                                    Tuple3<String, String, String> kafkaInfo) {
        List<Map<String, Object>> partitionOffsetMapList = new ArrayList<>();
        String querySql = parameters.get("query.latest.checkpoint");
        try (Connection connection = getConnection(parameters.toMap())) {
            QueryRunner runner = new QueryRunner();
            partitionOffsetMapList =
                    runner.query(connection, querySql, new MapListHandler(), topicName, kafkaInfo.f1, kafkaInfo.f0);
        } catch (Exception e) {
            log.error("queryPropertyMappingList error", e);
        }
        log.info("[C:DBTools] - [M:getLatestCheckpointInfo] sql is {} and parameters  is {{}:{}} result is {}",
                querySql, topicName, kafkaInfo, partitionOffsetMapList);
        return partitionOffsetMapList;
    }
}
