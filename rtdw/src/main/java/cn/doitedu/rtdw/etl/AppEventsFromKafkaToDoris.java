package cn.doitedu.rtdw.etl;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;

public class AppEventsFromKafkaToDoris {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 读kafka的app行为日志
        // 建表，映射kafka中的日志数据所在topic
        tenv.executeSql(
                " CREATE TABLE kafka_app_events (                      "+
                        "   `guid` BIGINT,                                     "+
                        "   `event_id` STRING,                                 "+
                        "   `properties` MAP<STRING,STRING>,                   "+
                        "   `ts` BIGINT                                        "+
                        " ) WITH (                                             "+
                        "   'connector' = 'kafka',                             "+
                        "   'topic' = 'app-events',                            "+
                        "   'properties.bootstrap.servers' = 'doitedu:9092',   "+
                        "   'properties.group.id' = 'testGroup',               "+
                        "   'scan.startup.mode' = 'earliest-offset',           "+
                        "   'format' = 'json'  ,                               "+
                        "   'json.ignore-parse-errors' = 'true'               )" );

        tenv.executeSql(
                " CREATE TABLE flink_doris_sink (               "+
                        "     `guid` BIGINT,                             "+
                        "     `event_id` STRING,                         "+
                        "     `properties` STRING,                       "+
                        "     `ts` BIGINT    ,                           "+
                        "      dt  STRING	                             "+
                        " )                                              "+
                        " WITH (                                         "+
                        "       'connector' = 'doris',                   "+
                        "       'fenodes' = 'doitedu:8030',              "+
                        "       'table.identifier' = 'test.app_events',  "+
                        "       'username' = 'root',                     "+
                        "       'password' = '',                         "+
                        "       'sink.label-prefix' = 'doris_label'      "+
                        " )                                              "
        );

        tenv.createTemporaryFunction("map2json",Map2Json.class);

        tenv.executeSql("insert into flink_doris_sink select guid,event_id,map2json(properties) as properties,ts, DATE_FORMAT(TO_TIMESTAMP_LTZ(ts,3) ,'yyyy-MM-dd') as dt from kafka_app_events");

    }

    public static class Map2Json extends ScalarFunction {

        public String eval(Map<String,String> properties){
            return JSON.toJSONString(properties);
        }
    }


}
