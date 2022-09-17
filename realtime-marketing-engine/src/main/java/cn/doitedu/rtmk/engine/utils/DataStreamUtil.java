package cn.doitedu.rtmk.engine.utils;

import cn.doitedu.rtmk.engine.functions.RowToRuleMetaBean;
import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DataStreamUtil {

    public static  DataStreamSource<String> getUserEventStream(StreamExecutionEnvironment env){

        // 从kafka中消费用户的实时的行为事件
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setGroupId("doe-rtmk")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("rtmk-events")
                .build();

        // {"guid":1,"eventId":"e1","properties":{"p1":"v1","p2":"2"},"eventTime":100000}
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk");
    }


    public static DataStream<RuleMetaBean> getRuleMetaDataStream(StreamTableEnvironment tEnv ){

        tEnv.executeSql("CREATE TABLE rule_meta_cdc (    " +
                " `id` int   PRIMARY KEY NOT ENFORCED,      " +
                " `rule_model_id` int   ,                   " +
                " `rule_id` string  ,                       " +
                " `static_bitmap` binary  ,                 " +
                " `rule_param_json` string,                 " +
                " `groovy_code` string,                     " +
                " `status` int                              " +
                "  ) WITH (                                 " +
                "     'connector' = 'mysql-cdc',            " +
                "     'hostname' = 'doitedu'   ,            " +
                "     'port' = '3306'          ,            " +
                "     'username' = 'root'      ,            " +
                "     'password' = 'root'      ,            " +
                "     'database-name' = 'doit32',           " +
                "     'table-name' = 'rule_engine_resource' " +
                ")");

        Table table = tEnv.from("rule_meta_cdc");
        DataStream<Row> rowDataStream = tEnv.toChangelogStream(table);

        return rowDataStream.map(new RowToRuleMetaBean());
    }

}
