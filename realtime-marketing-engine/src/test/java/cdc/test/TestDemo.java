package cdc.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // 建表，映射mysql的元数据表的binlog

        // 用cdc抓取规则系统的元数据库中的规则定义变更数据（新增规则，停用规则，删除规则）
        /**
         * CREATE TABLE `rule_engine_resource` (
         *   `id` int(11) NOT NULL AUTO_INCREMENT,
         *   `rule_model_id` int(11) DEFAULT NULL,
         *   `rule_id` varchar(255) DEFAULT NULL,
         *   `static_bitmap` longblob,
         *   `rule_param_json` text,
         *   `groovy_code` longtext,
         *   `status` int(11) DEFAULT NULL,
         *   PRIMARY KEY (`id`)
         * ) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
         */
        tEnv.executeSql("CREATE TABLE rule_meta_cdc (    " +
                " `id` int   PRIMARY KEY NOT ENFORCED,    " +
                " `rule_model_id` int   ,                 " +
                " `rule_id` string  ,                     " +
                " `static_bitmap` binary  ,               " +
                " `rule_param_json` string,               " +
                " `groovy_code` string,                   " +
                " `status` int                            " +
                "  ) WITH (                                        " +
                "     'connector' = 'mysql-cdc',                   " +
                "     'hostname' = 'doitedu'   ,                   " +
                "     'port' = '3306'          ,                   " +
                "     'username' = 'root'      ,                   " +
                "     'password' = 'root'      ,                   " +
                "     'database-name' = 'doit32',                  " +
                "     'table-name' = 'rule_engine_resource'        " +
                ")");

        tEnv.executeSql("select * from rule_meta_cdc").print();

    }
}
