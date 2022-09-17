package cn.doitedu.rtmk.engine.entry;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import cn.doitedu.rtmk.engine.functions.RuleProcessFunction;
import cn.doitedu.rtmk.engine.functions.StrToUserEvent;
import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import cn.doitedu.rtmk.engine.utils.DataStreamUtil;
import cn.doitedu.rtmk.engine.utils.StateDescriptors;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class EngineEntry {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 获取 用户行为事件 流
        DataStreamSource<String> eventsSourceStream = DataStreamUtil.getUserEventStream(env);

        // 将用户行为事件流，转成javabean的流
        DataStream<UserEvent> events = eventsSourceStream.map(new StrToUserEvent());

        // 将数据按用户进行分发处理
        KeyedStream<UserEvent, Integer> keyedEventsStream = events.keyBy(UserEvent::getGuid);

        // 获取 规则资源流
        DataStream<RuleMetaBean> ruleMetaDataStream = DataStreamUtil.getRuleMetaDataStream(tEnv);

        // 将规则资源流广播出去
        BroadcastStream<RuleMetaBean> ruleBroadcast = ruleMetaDataStream.broadcast(StateDescriptors.ruleMetaBroadCastStateDesc);

        // 将用户行为事件流  connect  规则资源广播流
        keyedEventsStream
                .connect(ruleBroadcast)
                .process(new RuleProcessFunction())
                .print();


        env.execute();


    }
}
