package cn.doitedu.rtmk.engine.functions;

import cn.doitedu.rtmk.common.interfaces.RuleCalculator;
import cn.doitedu.rtmk.common.pojo.UserEvent;
import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import cn.doitedu.rtmk.engine.utils.StateDescriptors;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import groovy.lang.GroovyClassLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

@Slf4j
public class RuleProcessFunction extends KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject> {

    GroovyClassLoader groovyClassLoader;

    @Override
    public void open(Configuration parameters) throws Exception {
        groovyClassLoader = new GroovyClassLoader();
    }

    @Override
    public void processElement(UserEvent userEvent, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        log.info("收到一条用户行为,事件id:{},pageId:{}",userEvent.getEventId(),userEvent.getProperties().get("pageId"));
        ReadOnlyBroadcastState<String, RuleMetaBean> broadcastState = ctx.getBroadcastState(StateDescriptors.ruleMetaBroadCastStateDesc);

        for (Map.Entry<String, RuleMetaBean> immutableEntry : broadcastState.immutableEntries()) {
            log.info("遍历规则对事件进行处理,规则id:{}",immutableEntry.getValue().getRuleId());

            RuleMetaBean ruleMetaBean = immutableEntry.getValue();
            RuleCalculator ruleCalculator = ruleMetaBean.getRuleCalculator();

            List<JSONObject> resultList = ruleCalculator.process(userEvent);
            if(resultList != null){
                for (JSONObject resJsonObject : resultList) {
                    out.collect(resJsonObject);
                }
            }
        }

    }



    @Override
    public void processBroadcastElement(RuleMetaBean ruleMetaBean, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        log.info("收到一条规则信息，规则id:{},操作类型:{}",ruleMetaBean.getRuleId(),ruleMetaBean.getOperateType());
        BroadcastState<String, RuleMetaBean> broadcastState = ctx.getBroadcastState(StateDescriptors.ruleMetaBroadCastStateDesc);

        if(ruleMetaBean.getOperateType().equals("I")) {
            // 取出规则资源中的运算机groovy代码
            String groovyCode = ruleMetaBean.getGroovyCode();
            log.info("规则的静态画像人群:{}",ruleMetaBean.getStaticProfileBitmap().toString());
            log.info("规则的运算机代码如下：{}" ,groovyCode);

            // 将规则运算机代码进行编译、加载、反射
            Class aClass = groovyClassLoader.parseClass(groovyCode);
            RuleCalculator ruleCalculator = (RuleCalculator) aClass.newInstance();
            // 将运算机进行初始化
            ruleCalculator.init(JSON.parseObject(ruleMetaBean.getRuleParamJson()),ruleMetaBean.getStaticProfileBitmap());

            ruleMetaBean.setRuleCalculator(ruleCalculator);

            // 然后将 ruleMetaBean对象，放入广播状态，以提供给上面的事件处理方法来使用
            broadcastState.put(ruleMetaBean.getRuleId(), ruleMetaBean);
        }else{
            broadcastState.remove(ruleMetaBean.getRuleId());
        }

    }
}
