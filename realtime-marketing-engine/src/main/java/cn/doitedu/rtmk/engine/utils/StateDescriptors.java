package cn.doitedu.rtmk.engine.utils;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;

public class StateDescriptors {

    public static MapStateDescriptor<String, RuleMetaBean> ruleMetaBroadCastStateDesc = new MapStateDescriptor<>("rule-meta-state", String.class, RuleMetaBean.class);


    public static ListStateDescriptor<UserEvent> userEventsStateDesc = new ListStateDescriptor<UserEvent>("events",UserEvent.class);

}
