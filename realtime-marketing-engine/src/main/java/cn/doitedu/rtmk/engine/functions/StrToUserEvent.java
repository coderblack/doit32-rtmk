package cn.doitedu.rtmk.engine.functions;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;

public class StrToUserEvent implements MapFunction<String, UserEvent> {

    @Override
    public UserEvent map(String value) throws Exception {
        return JSON.parseObject(value,UserEvent.class);
    }
}
