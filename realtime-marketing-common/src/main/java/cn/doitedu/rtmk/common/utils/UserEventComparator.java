package cn.doitedu.rtmk.common.utils;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;

import java.text.ParseException;

@Slf4j
public class UserEventComparator {

    public static boolean userEventIsEqualParam(UserEvent userEvent, JSONObject eventParam) throws ParseException {
        String eventIdParam = eventParam.getString("eventId");
        JSONArray attributeParams = eventParam.getJSONArray("attributeParams");

        // 2022-09-01 00:00:00
        String windowStart = eventParam.getString("windowStart");
        String windowEnd = eventParam.getString("windowEnd");

        long startTime = DateUtils.parseDate(windowStart, new String[]{"yyyy-MM-dd HH:mm:ss"}).getTime();
        long endTime = DateUtils.parseDate(windowEnd, new String[]{"yyyy-MM-dd HH:mm:ss"}).getTime();

        if( userEvent.getEventTime()>=startTime && userEvent.getEventTime()<=endTime &&  eventIdParam.equals(userEvent.getEventId())) {
            // 对每一个属性条件进行判断
            for (int j = 0; j < attributeParams.size(); j++) {
                // 取出一个属性参数
                JSONObject attributeParam = attributeParams.getJSONObject(j);

                String paramAttributeName = attributeParam.getString("attributeName");
                String paramCompareType = attributeParam.getString("compareType");
                String paramValue = attributeParam.getString("compareValue");

                String eventAttributeValue = userEvent.getProperties().get(paramAttributeName);

                //log.info("比较事件是否匹配条件参数,paramAttributeName:{} , paramCompareType:{} , paramValue:{},eventAttributeValue:{}",paramAttributeName,paramCompareType,paramValue,eventAttributeValue);

                if(eventAttributeValue!=null) {
                    if ("=".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue) == 0)) {
                        return false;
                    }

                    if (">".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue) > 0)) {
                        return false;
                    }

                    if ("<".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue) < 0)) {
                        return false;
                    }

                    if ("<=".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue) <= 0)) {
                        return false;
                    }

                    if (">=".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue) >= 0)) {
                        return false;
                    }
                }
            }
            return true;
        }

        return false;
    }
}
