package cn.doitedu.rtmk.engine.caculator

import cn.doitedu.rtmk.common.interfaces.RuleCalculator
import cn.doitedu.rtmk.common.pojo.UserEvent
import cn.doitedu.rtmk.common.utils.UserEventComparator
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import groovy.util.logging.Slf4j
import org.roaringbitmap.RoaringBitmap
import redis.clients.jedis.Jedis

@Slf4j
class RuleMode01Calculator implements RuleCalculator {
    RoaringBitmap staticProfileUserBitmap;
    JSONObject ruleDefineParamJsonObject;
    Jedis jedis;


    @Override
    void init(JSONObject ruleDefineParamJsonObject, RoaringBitmap staticProfileUserBitmap) {
        this.ruleDefineParamJsonObject = ruleDefineParamJsonObject
        this.staticProfileUserBitmap = staticProfileUserBitmap
        this.jedis = new Jedis("doitedu", 6379)
    }

    /**
     * 规则运算机的功能入口方法
     * @param userEvent 输入的用户行为事件
     * @return
     */
    @Override
    List<JSONObject> process(UserEvent userEvent) {
        println("判断用户是否在画像人群中")
        if (staticProfileUserBitmap.contains(userEvent.getGuid())) {
            println("用户在画像人群中")
            // 从规则参数中取出触发事件条件
            JSONObject triggerParamObject = ruleDefineParamJsonObject.getJSONObject("trigger")

            // 判断用户行为是否是触发事件，如果是则调用规则匹配逻辑进行匹配判断
            if (UserEventComparator.userEventIsEqualParam(userEvent, triggerParamObject)) {
                boolean isMatch = isMatch(userEvent.getGuid())

                if (isMatch) {
                    JSONObject resultJsonObject = new JSONObject()
                    resultJsonObject.put("guid", userEvent.getGuid())
                    resultJsonObject.put("matchTime", userEvent.getEventTime())
                    resultJsonObject.put("matchRule", ruleDefineParamJsonObject.getString("ruleId"))

                    return Collections.singletonList(resultJsonObject)
                }
            }
            // 如果不是触发事件，则调用运算逻辑进行动态画像运算
            else {
                println("事件不是触发事件，准备做动态计算")
                calc(userEvent)
            }
        }

        return null
    }

    @Override
    void calc(UserEvent userEvent) {

        println("进入计算方法")
        String ruleId = ruleDefineParamJsonObject.getString("ruleId")

        JSONObject actionCountParam = ruleDefineParamJsonObject.getJSONObject("actionCountCondition")
        JSONArray eventParams = actionCountParam.getJSONArray("eventParams")

        // 判断 用户的行为事件，是否满足某条件参数
        for (int i = 0; i < eventParams.size(); i++) {
            JSONObject eventParam = eventParams.getJSONObject(i)
            println("遍历到事件条件:" + eventParam.getString("eventId"))

            String conditionId = eventParam.getString("conditionId")
            // 如果用户行为与参数条件吻合
            println("准备判断用户事件,与规则参数是否吻合")
            if (UserEventComparator.userEventIsEqualParam(userEvent, eventParam)) {
                println("判断结果为吻合")
                // 则去更新redis中的聚合状态
                jedis.hincrBy(ruleId + ":" + conditionId, userEvent.getGuid() + "", 1)
            }
        }
    }

    @Override
    boolean isMatch(int guid) {
        String ruleId = ruleDefineParamJsonObject.getString("ruleId")

        JSONObject actionCountParam = ruleDefineParamJsonObject.getJSONObject("actionCountCondition")
        JSONArray eventParams = actionCountParam.getJSONArray("eventParams")

        // 遍历每一个事件次数画像条件，看用户的实际值是否满足阈值
        JSONObject eventParam0 = eventParams.getJSONObject(0)
        String conditionId0 = eventParam0.getString("conditionId")
        // 获取条件参数所要求的的阈值
        int paramValue0 = eventParam0.getInteger("eventCount")
        // 去redis中获取该条件的实际值
        String realValueStr0 = jedis.hget(ruleId + ":" + conditionId0, guid + "")
        int realValue0 = realValueStr0==null ? 0 : Integer.parseInt(realValueStr0)
        boolean res0 = realValue0 >= paramValue0

        JSONObject eventParam1 = eventParams.getJSONObject(1)
        String conditionId1 = eventParam1.getString("conditionId")
        // 获取条件参数所要求的的阈值
        int paramValue1 = eventParam1.getInteger("eventCount")
        // 去redis中获取该条件的实际值
        String realValueStr1 = jedis.hget(ruleId + ":" + conditionId1, guid + "")
        int realValue1 = realValueStr1==null ? 0 : Integer.parseInt(realValueStr1)
        boolean res1 = realValue1 >= paramValue1

        JSONObject eventParam2 = eventParams.getJSONObject(2)
        String conditionId2 = eventParam2.getString("conditionId")
        // 获取条件参数所要求的的阈值
        int paramValue2 = eventParam2.getInteger("eventCount")
        // 去redis中获取该条件的实际值
        String realValueStr2 = jedis.hget(ruleId + ":" + conditionId2, guid + "")
        int realValue2 = realValueStr2==null ? 0 : Integer.parseInt(realValueStr2)
        boolean res2 = realValue2 >= paramValue2

        return res0 && ( res1  || res2 )
    }
}
