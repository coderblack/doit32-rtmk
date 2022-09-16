package cn.doitedu.rtmk.realtimemarketingmanager.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.Jedis;

import java.sql.*;

@Repository
public class DynamicProfileInitDaoImpl {
    Connection dorisConn;
    Jedis jedis;

    public DynamicProfileInitDaoImpl() throws SQLException {

        // 创建doris的jdbc连接
        dorisConn = DriverManager.getConnection("jdbc:mysql://doitedu:9030/test", "root", "");

        // 创建redis连接
        jedis = new Jedis("doitedu", 6379);

    }


    /**
     * 根据sql，去doris中查询行为次数类动态画像初始值，并发布到的redis去
     * @param ruleParamJsonObject
     * @param querySql
     */
    public void initActionCountProfile(JSONObject ruleParamJsonObject, String querySql, RoaringBitmap staticProfileUsers) throws SQLException {
        // 从规则参数中获取 规则id
        String ruleId = ruleParamJsonObject.getString("ruleId");

        // 从规则参数中，获取动态画像条件（事件次数条件）
        JSONObject actionCountCondition = ruleParamJsonObject.getJSONObject("actionCountCondition");
        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");

        // 执行查询sql
        Statement statement = dorisConn.createStatement();
        ResultSet resultSet = statement.executeQuery(querySql);

        /**
         * SELECT
         *   guid,count(if(event_id="addcart",1,null)) as cnt1 ,count(if(event_id="submitorder",1,null)) as cnt2
         * from app_events
         */
        // 遍历结果，并将得到的结果发布到redis
        while(resultSet.next()){
            int guid = resultSet.getInt("guid");

            // 只有存在于静态画像条件人群中的人，才去发布动态画像的条件初始值
            // 如果要做的效率更高的话，应该在查询的sql模板中添加 where guid in []
            if(staticProfileUsers.contains(guid)) {
                for (int i = 0; i < eventParams.size(); i++) {
                    long cnt = resultSet.getLong(i + 2);
                    if(cnt>0) {
                        String conditionId = eventParams.getJSONObject(i).getString("conditionId");
                        // 将查询到的结果，放入redis的 hash结构：  规则id:条件id  ==>  用户id -> 次数
                        jedis.hset(ruleId + ":" + conditionId, guid + "", cnt + "");
                    }
                }
            }
        }
    }
}
