package cn.doitedu.rtmk.realtimemarketingmanager.service;

import cn.doitedu.rtmk.realtimemarketingmanager.dao.MetaDataDaoImpl;
import cn.doitedu.rtmk.realtimemarketingmanager.pojo.ActionCountParam;
import cn.doitedu.rtmk.realtimemarketingmanager.pojo.AttributeParam;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

@Service
public class MetaDataServiceImpl implements MetaDataService {

    private MetaDataDaoImpl metaDataDao;
    private Engine engine;

    @Autowired
    public MetaDataServiceImpl(MetaDataDaoImpl metaDataDao) {
        this.metaDataDao = metaDataDao;
        this.engine = Engine.use();
    }

    @Override
    public String getActionCountQuerySql(JSONObject ruleParamJsonObject) throws SQLException {

        JSONObject actionCountCondition = ruleParamJsonObject.getJSONObject("actionCountCondition");
        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");

        // 解析规则的json参数，封装到自定义的数据bean中
        ArrayList<ActionCountParam> actionCountParamList = new ArrayList<>();
        for(int i=0;i<eventParams.size();i++){
            JSONObject eventParam = eventParams.getJSONObject(i);
            String eventId = eventParam.getString("eventId");
            String windowStart = eventParam.getString("windowStart");
            String windowEnd = eventParam.getString("windowEnd");

            JSONArray attributeParams = eventParam.getJSONArray("attributeParams");

            ArrayList<AttributeParam> attributeParamList = new ArrayList<>();
            for(int j=0;j<attributeParams.size();j++) {
                JSONObject attributeParam = attributeParams.getJSONObject(j);
                String attributeName = attributeParam.getString("attributeName");
                String compareType = attributeParam.getString("compareType");
                String compareValue = attributeParam.getString("compareValue");

                AttributeParam atp = new AttributeParam(attributeName, compareType, compareValue);
                attributeParamList.add(atp);
            }

            ActionCountParam actionCountParam = new ActionCountParam(eventId, windowStart, windowEnd, attributeParamList);
            actionCountParamList.add(actionCountParam);

        }

        // 将数据bean，放入一个hashmap中，用于渲染sql
        HashMap<String, Object> data = new HashMap<>();
        data.put("actionCountParamList",actionCountParamList);


        // 取查询sql模板
        String sqlTemplateStr = metaDataDao.getActionCountParamQueryTemplate(ruleParamJsonObject.getInteger("ruleModelId"), "action_cnt");
        Template template = engine.getTemplateByString(sqlTemplateStr);
        String sql = template.renderToString(data);

        return sql;
    }


    @Override
    public void addRuleResources(RoaringBitmap staticProfileBitmap, JSONObject ruleParamJsonObject, String groovyCode) throws Exception {
        metaDataDao.insertRuleResourceToMysql(staticProfileBitmap,ruleParamJsonObject,groovyCode);
    }



}
