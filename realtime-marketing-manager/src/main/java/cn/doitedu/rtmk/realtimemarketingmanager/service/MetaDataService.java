package cn.doitedu.rtmk.realtimemarketingmanager.service;

import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;

import java.sql.SQLException;

public interface MetaDataService {
    String getActionCountQuerySql(JSONObject ruleParamJsonObject) throws SQLException;

    String getRuleModelCalculatorCodeTemplate(int ruleModelId) throws SQLException;

    void addRuleResources(RoaringBitmap staticProfileBitmap, JSONObject ruleParamJsonObject, String groovyCode) throws Exception;
}
