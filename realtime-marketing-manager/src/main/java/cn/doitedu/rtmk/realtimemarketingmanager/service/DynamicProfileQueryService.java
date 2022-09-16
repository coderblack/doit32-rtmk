package cn.doitedu.rtmk.realtimemarketingmanager.service;

import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;

import java.sql.SQLException;

public interface DynamicProfileQueryService {

    void initDynamicProfileActionCount(JSONObject ruleParamJsonObject, String querySql, RoaringBitmap staticProfileUsers) throws SQLException;
}
