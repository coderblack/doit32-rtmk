package cn.doitedu.rtmk.realtimemarketingmanager.service;

import cn.doitedu.rtmk.realtimemarketingmanager.dao.DynamicProfileInitDaoImpl;
import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

@Service
public class DynamicProfileQueryServiceImpl implements DynamicProfileQueryService {

    DynamicProfileInitDaoImpl dynamicProfileInitDao;

    @Autowired
    public DynamicProfileQueryServiceImpl(DynamicProfileInitDaoImpl dynamicProfileInitDao){
        this.dynamicProfileInitDao = dynamicProfileInitDao;
    }

    @Override
    public void initDynamicProfileActionCount(JSONObject ruleParamJsonObject, String querySql, RoaringBitmap staticProfileUsers) throws SQLException {

        dynamicProfileInitDao.initActionCountProfile(ruleParamJsonObject,querySql,staticProfileUsers);
    }

}
