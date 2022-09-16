package cn.doitedu.rtmk.realtimemarketingmanager.controller;

import cn.doitedu.rtmk.realtimemarketingmanager.service.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.sql.SQLException;

@RestController
public class RulePublishController {

    private ProfileQueryService profileQueryService;
    private MetaDataService metaDataService;
    DynamicProfileQueryService dynamicProfileQueryService;

    @Autowired
    public RulePublishController(ProfileQueryService profileQueryService,
                                 MetaDataService metaDataService,
                                 DynamicProfileQueryService dynamicProfileQueryService) {

        this.profileQueryService = profileQueryService;
        this.metaDataService = metaDataService;
        this.dynamicProfileQueryService = dynamicProfileQueryService;
    }

    @RequestMapping("/api/rule/publish")
    public String publishRule(@RequestBody String ruleParamJson) throws Exception {

        JSONObject ruleParamJsonObject = JSON.parseObject(ruleParamJson);

        // 去 es中圈选规则参数中所限定的人群
        RoaringBitmap profileBitmap = profileQueryService.findUsersByProfileTags(ruleParamJsonObject.getJSONArray("profileCondition"));


        // 用元数据查询服务，得到本规则的动态画像历史值查询sql
        String actionCountQuerySql = metaDataService.getActionCountQuerySql(ruleParamJsonObject);


        // 利用上面得到的sql，去doris中查询动态画像条件的历史值,将查询到的历史值，发布到redis中
        dynamicProfileQueryService.initDynamicProfileActionCount(ruleParamJsonObject,actionCountQuerySql,profileBitmap);

        // 获取规则运算机的groovy代码
        // TODO
        String groovyCode = "我爱你";

        // 将形成各种原材料，插入数据库，以便于flink规则引擎去抓取
        //   1. 规则参数json
        //   2. 静态画像的人群bitmap
        //   3. 规则运算机的groovy代码
        metaDataService.addRuleResources(profileBitmap,ruleParamJsonObject,groovyCode);

        return "ok";
    }

}
