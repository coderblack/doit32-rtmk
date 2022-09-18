package cn.doitedu.rtmk.realtimemarketingmanager.controller;

import cn.doitedu.rtmk.realtimemarketingmanager.service.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;

@RestController
public class RulePublishController {

    private EsProfileQueryService esProfileQueryService;
    private MetaDataService metaDataService;
    private DynamicProfileQueryService dynamicProfileQueryService;
    private Engine engine;

    @Autowired
    public RulePublishController(EsProfileQueryService esProfileQueryService,
                                 MetaDataService metaDataService,
                                 DynamicProfileQueryService dynamicProfileQueryService) {

        this.esProfileQueryService = esProfileQueryService;
        this.metaDataService = metaDataService;
        this.dynamicProfileQueryService = dynamicProfileQueryService;
        engine = Engine.use();
    }

    /**
     * 规则模型1发布功能入口
     * @param ruleParamJson
     * @return
     * @throws Exception
     */
    @RequestMapping("/api/rule/publish1")
    public String publishRule01(@RequestBody String ruleParamJson) throws Exception {

        JSONObject ruleParamJsonObject = JSON.parseObject(ruleParamJson);

        // 去 es中圈选规则参数中所限定的人群
        RoaringBitmap profileBitmap = esProfileQueryService.findUsersByProfileTags(ruleParamJsonObject.getJSONArray("profileCondition"));


        // 用元数据查询服务，得到本规则的动态画像历史值查询sql
        String actionCountQuerySql = metaDataService.getActionCountQuerySql(ruleParamJsonObject);


        // 利用上面得到的sql，去doris中查询动态画像条件的历史值,将查询到的历史值，发布到redis中
        dynamicProfileQueryService.initDynamicProfileActionCount(ruleParamJsonObject,actionCountQuerySql,profileBitmap);

        // 获取规则运算机的groovy代码
        String codeTemplate = metaDataService.getRuleModelCalculatorCodeTemplate(ruleParamJsonObject.getInteger("ruleModelId"));
        Template template = engine.getTemplateByString(codeTemplate);

        JSONObject actionCountCondition = ruleParamJsonObject.getJSONObject("actionCountCondition");

        // 获取规则参数中的行为次数画像条件的个数
        int eventParamSize = actionCountCondition.getJSONArray("eventParams").size();
        ArrayList<Integer> eventParamList = new ArrayList<>();
        // 根据条件个数，准备一个等长的list
        for(int i=0;i<eventParamSize;i++) eventParamList.add(0);

        // 获取规则参数中的行为次数画像条件的逻辑组合表达式
        String combineExpr = actionCountCondition.getString("combineExpr");

        // 将逻辑组合表达式，和条件个数list，放入渲染数据中
        HashMap<String, Object> data = new HashMap<>();
        data.put("eventParamList",eventParamList);
        data.put("combineExpr",combineExpr);

        // 渲染模板，得到最终代码
        String realCode = template.renderToString(data);

        // 将形成各种原材料，插入数据库，以便于flink规则引擎去抓取
        //   1. 规则参数json
        //   2. 静态画像的人群bitmap
        //   3. 规则运算机的groovy代码
        metaDataService.addRuleResources(profileBitmap,ruleParamJsonObject,realCode);

        return "ok";
    }




    /**
     * 规则模型2发布功能入口
     * @param ruleParamJson
     * @return
     * @throws Exception
     */
    @RequestMapping("/api/rule/publish2")
    public String publishRule02(@RequestBody String ruleParamJson) throws Exception {

        JSONObject ruleParamJsonObject = JSON.parseObject(ruleParamJson);

        // 去 es中圈选规则参数中所限定的人群
        RoaringBitmap profileBitmap = esProfileQueryService.findUsersByProfileTags(ruleParamJsonObject.getJSONArray("profileCondition"));


        /**
         * 行为次数类动态画像条件初始值处理
         */
        // 用元数据查询服务，得到本规则的动态画像历史值查询sql
        String actionCountQuerySql = metaDataService.getActionCountQuerySql(ruleParamJsonObject);
        // 利用上面得到的sql，去doris中查询动态画像条件的历史值,将查询到的历史值，发布到redis中
        dynamicProfileQueryService.initDynamicProfileActionCount(ruleParamJsonObject,actionCountQuerySql,profileBitmap);


        /**
         * 行为序列类动态画像条件初始值处理
         */
        // 用元数据查询服务，得到本规则的行为序列动态画像历史值查询sql
        String actionSeqQuerySql = metaDataService.getActionSeqQuerySql(ruleParamJsonObject);
        // 利用上面得到的sql，去doris中查询动态画像条件的历史值,将查询到的历史值，发布到redis中
        dynamicProfileQueryService.initDynamicProfileActionSeq(ruleParamJsonObject,actionSeqQuerySql,profileBitmap);



        // 获取规则运算机的groovy代码
        String codeTemplate = metaDataService.getRuleModelCalculatorCodeTemplate(ruleParamJsonObject.getInteger("ruleModelId"));
        Template template = engine.getTemplateByString(codeTemplate);

        JSONObject actionCountCondition = ruleParamJsonObject.getJSONObject("actionCountCondition");

        // 获取规则参数中的行为次数画像条件的个数
        int eventParamSize = actionCountCondition.getJSONArray("eventParams").size();
        ArrayList<Integer> eventParamList = new ArrayList<>();
        // 根据条件个数，准备一个等长的list
        for(int i=0;i<eventParamSize;i++) eventParamList.add(0);

        // 获取规则参数中的行为次数画像条件的逻辑组合表达式
        String combineExpr = actionCountCondition.getString("combineExpr");

        // 将逻辑组合表达式，和条件个数list，放入渲染数据中
        HashMap<String, Object> data = new HashMap<>();
        data.put("eventParamList",eventParamList);
        data.put("combineExpr",combineExpr);

        // 渲染模板，得到最终代码
        String realCode = template.renderToString(data);

        // 将形成各种原材料，插入数据库，以便于flink规则引擎去抓取
        //   1. 规则参数json
        //   2. 静态画像的人群bitmap
        //   3. 规则运算机的groovy代码
        metaDataService.addRuleResources(profileBitmap,ruleParamJsonObject,realCode);

        return "ok";
    }







}
