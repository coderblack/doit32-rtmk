package cn.doitedu.rtmk.engine.caculator;

import cn.doitedu.rtmk.common.interfaces.RuleCalculator;
import cn.doitedu.rtmk.common.pojo.UserEvent;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.io.FileUtils;
import org.codehaus.groovy.tools.GroovyClass;
import org.roaringbitmap.RoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestRuleMode01Calculator {

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
        testGroovyCodeTemplate();
    }


    public static void test1() {

        RuleMode01Calculator ruleMode01Calculator = new RuleMode01Calculator();
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(2, 3);

        // 初始化运算机
        ruleMode01Calculator.init(getParamOjbect(),bitmap);

        HashMap<String, String> properties = new HashMap<>();
        properties.put("pageId","page001");
        UserEvent e5 = new UserEvent(2, "e5", properties, 1660078800000L);

        // 调用运算机处理
        List<JSONObject> resultList = ruleMode01Calculator.process(e5);
        System.out.println(resultList);

        HashMap<String, String> properties2 = new HashMap<>();
        properties2.put("pageId","page001");
        UserEvent e1 = new UserEvent(2, "e1", properties2, 1660078800000L);
        List<JSONObject> res2 = ruleMode01Calculator.process(e1);
        System.out.println(res2);


        HashMap<String, String> properties3 = new HashMap<>();
        properties3.put("pageId","page001");
        properties3.put("itemId","item002");
        UserEvent e2 = new UserEvent(2, "e2", properties3, 1660078800000L);
        List<JSONObject> res3 = ruleMode01Calculator.process(e2);
        System.out.println(res3);




    }


    public static void testGroovyCodeTemplate() throws IOException, InstantiationException, IllegalAccessException {

        Engine engine = Engine.use();

        String code = FileUtils.readFileToString(new File("D:\\dev_works\\doit32-rtmk\\realtime-marketing-resource\\groovy_enjoy_templates\\RuleMode01Calculator.enjoy"), "utf-8");
        Template template = engine.getTemplateByString(code);

        // 准备数据
        JSONObject paramOjbect = getParamOjbect();
        JSONObject actionCountCondition = paramOjbect.getJSONObject("actionCountCondition");

        JSONArray eventParams = actionCountCondition.getJSONArray("eventParams");
        ArrayList<Integer> eventParamList = new ArrayList<>(eventParams.size());
        for(int i=0;i<eventParams.size();i++) eventParamList.add(0);


        String combineExpr = actionCountCondition.getString("combineExpr");

        HashMap<String, Object> data = new HashMap<>();
        data.put("eventParamList",eventParamList);
        data.put("combineExpr",combineExpr);

        String realCode = template.renderToString(data);
        //System.out.println(realCode);

        GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
        Class aClass = groovyClassLoader.parseClass(realCode);
        RuleCalculator ruleCalculator = (RuleCalculator) aClass.newInstance();

        // 初始化运算机
        ruleCalculator.init(paramOjbect,RoaringBitmap.bitmapOf(2,3));


        HashMap<String, String> properties = new HashMap<>();
        properties.put("pageId","page001");
        UserEvent e5 = new UserEvent(2, "e5", properties, 1660078800000L);

        // 调用运算机进行事件处理
        List<JSONObject> resList = ruleCalculator.process(e5);
        System.out.println(resList);

    }






    public static JSONObject getParamOjbect(){
        String paramJson = "{\n" +
                "  \"ruleModelId\": 1,\n" +
                "  \"ruleId\": \"rule001\",\n" +
                "  \"trigger\": {\n" +
                "    \"eventId\": \"e5\",\n" +
                "    \"attributeParams\": [\n" +
                "      {\n" +
                "        \"attributeName\": \"pageId\",\n" +
                "        \"compareType\": \"=\",\n" +
                "        \"compareValue\": \"page001\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "    \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "    \"eventCount\": 1\n" +
                "  },\n" +
                "  \"//\": \"**规则画像条件参数\",\n" +
                "  \"profileCondition\": [\n" +
                "    {\n" +
                "      \"tagId\": \"tg01\",\n" +
                "      \"compareType\": \"gt\",\n" +
                "      \"compareValue\": \"2\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"tagId\": \"tg04\",\n" +
                "      \"compareType\": \"match\",\n" +
                "      \"compareValue\": \"运动\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"//\": \"**规则行为次数条件参数\",\n" +
                "  \"actionCountCondition\": {\n" +
                "    \"eventParams\": [\n" +
                "      {\n" +
                "        \"eventId\": \"e1\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page001\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "        \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "        \"eventCount\": 3,\n" +
                "        \"conditionId\": \"ac1\",\n" +
                "        \"dorisQueryTemplate\": \"action_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"eventId\": \"e2\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"itemId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"item002\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page001\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "        \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "        \"eventCount\": 1,\n" +
                "        \"conditionId\": \"ac2\",\n" +
                "        \"dorisQueryTemplate\": \"action_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"eventId\": \"e3\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page002\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "        \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "        \"eventCount\": 1,\n" +
                "        \"conditionId\": \"ac3\",\n" +
                "        \"dorisQueryTemplate\": \"action_count\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"combineExpr\": \" res0 && (res1 || res2) \"\n" +
                "  }\n" +
                "}";


        return JSON.parseObject(paramJson);

    }

}
