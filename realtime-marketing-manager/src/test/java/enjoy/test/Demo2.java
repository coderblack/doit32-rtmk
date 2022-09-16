package enjoy.test;

import cn.doitedu.rtmk.realtimemarketingmanager.pojo.ActionCountParam;
import cn.doitedu.rtmk.realtimemarketingmanager.pojo.AttributeParam;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;

import java.util.Arrays;
import java.util.HashMap;

public class Demo2 {

    public static void main(String[] args) {
        Engine engine = Engine.use();

        String sqlStr = "SELECT\n" +
                "  guid,\n" +
                "  \n" +
                "  #for(actionCountParam:actionCountParamList)\n" +
                "  count(if(event_id=\"#(actionCountParam.eventId)\",1,null)) as cnt#(for.count) #if(! for.last) , #end\n" +
                "  #end\n" +
                "  \n" +
                "from app_events\n" +
                "where \n" +
                "\n" +
                "#for(actionCountParam:actionCountParamList)\n" +
                "#if(! for.first) or  #end   (event_id=\"#(actionCountParam.eventId)\" \n" +
                "  #for(attributeParam:actionCountParam.attributeParams) \n" +
                "  and  get_json_string(properties,\"$.#(attributeParam.attrName)\") #(attributeParam.compareType) \"#(attributeParam.compareValue)\" \n" +
                "  #end\n" +
                "  and from_unixtime(ts/1000) between \"#(actionCountParam.windowStart)\" and \"#(actionCountParam.windowEnd)\"\n" +
                ")\n" +
                "#end\n" +
                "\n" +
                "group by guid\n" +
                ";";


        Template template = engine.getTemplateByString(sqlStr);

        // 准备数据
        AttributeParam p1 = new AttributeParam("pageId", "=", "pg001");
        AttributeParam p2 = new AttributeParam("itemId", "=", "item001");
        ActionCountParam ac1 = new ActionCountParam("addcart", "2022-09-01 00:00:00", "2022-09-30 00:00:00", Arrays.asList(p1, p2));


        AttributeParam p3 = new AttributeParam("pageId", "=", "pg003");
        ActionCountParam ac2 = new ActionCountParam("submitorder", "2022-09-11 00:00:00", "2022-09-20 00:00:00", Arrays.asList(p3));

        HashMap<String, Object> data = new HashMap<>();
        data.put("actionCountParamList",Arrays.asList(ac1,ac2));

        String sql = template.renderToString(data);

        System.out.println(sql);


    }


}
