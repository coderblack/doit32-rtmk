package enjoy.test;

import com.jfinal.template.Engine;
import com.jfinal.template.Template;

import java.util.Arrays;
import java.util.HashMap;

public class Demo {
    public static void main(String[] args) {

        String tempStr1 = "hello  \n" +
                "#if(name!=null) \n" +
                "你好: #(name)  \n" +
                "#end";

        // 构造一个模板引擎，加载模板字符串
        Engine engine = Engine.use();
        Template template1 = engine.getTemplateByString(tempStr1);

        HashMap<String, Object> data = new HashMap<>();
        data.put("name","张三");

        // 对模板对象，按照给定的数据，渲染出最终字符串
        String s = template1.renderToString(data);
        //System.out.println(s);

        // ---------------------------
        String tempStr2 = "hello  \n" +
                "#for(name:nameList)\n" +
                "你好: #(name)\n" +
                "#end";
        Template template2 = engine.getTemplateByString(tempStr2);

        data.put("nameList", Arrays.asList("zz","cc","bb","dd"));
        String s2 = template2.renderToString(data);

        System.out.println(s2);

    }
}
