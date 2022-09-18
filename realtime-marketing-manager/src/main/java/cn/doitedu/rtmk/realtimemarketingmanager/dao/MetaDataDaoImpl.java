package cn.doitedu.rtmk.realtimemarketingmanager.dao;

import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.stereotype.Repository;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.sql.*;

@Repository
public class MetaDataDaoImpl {
    PreparedStatement preparedStatement;
    PreparedStatement preparedStatement2;
    Connection conn;

    public MetaDataDaoImpl() throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/doit32", "root", "root");

        // 用于获取动态画像条件（事件次数条件）的sql模板的语句
        preparedStatement = conn.prepareStatement("select  sql_template  from rule_model_sql_templates where rule_model_id = ? and query_id = ?");

        // 用于插入规则资源的语句
        preparedStatement2 = conn.prepareStatement("insert into rule_engine_resource (rule_model_id,rule_id,static_bitmap,rule_param_json,groovy_code,status) values (?,?,?,?,?,?)");

    }



    public String getActionCountParamQueryTemplate(int ruleModelId,String queryId) throws SQLException {

        preparedStatement.setInt(1,ruleModelId);
        preparedStatement.setString(2,queryId);

        String sqlTempate = null;
        ResultSet resultSet = preparedStatement.executeQuery();

        while(resultSet.next()){
            sqlTempate = resultSet.getString("sql_template");
        }

        return sqlTempate;
    }

    /**
     * 插入新增规则的所有引擎需要用的资源到元数据库
     * @param staticProfileBitmap 静态画像条件圈选人群bitmap
     * @param groovyCode 规则运算机groovy代码
     */
    public void insertRuleResourceToMysql(RoaringBitmap staticProfileBitmap, JSONObject ruleParamJsonObject, String groovyCode) throws Exception {

        ByteArrayOutputStream baOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(baOut);
        // 将bitmap序列化成一个字节数组
        staticProfileBitmap.serialize(dataOut);
        byte[] bytes = baOut.toByteArray();

        // 设置插入语句中的？参数
        preparedStatement2.setInt(1,ruleParamJsonObject.getInteger("ruleModelId"));
        preparedStatement2.setString(2,ruleParamJsonObject.getString("ruleId"));
        preparedStatement2.setBytes(3,bytes);
        preparedStatement2.setString(4,ruleParamJsonObject.toJSONString());
        preparedStatement2.setString(5,groovyCode);
        preparedStatement2.setInt(6,1);

        // 执行插入语句
        preparedStatement2.execute();

    }

    public String findModelCalculatorCodeTemplateByModelId(int ruleModelId) throws SQLException {

        PreparedStatement pstmt = conn.prepareStatement("select calculator_code_template from caculator_template where rule_model_id = ?");
        pstmt.setInt(1,ruleModelId);

        ResultSet rs = pstmt.executeQuery();
        rs.next();

        return rs.getString("calculator_code_template");
    }
}
