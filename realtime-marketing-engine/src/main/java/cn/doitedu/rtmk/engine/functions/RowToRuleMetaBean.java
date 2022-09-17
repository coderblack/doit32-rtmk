package cn.doitedu.rtmk.engine.functions;

import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;

public class RowToRuleMetaBean implements MapFunction<Row,RuleMetaBean> {
    @Override
    public RuleMetaBean map(Row row) throws Exception {

        String operateType = "I";

        RowKind kind = row.getKind();
        if(kind == RowKind.DELETE) operateType="D";


        int id = row.getFieldAs("id");
        int rule_model_id = row.getFieldAs("rule_model_id");
        String rule_id = row.getFieldAs("rule_id");

        String rule_param_json = row.getFieldAs("rule_param_json");
        String groovy_code = row.getFieldAs("groovy_code");
        int status = row.getFieldAs("status");

        // 取出元数据中的bitmap字节，进行反序列化
        byte[] static_bitmap = row.getFieldAs("static_bitmap");
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        bitmap.deserialize(ByteBuffer.wrap(static_bitmap));

        return new RuleMetaBean(operateType,rule_model_id,rule_id,bitmap,rule_param_json,groovy_code,status,null);
    }
}
