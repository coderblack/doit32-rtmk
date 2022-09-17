package cn.doitedu.rtmk.engine.pojo;

import cn.doitedu.rtmk.common.interfaces.RuleCalculator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.roaringbitmap.RoaringBitmap;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleMetaBean {
    private String operateType; // 规则管理动作
    private int ruleModelId;
    private String ruleId;
    private RoaringBitmap staticProfileBitmap;
    private String ruleParamJson;
    private String groovyCode;
    private int status;

    private RuleCalculator ruleCalculator;

}
