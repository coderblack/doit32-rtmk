package cn.doitedu.rtmk.realtimemarketingmanager.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AttributeParam{
    private String attrName;
    private String compareType;
    private String compareValue;
}