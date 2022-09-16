package cn.doitedu.rtmk.realtimemarketingmanager.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class ActionCountParam {
    private String eventId;
    private String windowStart;
    private String windowEnd;
    private List<AttributeParam> attributeParams;
}
