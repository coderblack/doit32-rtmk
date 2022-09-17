package cn.doitedu.rtmk.realtimemarketingmanager.service;

import com.alibaba.fastjson.JSONArray;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public interface EsProfileQueryService {

    RoaringBitmap findUsersByProfileTags(JSONArray profileConditions) throws IOException;
}
