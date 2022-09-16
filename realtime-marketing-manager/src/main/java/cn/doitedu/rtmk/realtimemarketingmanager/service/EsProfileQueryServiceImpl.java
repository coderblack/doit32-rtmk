package cn.doitedu.rtmk.realtimemarketingmanager.service;

import cn.doitedu.rtmk.realtimemarketingmanager.dao.EsQueryDaoImpl;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class EsProfileQueryServiceImpl implements ProfileQueryService {


    private EsQueryDaoImpl esQueryDao;

    @Autowired
    public EsProfileQueryServiceImpl(EsQueryDaoImpl esQueryDao) {
        this.esQueryDao = esQueryDao;
    }


    /**
     * 根据画像条件，去es中圈选人群
     *
     * @param profileConditions
     * @return
     */
    @Override
    public RoaringBitmap findUsersByProfileTags(JSONArray profileConditions) throws IOException {

        List<Integer> resultIdList = esQueryDao.queryByTagConditions(profileConditions);

        // 解析响应结果,将结果封装为bitmap
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        for (Integer id : resultIdList) {
            bitmap.add(id);
        }
        return bitmap;
    }




    public static void main(String[] args) throws IOException {
        String testJsonArray = "[\n" +
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
                "]";
        JSONArray jsonArray = JSON.parseArray(testJsonArray);

        EsProfileQueryServiceImpl esProfileQueryService = new EsProfileQueryServiceImpl(new EsQueryDaoImpl());
        RoaringBitmap bitmap = esProfileQueryService.findUsersByProfileTags(jsonArray);
        System.out.println(bitmap.toString());

    }

}
