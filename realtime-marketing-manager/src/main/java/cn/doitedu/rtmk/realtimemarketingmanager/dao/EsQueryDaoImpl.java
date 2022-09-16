package cn.doitedu.rtmk.realtimemarketingmanager.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Repository
public class EsQueryDaoImpl {

    RestHighLevelClient client;
    SearchRequest request;

    public EsQueryDaoImpl(){

        // es的请求客户端
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("doitedu", 9200, "http")));
        // 用于查询参数封装的对象
        request = new SearchRequest("doeusers");

    }

    /**
     *  [
     *     {
     *       "tagId": "tg01",
     *       "compareType": "eq",
     *       "compareValue": "3"
     *     },
     *     {
     *       "tagId": "tg04",
     *       "compareType": "match",
     *       "compareValue": "运动"
     *     }
     *   ]
     * @param tagConditions
     * @return
     */
    public List<Integer> queryByTagConditions(JSONArray tagConditions) throws IOException {

        ArrayList<Integer> resultList = new ArrayList<>();

        // 用于组合多个具体条件用
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        // 开始遍历json参数中的每一个画像条件，并转换为es的查询条件，并放入es的组合条件 boolQueryBuilder中
        for(int i=0;i< tagConditions.size();i++){

            // 取出一个画像条件参数对象
            JSONObject tagConditionJsonObject = tagConditions.getJSONObject(i);
            // 从参数对象中，解析出 ： 标签名，运算符，比较值
            String tagId = tagConditionJsonObject.getString("tagId");
            String compareType = tagConditionJsonObject.getString("compareType"); // eq / lt / gt / lte / gte / nq / match / ....
            String compareValue = tagConditionJsonObject.getString("compareValue");  // 5  |  湖南  |  10.65

            // 将参数，封装到es的查询参数对象中
            switch (compareType){
                case "lt":
                    RangeQueryBuilder lt = QueryBuilders.rangeQuery(tagId).lt(compareValue);
                    if(compareValue.matches("\\d+(.\\d+)?")){
                        lt = QueryBuilders.rangeQuery(tagId).lt(Float.parseFloat(compareValue));
                    }
                    boolQueryBuilder.must(lt);
                    break;
                case "gt":
                    RangeQueryBuilder gt = QueryBuilders.rangeQuery(tagId).gt(compareValue);
                    if(compareValue.matches("\\d+(.\\d+)?")){
                        gt = QueryBuilders.rangeQuery(tagId).gt(Float.parseFloat(compareValue));
                    }
                    boolQueryBuilder.must(gt);
                    break;
                case "ge":
                    RangeQueryBuilder gte = QueryBuilders.rangeQuery(tagId).gte(compareValue);
                    if(compareValue.matches("\\d+(.\\d+)?")){
                        gte = QueryBuilders.rangeQuery(tagId).gte(Float.parseFloat(compareValue));
                    }
                    boolQueryBuilder.must(gte);
                    break;
                case "le":
                    RangeQueryBuilder lte = QueryBuilders.rangeQuery(tagId).lte(compareValue);
                    if(compareValue.matches("\\d+(.\\d+)?")){
                        lte = QueryBuilders.rangeQuery(tagId).lte(Float.parseFloat(compareValue));
                    }
                    boolQueryBuilder.must(lte);
                    break;
                case "between":
                    String[] fromTo = compareValue.split(",");
                    RangeQueryBuilder btw = QueryBuilders.rangeQuery(tagId).from(fromTo[0],true).to(fromTo[1],true);
                    if(fromTo[0].matches("\\d+(.\\d+)?") && fromTo[1].matches("\\d+(.\\d+)?")){
                        btw = QueryBuilders.rangeQuery(tagId).from(Float.parseFloat(fromTo[0]),true).to(Float.parseFloat(fromTo[1]),true);
                    }
                    boolQueryBuilder.must(btw);
                    break;
                default:
                    MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(tagId, compareValue);
                    boolQueryBuilder.must(matchQuery);
            }



        }


        // 将封装好的查询条件builder，变成request
        request.source(new SearchSourceBuilder().query(boolQueryBuilder));

        // 用es客户端，向es发出查询请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        // System.out.println("耗时：" + response2.getTook());
        // System.out.println("命中条数：" + hits.getTotalHits());

        // 遍历搜索到的每一个结果,并添加到bitmap
        for (SearchHit hit : hits) {
            resultList.add(Integer.parseInt(hit.getId()));
        }

        return resultList;
    }

    /**
     * 测试用的
     * @param args
     * @throws IOException
     */
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
                "      \"compareValue\": \"咖啡\"\n" +
                "    }\n" +
                "]";

        EsQueryDaoImpl esQueryDao = new EsQueryDaoImpl();

        JSONArray jsonArray = JSON.parseArray(testJsonArray);
        List<Integer> integers = esQueryDao.queryByTagConditions(jsonArray);

        System.out.println(integers);

    }


}
