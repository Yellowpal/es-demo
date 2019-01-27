package win.yellowpal.esdemo.config;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

@Controller
@RequestMapping("/api")
public class ApiController {

    @Autowired
    TransportClient client;

    @PostMapping("/insert")
    @ResponseBody
    public ResponseEntity insert(
            @RequestParam("title") String title,
            @RequestParam("author") String author,
            @RequestParam("desc") String desc,
            @RequestParam("publish_date")
                @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date publishDate
            ){

        try {
            XContentBuilder content = XContentFactory.jsonBuilder().startObject()
                    .field("title",title)
                    .field("author",author)
                    .field("desc",desc)
                    .field("publish_date",publishDate.getTime())
                    .endObject();

            IndexResponse response = client.prepareIndex("test", "_doc")
                    .setSource(content)
                    .get();

            return new ResponseEntity(response.getId(),HttpStatus.OK);
        } catch (IOException e){
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    @GetMapping("/get")
    @ResponseBody
    public ResponseEntity get(@RequestParam("id") String id){

        GetResponse response = client.prepareGet("article", "cms", id).get();
        if(response.isSourceEmpty()){
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }else{
            return new ResponseEntity(response.getSource(),HttpStatus.OK);
        }
    }

    @DeleteMapping("/delete")
    @ResponseBody
    public ResponseEntity delete(@RequestParam("id") String id){

        DeleteResponse response = client.prepareDelete("article", "cms", id).get();

        return new ResponseEntity(response.getResult(),HttpStatus.OK);
    }

    @PostMapping("/update")
    @ResponseBody
    public ResponseEntity update(
            @RequestParam("id") String id,
            @RequestParam(value = "title",required = false) String title,
            @RequestParam(value = "author", required = false) String author,
            @RequestParam(value = "desc", required = false) String desc,
            @RequestParam(value = "publish_date", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date publishDate
    ){

        try {
            XContentBuilder content = XContentFactory.jsonBuilder().startObject();
            if(title != null){
                content.field("title",title);
            }

            if(author != null){
                content.field("author",author);
            }

            if(desc != null){
                content.field("desc",desc);
            }

            if(publishDate != null){
                content.field("publish_date",publishDate.getTime());
            }

            content.endObject();
            UpdateRequest request = new UpdateRequest("article","cms",id);
            request.doc(content);
            UpdateResponse response = client.update(request).get();

            return new ResponseEntity(response.getResult(),HttpStatus.OK);
        } catch (IOException e){
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e){
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    @PostMapping("/search")
    @ResponseBody
    public ResponseEntity search(
            @RequestParam(value = "title",required = false) String title,
            @RequestParam(value = "from", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date from,
            @RequestParam(value = "to", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date to
    ){

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (title != null){
            boolQuery.must(QueryBuilders.matchQuery("title",title));
        }

        if(from != null || to != null) {
            RangeQueryBuilder rangeQuery = rangeQuery("publish_date");
            if (from != null) {
                rangeQuery.from(from.getTime());
            }
            if (to != null) {
                rangeQuery.to(to.getTime());
            }

            boolQuery.filter(rangeQuery);
        }

        SearchRequestBuilder builder = client.prepareSearch("article","test")
//                .setTypes("cms","_doc") //type即将废弃
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery)
                .setFrom(0)
                .setSize(10);

        System.out.println(builder);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM:dd HH:mm:ss");
        List<Map<String,Object>> list = new ArrayList<>();
        SearchResponse response = builder.get();
        for(SearchHit hit : response.getHits()){
            Map<String,Object> map = hit.getSourceAsMap();
            long publishDate = Long.parseLong(map.get("publish_date")+"");
            map.put("publishDate",sdf.format(publishDate));
            list.add(map);
        }
        Map<String,Object> result = new HashMap<>();
        result.put("list",list);
        result.put("total",response.getHits().getTotalHits());//查询匹配总条数
        return new ResponseEntity(result,HttpStatus.OK);
    }
}
