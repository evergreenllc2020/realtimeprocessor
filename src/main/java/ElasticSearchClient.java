
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;

import java.io.IOException;
import java.util.Map;

public class ElasticSearchClient
{
    private RestHighLevelClient client;

    public ElasticSearchClient()
    {

        this.client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));






    }

    public String insert(Map jsonMap, String indexName, String indexMapping) throws IOException
    {
        IndexRequest indexRequest = new IndexRequest("twitter").source(jsonMap);
        RequestOptions options;

        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        return response.getId();


    }

    public void Close() throws IOException {
        client.close();
    }

}
