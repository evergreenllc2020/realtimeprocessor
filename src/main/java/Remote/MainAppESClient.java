package Remote;

import Remote.ElasticSearchClient;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MainAppESClient
{
    public static void main(String[] args) throws Exception {
        System.out.println("hello world");
        ElasticSearchClient client = new ElasticSearchClient();
        Map jsonMap = new HashMap();
        UUID uuid = UUID.randomUUID();
        jsonMap.put("tweet", "tweet from java " + uuid);
        jsonMap.put("createdat", "2020-02-02T15:12:12");
        jsonMap.put("user","java");
        jsonMap.put("retweetcount", 2000);

        try {
            String docId = client.insert(jsonMap, "twitter", "_doc");
            System.out.println("Successfully inserted doc " + docId);
        }
        catch(Exception ex)
        {
            String message = "";
        }
        finally
        {
            client.Close();
        }
        System.out.println("exiting app");





    }


}
