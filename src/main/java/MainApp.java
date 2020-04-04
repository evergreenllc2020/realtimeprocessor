import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MainApp
{
    public static void main(String[] args) throws Exception {
        System.out.println("hello world");
        ElasticSearchClient client = new ElasticSearchClient();
        Map jsonMap = new HashMap();
        jsonMap.put("tweet", "tweet from java");
        jsonMap.put("createdat", "2020-02-02T15:12:12");
        jsonMap.put("user","java");
        jsonMap.put("retweetcount", 2000);

        try {
            String docId = client.insert(jsonMap, "twitter", "_doc");
        }
        catch(Exception ex)
        {
            String message = "";
        }
        System.out.println("exiting app");

        /*
        TwitterProcessor processor = new TwitterProcessor();
        processor.connect();
        Thread.sleep(5000);
        List<String> messages = processor.getTop5Messages();
        for(String message:messages)
        {
            System.out.println(message);
        }
        System.out.println("exiting app");
        */



    }


}
