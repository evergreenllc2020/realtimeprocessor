package Remote;

import Remote.ElasticSearchClient;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import twitter4j.Status;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Date;

public class HashTagCounter extends BaseBasicBolt
{
    Integer id;
    String name;
    Map<String, Long> counters;
    ElasticSearchClient client ;
    private long docCounter ;
    private long lastClearTime;
    /** Number of seconds before the top list will be cleared. */
    private long clearIntervalSec;

    public void prepare(Map stormConf, TopologyContext context)
    {
        this.docCounter = 0 ;
        counters = new HashMap<String, Long>();
        this.client = new ElasticSearchClient();

    }

    public void execute(Tuple input,BasicOutputCollector collector)
    {
        String hashtag = input.getString(0);
        System.out.println("Got hashtag " + hashtag);

        if(!counters.containsKey(hashtag)){
            counters.put(hashtag, 1L);
        }else{
            Long c = counters.get(hashtag) + 1;
            counters.put(hashtag, c);
        }

        //collector.
        //collector.emit(new Values(tweetText));

    }

    private String insertDoc(String hashtag, long count)
    {

        String docId = "";
        Map jsonMap = new HashMap();
        UUID uuid = UUID.randomUUID();
        jsonMap.put("hashtag", hashtag);
        //jsonMap.put("createdat", createdAt);
        jsonMap.put("count", count);

        try {
            System.out.println("Writing to ES");
             docId = client.insert(jsonMap, "hashtag", "_doc");

            System.out.println("Successfully inserted doc " + docId);
            this.docCounter += 1 ;
            System.out.println("****** DocNUM ****** = " + this.docCounter);
        }
        catch(Exception ex)
        {
            System.out.println(ex.getStackTrace());
        }
        finally
        {

        }
        return docId;

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("tweet"));
    }

    public void cleanup()
    {
        try
        {
            for(Map.Entry<String, Long> entry : counters.entrySet())
            {
                String hashtag = entry.getKey();
                long count = entry.getValue();
                String docID = insertDoc(hashtag, count);
                System.out.println("inserted doc " +  docID + "in hashtag index");
            }


            this.client.Close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
