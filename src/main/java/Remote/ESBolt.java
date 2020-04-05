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

public class ESBolt extends BaseBasicBolt
{
    ElasticSearchClient client ;
    private long docCounter ;

    public void prepare(Map stormConf, TopologyContext context)
    {
        this.docCounter = 0 ;
        this.client = new ElasticSearchClient();
    }

    public void cleanup()
    {
        try {
            this.client.Close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void execute(Tuple input,BasicOutputCollector collector) {


        System.out.println("Inside es bolt's execute method");
        Status status = (Status) input.getValueByField("tweet");
        String tweetText = status.getText();
        long retweetCount = status.getRetweetCount();
        String user = status.getUser().getName();
        Date createdAt = status.getCreatedAt();
        String profileUrl = status.getUser().get400x400ProfileImageURLHttps();


        Map jsonMap = new HashMap();
        UUID uuid = UUID.randomUUID();
        jsonMap.put("tweet", tweetText);
        jsonMap.put("createdat", createdAt);
        jsonMap.put("user",user);
        jsonMap.put("retweetcount", retweetCount);

        try {
            System.out.println("Writing to ES");
            String docId = client.insert(jsonMap, "twitter", "_doc");

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
        //collector.
        //collector.emit(new Values(tweetText));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("tweet"));
    }



}
