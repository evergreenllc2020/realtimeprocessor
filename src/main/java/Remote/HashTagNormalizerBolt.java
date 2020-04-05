package Remote;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

public class HashTagNormalizerBolt  extends BaseBasicBolt
{
    public void execute(Tuple input,BasicOutputCollector collector) {
        Status status = (Status) input.getValueByField("tweet");
        String tweet = status.getText();
        if(tweet != null)
        {
            String[] hashtags = tweet.split(" ");
            if(hashtags != null) {
                for (String hashtag : hashtags) {
                    hashtag = hashtag.trim();
                    if (!hashtag.isEmpty() && hashtag.startsWith("#")) {
                        hashtag = hashtag.toLowerCase();
                        System.out.println("******Got hashtag " + hashtag + " ********");
                        hashtag = hashtag.replace("#", "");
                        collector.emit(new Values(hashtag));
                    }
                }
            }
        }

    }


    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("hashtag"));
    }


}
