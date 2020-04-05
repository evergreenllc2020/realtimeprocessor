package Remote;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class HashTagTopology
{
    public static void main(String[] args) throws InterruptedException
    {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Twitter-Spout", new TwitterSpout());
        builder.setBolt("hashtag-normalizer", new HashTagNormalizerBolt())
                .shuffleGrouping("Twitter-Spout");
        builder.setBolt("hashtag-counter", new HashTagCounter(),2)
                .fieldsGrouping("hashtag-normalizer", new Fields("hashtag"));


        //Configuration
        Config conf = new Config();
        conf.setDebug(true);


        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("HashTag-Topology", conf, builder.createTopology());
            Thread.sleep(60000);
        }
        finally{
            cluster.shutdown();}
    }
}
