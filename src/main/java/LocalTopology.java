

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class LocalTopology
{
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Twitter-Spout", new TwitterSpout());
        builder.setBolt("es-bolt", new ESBolt(),3)
                .shuffleGrouping("Twitter-Spout");

        //Configuration
        Config conf = new Config();
        conf.setDebug(true);


        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("TwitterTopology", conf, builder.createTopology());
            Thread.sleep(10000);
        }
        finally{
            cluster.shutdown();}
    }
}
