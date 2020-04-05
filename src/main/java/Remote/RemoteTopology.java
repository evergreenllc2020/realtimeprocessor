package Remote;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.StormSubmitter;

public class RemoteTopology {

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        try {

            //Topology definition
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("Twitter-Spout", new TwitterSpout());
            builder.setBolt("es-bolt", new ESBolt()).shuffleGrouping("Twitter-Spout");
            //Configuration
            Config conf = new Config();
            conf.setNumWorkers(2);
            conf.setMessageTimeoutSecs(60);
            conf.setDebug(true);
            System.out.println("submitting topology");

            //Topology run
            StormSubmitter.submitTopology("Twitter-Topology", conf, builder.createTopology());

            System.out.println("exiting topology");

        }
        catch(Exception ex)
        {
            System.out.println(ex.getStackTrace());
        }
    }
}
