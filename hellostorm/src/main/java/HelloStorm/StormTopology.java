package HelloStorm;


import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.Config;

public class StormTopology {

    public static void main(String[] args) throws Exception {
        //Build Toplogy
        System.out.println("Inside main");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("hello-storm-spout", new HelloStormSpout());
        builder.setBolt("hello-storm-bolt", new HelloStormBolt()).shuffleGrouping("hello-storm-spout");

        System.out.println("after setting spout and bolt");

        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead","/Volumes/My Passport for Mac/data/input.txt");
        config.put("dirToWrite","/Volumes/My Passport for Mac/data/");
        System.out.println("after setting config");

        LocalCluster cluster = new LocalCluster();

        System.out.println("after setting local cluster");

        try
        {
            System.out.println("just before submitting topology");
            cluster.submitTopology ("hello-storm-toplogy", config, builder.createTopology());
            System.out.println("just after submitting topology");
            System.out.println("about to sleep for 10 s");
            Thread.sleep(10000);
            System.out.println("waking up after 5 s");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally
        {
            System.out.println("shutting down topology");
            cluster.shutdown();
            System.out.println("done with shutting down of  topology");
        }




        System.out.println("hello storm");

    }

}
