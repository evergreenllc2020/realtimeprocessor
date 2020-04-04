package HelloStorm;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class HelloStormBolt extends BaseBasicBolt
{
    private PrintWriter printWriter;
    private Integer lineCounter = 0 ;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String fileName = "output" + "-" + context.getThisTaskId() + "-" + context.getThisComponentId() + ".txt" ;
        try
        {
            this.printWriter = new PrintWriter(stormConf.get("dirToWrite").toString()  + fileName, "UTF-8");
            this.lineCounter = 0;

        }
        catch(Exception e)
        {
            e.printStackTrace();

        }

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector)
    {

        String first_name = tuple.getStringByField("first_name");
        String last_name = tuple.getStringByField("last_name");
        System.out.println("bolt is processing . Bolt output is "  + first_name + " " + last_name);
        printWriter.println(first_name + "," + last_name);
        this.lineCounter+=1;
        if(this.lineCounter == 6)
        {
            printWriter.flush();
            printWriter.close();
        }
        basicOutputCollector.emit(new Values(first_name, last_name));



    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("first_name","last_name"));
    }


}
