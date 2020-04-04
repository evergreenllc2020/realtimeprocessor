package HelloStorm;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class HelloStormSpout extends BaseRichSpout
{
    private SpoutOutputCollector collector;
    private Integer i = 0 ;
    private FileReader fileReader ;
    private BufferedReader reader ;
    private String str;
    private boolean completed = false;


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        System.out.println("Spout Initialization");

        try {
            fileReader = new FileReader(map.get("fileToRead").toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.collector = spoutOutputCollector ;
        this.reader = new BufferedReader(fileReader);
        this.completed = false;


    }

    @Override
    public void nextTuple()
    {
        if(this.completed == false)
        {
            System.out.println("inside Spout ... reading input from file  ");
            try {
                this.str = reader.readLine();
                System.out.println("read input from file : " + str);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(str!=null)
            {
                this.collector.emit(new Values(this.str.split(",")));
            }
            else
            {
                completed = true;
                try {
                    fileReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        System.out.println("Inside nextTuple of spout");
        //this.collector.emit(new Values(this.i));
        //this.i += 1 ;


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("id", "first_name","last_name","gender","email"));

    }
}
