package Remote;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MainAppTwitterProcessor
{
    public static void main(String[] args) throws Exception {
        System.out.println("hello world");



        TwitterProcessor processor = new TwitterProcessor();
        processor.connect();
        Thread.sleep(5000);
        List<String> messages = processor.getTop5Messages();
        for(String message:messages)
        {
            System.out.println(message);
        }
        System.out.println("exiting app");


        System.out.println("exiting app");



    }


}

