package Remote;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;



public class TwitterProcessor {
    //Queue for tweets
    private LinkedBlockingQueue<Status> queue;
    //stream of tweets
    private TwitterStream twitterStream;

    public void connect()
    {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("w8KEQztNWZ6US0N6mQwRtPsiT")
                .setOAuthConsumerSecret("dOGiEosuWKUgPsx49i5WVjJfBaHnwLAGO4dqUIE4O6VjzJbmrN")
                .setOAuthAccessToken("1199506694830559232-x44B1EUGqgL4CGACjyzvIIVvptgRvn")
                .setOAuthAccessTokenSecret("6WH4oXbPHkTz13XxoUCEeuqpTx9s6RPqZ0uNfaY5JQSsw");


        this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        this.queue = new LinkedBlockingQueue<Status>();

        final StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {

                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }


            public void onTrackLimitationNotice(int i) {
            }


            public void onScrubGeo(long l, long l1) {
            }


            public void onException(Exception e) {
            }


            public void onStallWarning(StallWarning warning) {
            }
        };

        twitterStream.addListener(listener);
        final FilterQuery query = new FilterQuery();
        query.track(new String[]{"covid-19"});
        twitterStream.filter(query);

    }

    public List<String> getTop5Messages()
    {
        List<String> messages = new ArrayList<String>();

        int counter;
        counter = 0;
        System.out.println("Queue Size is " + queue.size());
        while (counter <5)
        {
            Status status = this.queue.poll();
            messages.add(status.getText());
            counter++;
        }
        return messages;
    }




}
