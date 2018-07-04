package publisher;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.impl.util.Util;
import datamodel.Tweet;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.locks.LockSupport;

import static datamodel.Constants.BOOKS_DIR;
import static datamodel.Constants.PUBLISHER;
import static datamodel.Constants.PUBLISHER_PORT;
import static datamodel.Constants.PUBLISH_KEY;
import static datamodel.Constants.TWEETS;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.stream.StreamSupport.stream;

class TweetPublisher extends Thread {

    private final IMap<Object, Tweet> map;
    private final Iterator<String> lines;

    private TweetPublisher(IMap<Object, Tweet> map) throws Exception {
        this.map = map;
        this.lines = stream(newDirectoryStream(Paths.get(BOOKS_DIR), "*.txt").spliterator(), false)
                .flatMap(p -> Util.uncheckCall(() -> Files.lines(p)))
                .iterator();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.partition.count", "1");

        Config cfg = new Config();
        cfg.getGroupConfig().setName(PUBLISHER);
        cfg.getNetworkConfig().setPort(PUBLISHER_PORT);
        cfg.getMapEventJournalConfig(TWEETS).setEnabled(true);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        IMap<Object, Tweet> map = hz.getMap(TWEETS);
        TweetPublisher publisher = new TweetPublisher(map);
        publisher.start();
    }

    @Override
    public void run() {
        int publishedCount = 0;
        while (lines.hasNext()) {
            LockSupport.parkNanos(MICROSECONDS.toNanos(200));
            if (publishedCount % 10_000 == 0) {
                System.out.format("Published %,3d events%n", publishedCount);
            }
            map.put(PUBLISH_KEY, new Tweet(System.currentTimeMillis(), lines.next()));
            publishedCount++;
        }
        System.out.println("No more lines");
    }
}