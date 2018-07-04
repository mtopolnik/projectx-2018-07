package projectx;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedComparator.comparing;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.pipeline.ContextFactories.iMapContext;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.map;
import static com.hazelcast.jet.pipeline.Sources.mapJournal;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.stream.Collectors.toList;
import static projectx.TweetPublisher.topN;

public class TrendingWordsInTweets {

    static final int PUBLISH_KEY = 42;
    static final String TWEETS = "tweets";
    static final String TOP_LIST = "top-list";
    static final String STOPWORDS = "stopwords";
    // git clone https://github.com/hazelcast/hazelcast-jet-code-samples.git
    static final String SAMPLES_HOME =
            "/Users/mtopol/dev/java/hazelcast-jet-code-samples/sample-data/src/main/resources";

    public static void main(String[] args) throws Exception {
        int partitionCount = 271;
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.partition.count", String.valueOf(partitionCount));

        Pipeline pipeline = buildPipeline();

        JetConfig cfg = new JetConfig();
        cfg.getHazelcastConfig().getMapEventJournalConfig(TWEETS).setEnabled(true);
        JetInstance jet = Jet.newJetInstance(cfg);

        loadStopwordsIntoIMap(jet);
        TweetPublisher publisher = new TweetPublisher(SAMPLES_HOME + "/books", jet, partitionCount);
        publisher.start();
        TopListGui gui = new TopListGui(jet.getMap(TOP_LIST));
        try {
            Job job = jet.newJob(pipeline);
            publisher.generateEvents(120);
            Thread.sleep(1000);
            job.cancel();
        } finally {
            publisher.shutdown();
            gui.shutdown();
            Jet.shutdownAll();
        }
    }

    static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        StreamStage<Tweet> tweets = p.drawFrom(
                mapJournal(TWEETS, mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST));

        // Tweet{10:23:00.0, "It was the age of wisdom"}, Tweet{10:23:03.0, "It was the age of foolishness"}, ...

        StreamStage<String> tweetTexts = tweets
                .addTimestamps(Tweet::timestamp, 10)
                .map(Tweet::text);

        // "It was the age of wisdom", "It was the age of foolishness", ...

        StreamStage<String> words = tweetTexts
                .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
                .filter(word -> !word.isEmpty() && !word.matches(".*?\\d.*"))
                .filterUsingContext(iMapContext(STOPWORDS),
                        (stopwords, word) ->!stopwords.containsKey(word));

        // "age", "wisdom", "age", "foolishness", ...

        StreamStage<TimestampedEntry<String, Long>> wordFrequencies = words
                .window(sliding(10_000, 100))
                .groupingKey(wholeItem())
                .aggregate(counting());

        // {10:23:10.0, "age", 2}, {10:23:10.0, "wisdom", 1}, {10:23:10.0, "foolishness", 1},
        // {10:23:10.1, "age", 1}, {10:23:10.1, "foolishness", 1}

        StreamStage<TimestampedItem<List<String>>> topLists = wordFrequencies
                .window(tumbling(100))
                .aggregate(topN(20, comparing(Entry::getValue)),
                        (winStart, winEnd, topList) -> new TimestampedItem<>(winEnd,
                                topList.stream().map(Entry::getKey).collect(toList())));

        // {10:23:10.0, ["age", "wisdom", "foolishness"]}
        // {10:23:10.1, ["age", "foolishness"]}

        topLists.map(timestampedTopList -> entry(PUBLISH_KEY, timestampedTopList))
                .drainTo(map(TOP_LIST));

        return p;
    }

    static void loadStopwordsIntoIMap(JetInstance jet) throws IOException {
        IMap<String, Integer> swMap = jet.getHazelcastInstance().getMap(STOPWORDS);
        Files.lines(Paths.get(SAMPLES_HOME + "/stopwords.txt")).forEach(sw -> swMap.put(sw, 0));
    }
}
