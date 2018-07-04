package projectx;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import datamodel.Constants;
import datamodel.LicenseKey;
import datamodel.Tweet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedComparator.comparing;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.pipeline.ContextFactories.iMapContext;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.map;
import static com.hazelcast.jet.pipeline.Sources.remoteMapJournal;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static datamodel.Constants.PUBLISHER;
import static datamodel.Constants.PUBLISHER_PORT;
import static datamodel.Constants.PUBLISH_KEY;
import static datamodel.Constants.SAMPLES_HOME;
import static java.util.stream.Collectors.toList;
import static projectx.JetRunner.TWEETS;
import static projectx.JetRunner.startJet;

public class TrendingWordsInTweets {

    public static void main(String[] args) throws Exception {
        JetInstance jet = startJet();
        loadStopwordsIntoIMap(jet);
        jet.newJob(buildPipeline());
    }

    static Pipeline buildPipeline() {

        Pipeline p = Pipeline.create();

        StreamStage<Tweet> tweets = p.drawFrom(remoteMapJournal(
                TWEETS, publisherClientConfig(), mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST));

        // Tweet{10:23:00.0, "It was the age of wisdom"}, Tweet{10:23:03.0, "It was the age of foolishness"}, ...

        StreamStage<String> tweetTexts = tweets
                .addTimestamps(Tweet::timestamp, 10)
                .map(Tweet::text);

        // "It was the age of wisdom", "It was the age of foolishness", ...

        StreamStage<String> words = tweetTexts
                .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
                .filter(word -> !word.isEmpty() && !word.matches(".*?\\d.*"))
                .filterUsingContext(iMapContext(Constants.STOPWORDS),
                        (stopwords, word) ->!stopwords.containsKey(word));

        // "age", "wisdom", "age", "foolishness", ...

        StreamStage<TimestampedEntry<String, Long>> wordFrequencies = words
                .window(sliding(10_000, 100))
                .addKey(wholeItem())
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
                .drainTo(map(Constants.TOP_LIST));

        return p;
    }

    private static void loadStopwordsIntoIMap(JetInstance jet) throws IOException {
        IMap<String, Integer> swMap = jet.getHazelcastInstance().getMap(Constants.STOPWORDS);
        Files.lines(Paths.get(SAMPLES_HOME + "/stopwords.txt")).forEach(sw -> swMap.put(sw, 0));
    }

    private static ClientConfig publisherClientConfig() {
        ClientConfig clientCfg = new ClientConfig();
        clientCfg.getGroupConfig().setName(PUBLISHER);
        clientCfg.setLicenseKey(LicenseKey.LICENSE_KEY);
        clientCfg.getNetworkConfig().addAddress("localhost:" + PUBLISHER_PORT);
        return clientCfg;
    }

    private static <T> AggregateOperation1<T, ?, List<T>> topN(
            int n, DistributedComparator<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        DistributedComparator<? super T> comparatorReversed = comparator.reversed();
        DistributedBiConsumer<PriorityQueue<T>, T> accumulateFn = (PriorityQueue<T> a, T i) -> {
            if (a.size() == n) {
                if (comparator.compare(i, a.peek()) <= 0) {
                    // the new item is smaller or equal to the smallest in queue
                    return;
                }
                a.poll();
            }
            a.offer(i);
        };
        return AggregateOperation
                .withCreate(() -> new PriorityQueue<T>(n, comparator))
                .andAccumulate(accumulateFn)
                .andCombine((a1, a2) -> {
                    for (T t : a2) {
                        accumulateFn.accept(a1, t);
                    }
                })
                .andExportFinish(a -> {
                    ArrayList<T> res = new ArrayList<>(a);
                    res.sort(comparatorReversed);
                    return res;
                });
    }
}

