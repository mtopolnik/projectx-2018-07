package projectx;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedComparator;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static projectx.TrendingWordsInTweets.TWEETS;

class TweetPublisher extends Thread {
    private final List<String> inputKeys;
    private volatile boolean keepRunning = true;
    private volatile boolean enabled;

    private final IMap<Object, Tweet> map;
    private final Iterator<String> lines;

    TweetPublisher(
            String pathToSourceFiles,
            JetInstance jet,
            int partitionCount
    ) throws Exception {
        this.map = jet.getMap(TWEETS);
        this.inputKeys = IntStream.range(0, partitionCount)
                                  .mapToObj(i -> generateKeyForPartition(jet.getHazelcastInstance(), i))
                                  .collect(toList());
        this.lines = stream(newDirectoryStream(Paths.get(pathToSourceFiles), "*.txt").spliterator(), false)
                .flatMap(p -> uncheckCall(() -> Files.lines(p)))
                .iterator();
    }

    @Override
    public void run() {
        Iterator<String> keyIter = inputKeys.iterator();
        while (lines.hasNext() && keepRunning) {
            if (!enabled) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(1));
                continue;
            }
            if (!keyIter.hasNext()) {
                keyIter = inputKeys.iterator();
            }
            map.put(keyIter.next(), new Tweet(System.currentTimeMillis(), lines.next()));
        }
    }

    void generateEvents(int seconds) throws InterruptedException {
        enabled = true;
        System.out.println("\n\nGenerating text events\n");
        Thread.sleep(SECONDS.toMillis(seconds));
        System.out.println("\n\nStopped text events\n");
        enabled = false;
    }

    void shutdown() {
        keepRunning = false;
    }



    static <T> AggregateOperation1<T, ?, List<T>> topN(
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
                .andFinish(a -> {
                    ArrayList<T> res = new ArrayList<>(a);
                    res.sort(comparatorReversed);
                    return res;
                });
    }

    private static String generateKeyForPartition(HazelcastInstance hz, int partitionId) {
        Cluster cluster = hz.getCluster();

        PartitionService partitionService = hz.getPartitionService();
        while (true) {
            String id = randomUUID().toString();
            Partition partition = partitionService.getPartition(id);
            if (partition.getPartitionId() == partitionId) {
                return id;
            }
        }
    }
}
