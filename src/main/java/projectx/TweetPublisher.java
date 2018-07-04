package projectx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.jet.impl.util.Util;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static projectx.Constants.BOOKS_DIR;
import static projectx.Constants.TWEETS;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

public class TweetPublisher extends Thread {

    private final IMap<Object, Tweet> map;
    private final Iterator<String> lines;
    private final List<String> inputKeys;

    TweetPublisher(HazelcastInstance hz, int partitionCount) throws Exception {
        this.map = hz.getMap(TWEETS);
        this.inputKeys = IntStream.range(0, partitionCount)
                                  .mapToObj(i -> generateKeyForPartition(hz, i))
                                  .collect(toList());
        this.lines = stream(newDirectoryStream(Paths.get(BOOKS_DIR), "*.txt").spliterator(), false)
                .flatMap(p -> Util.uncheckCall(() -> Files.lines(p)))
                .iterator();
    }

    @Override
    public void run() {
        Iterator<String> keyIter = inputKeys.iterator();
        int publishedCount = 0;
        while (lines.hasNext()) {
            LockSupport.parkNanos(MICROSECONDS.toNanos(200));
            if (publishedCount % 10_000 == 0) {
                System.out.format("Published %,3d events%n", publishedCount);
            }
            if (!keyIter.hasNext()) {
                keyIter = inputKeys.iterator();
            }
            map.put(keyIter.next(), new Tweet(System.currentTimeMillis(), lines.next()));
            publishedCount++;
        }
        System.out.println("No more events");
    }

    private static String generateKeyForPartition(HazelcastInstance hz, int partitionId) {
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
