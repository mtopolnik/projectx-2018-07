package projectx;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import serializer.PriorityQueueSerializer;

import java.io.File;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.IntStream;

import static projectx.Constants.GROUP_NAME;
import static projectx.Constants.GROUP_PASSWORD;
import static projectx.Constants.TWEETS;
import static java.util.stream.Collectors.toList;
import static projectx.LicenseKey.LICENSE_KEY;

public class JetRunner {

    private static JetConfig config(int instanceId) {
        JetConfig jetCfg = new JetConfig();
        Config cfg = jetCfg.getHazelcastConfig();
        cfg.getGroupConfig().setName(GROUP_NAME).setPassword(GROUP_PASSWORD);
        cfg.getNetworkConfig().setPort(5700 + instanceId);
        cfg.setLicenseKey(LICENSE_KEY);
        cfg.getMapEventJournalConfig(TWEETS).setEnabled(true);
        cfg.getSerializationConfig().addSerializerConfig(
                new SerializerConfig()
                        .setImplementation(new PriorityQueueSerializer())
                        .setTypeClass(PriorityQueue.class));
        cfg.getHotRestartPersistenceConfig()
             .setEnabled(true)
             .setParallelism(2)
             .setBaseDir(new File("jet-hot-restart-" + instanceId));
        jetCfg.getInternalMapConfig().getHotRestartConfig().setEnabled(true);
        return jetCfg;
    }

    static JetInstance startJet() throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.partition.count", String.valueOf(Constants.PARTITION_COUNT));
        List<JetInstance> jets = IntStream.rangeClosed(1, 2)
                                          .parallel()
                                          .mapToObj(i -> Jet.newJetInstance(config(i)))
                                          .collect(toList());
        JetInstance jet = jets.get(0);
        System.out.println("\n\nSpecial records map size: " + jet.getMap("__jet.records").size());
        new TweetPublisher(jet.getHazelcastInstance(), Constants.PARTITION_COUNT).start();
        return jet;
    }

    public static void main(String[] args) throws Exception {
        startJet();
    }
}
