package projectx;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import datamodel.Constants;
import serializer.PriorityQueueSerializer;

import java.io.File;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static datamodel.LicenseKey.LICENSE_KEY;

public class JetRunner {
    static final String TWEETS = "tweets";
    static final String JET = "jet";
    static final int PARTITION_COUNT = 271;

    private static JetConfig config(int instanceId) {
        JetConfig jetCfg = new JetConfig();
        Config cfg = jetCfg.getHazelcastConfig();
        cfg.getGroupConfig().setName(JET).setPassword(Constants.PASSWORD);
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
        System.setProperty("hazelcast.partition.count", String.valueOf(PARTITION_COUNT));
        List<JetInstance> jets = IntStream.rangeClosed(1, 2)
                                          .parallel()
                                          .mapToObj(i -> Jet.newJetInstance(config(i)))
                                          .collect(toList());
        JetInstance jet = jets.get(0);
        new TweetPublisher(jet.getHazelcastInstance(), PARTITION_COUNT).start();
        return jet;
    }

    public static void main(String[] args) throws Exception {
        startJet();
    }
}
