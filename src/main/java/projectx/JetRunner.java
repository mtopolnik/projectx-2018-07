package projectx;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import serializer.PriorityQueueSerializer;

import java.io.File;
import java.util.PriorityQueue;

import static projectx.LicenseKey.LICENSE_KEY;

public class JetRunner {
    static final String TWEETS = "tweets";
    static final int PARTITION_COUNT = 271;

    static JetConfig config(int instanceId) {
        JetConfig cfg = new JetConfig();
        Config hzCfg = cfg.getHazelcastConfig();
        hzCfg.setLicenseKey(LICENSE_KEY);
        hzCfg.getMapEventJournalConfig(TWEETS).setEnabled(true);
        hzCfg.getSerializationConfig().addSerializerConfig(
                new SerializerConfig()
                        .setImplementation(new PriorityQueueSerializer())
                        .setTypeClass(PriorityQueue.class));
        HotRestartPersistenceConfig hrCfg = hzCfg.getHotRestartPersistenceConfig();
        hrCfg.setEnabled(true).setParallelism(2).setBaseDir(new File("jet-hot-restart-" + instanceId));
        hzCfg.getMapConfig("*").getHotRestartConfig().setEnabled(true);
        cfg.getInternalMapConfig().getHotRestartConfig().setEnabled(true);
        return cfg;
    }

    public static JetInstance startJet() {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.partition.count", String.valueOf(PARTITION_COUNT));
        JetInstance jet = Jet.newJetInstance(config(1));
        Jet.newJetInstance(config(2));
        return jet;
    }

    public static void main(String[] args) {
        startJet();
    }
}
