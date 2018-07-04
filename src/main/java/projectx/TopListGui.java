package projectx;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.datamodel.TimestampedItem;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import static java.awt.EventQueue.invokeLater;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toMap;
import static projectx.TrendingWordsInTweets.PUBLISH_KEY;

class TopListGui {
    private static final int WINDOW_X = 600;
    private static final int WINDOW_Y = 150;
    private static final int WINDOW_WIDTH = 500;
    private static final int WINDOW_HEIGHT = 650;

    private final IMap<Object, TimestampedItem<List<String>>> topList;
    private Timer timer;
    private JFrame frame;

    TopListGui(IMap<Object, TimestampedItem<List<String>>> topList) {
        this.topList = topList;
        invokeLater(this::buildFrame);
    }

    void shutdown() {
        timer.stop();
        frame.dispose();
    }

    private void buildFrame() {
        frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setTitle("Hazelcast Jet - Trending Words in Tweets");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        final JPanel mainPanel = new JPanel();
        mainPanel.setBackground(Color.WHITE);
        mainPanel.setLayout(new BorderLayout(10, 10));
        mainPanel.setBorder(new EmptyBorder(20, 120, 20, 20));
        frame.add(mainPanel);
        final JTextArea output = new JTextArea();
        mainPanel.add(output, BorderLayout.CENTER);
        output.setFont(output.getFont().deriveFont(18f));
        DateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
        timer = new Timer(100, e -> {
            TimestampedItem<List<String>> timestampedTopList = topList.get(PUBLISH_KEY);
            if (timestampedTopList == null) {
                return;
            }
            output.setText(
                    df.format(timestampedTopList.timestamp()) + "\n\n" +
                    timestampedTopList.item().stream().collect(joining("\n")));
        });
        timer.start();
        frame.setVisible(true);
    }
}
