package cn.hashdata.bireme.pipeline;

import cn.hashdata.bireme.AbstractCommitCallback;
import cn.hashdata.bireme.PipeLineStat;
import com.codahale.metrics.Timer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Date;
import java.util.HashMap;

/**
 * {@code KafkaCommitCallback} is used to trace a {@code ChangeSet} polled from Kafka. After the
 * change data has been applied, commit the offset to Kafka.
 *
 * @author yuze
 */
public class KafkaCommitCallback extends AbstractCommitCallback {
    protected KafkaConsumer<String, String> consumer;

    public HashMap<String, Long> partitionOffset;
    private Timer.Context timerCTX;
    private Date start;
    public PipeLineStat stat;

    public KafkaCommitCallback() {
        this.partitionOffset = new HashMap<String, Long>();

        // record the time being created
        timerCTX = stat.avgDelay.time();
        start = new Date();
    }

    @Override
    public void commit() {
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();

        partitionOffset.forEach((key, value) -> {
            String topic = key.split("\\+")[0];
            int partition = Integer.valueOf(key.split("\\+")[1]);
            offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(value + 1));
        });

        consumer.commitSync(offsets);
        committed.set(true);
        partitionOffset.clear();

        // record the time being committed
        timerCTX.stop();

        stat.newestCompleted = newestRecord;
        stat.delay = System.currentTimeMillis() - start.getTime();
    }

    @Override
    public void destory() {
        super.destory();
        partitionOffset.clear();
        partitionOffset = null;
        timerCTX = null;
        start = null;
    }
}