package cn.hashdata.bireme.pipeline;

import cn.hashdata.bireme.*;
import com.codahale.metrics.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * {@code KafkaPipeLine} is a kind of {@code PipeLine} that polls data from Kafka.
 *
 * @author yuze
 */
public abstract class KafkaPipeLine extends PipeLine {

    private final long POLL_TIMEOUT = 100L;

    protected KafkaConsumer<String, String> consumer;
    protected LinkedBlockingQueue<KafkaCommitCallback> commitCallbacks;

    public KafkaPipeLine(Context cxt, SourceConfig conf, String myName) {
        super(cxt, conf, myName);
        consumer = KafkaPipeLine.createConsumer(conf.server, conf.groupID);
        commitCallbacks = new LinkedBlockingQueue<>();
    }

    @Override
    public ChangeSet pollChangeSet() throws BiremeException {
        ConsumerRecords<String, String> records = null;

        try {
            records = consumer.poll(POLL_TIMEOUT);
        } catch (InterruptException e) {
        }

        if (cxt.stop || records == null || records.isEmpty()) {
            return null;
        }

        KafkaCommitCallback callback = new KafkaCommitCallback();

        if (!commitCallbacks.offer(callback)) {
            String Message = "Can't add CommitCallback to queue.";
            throw new BiremeException(Message);
        }

        stat.recordCount.mark(records.count());

        return packRecords(records, callback);
    }

    @Override
    public void checkAndCommit() {
        CommitCallback callback = null;

        while (!commitCallbacks.isEmpty()) {
            if (commitCallbacks.peek().ready()) {
                callback = commitCallbacks.remove();
            } else {
                break;
            }
        }

        if (callback != null) {
            callback.commit();
        }
    }

    private ChangeSet packRecords(ConsumerRecords<String, String> records, KafkaCommitCallback callback) {
        ChangeSet changeSet = new ChangeSet();
        changeSet.createdAt = new Date();
        changeSet.changes = records;
        changeSet.callback = callback;

        return changeSet;
    }

    /**
     * Create a new KafkaConsumer, specify the server's ip and port, and groupID.
     *
     * @param server  ip and port for Kafka server
     * @param groupID consumer's group id
     * @return the consumer
     */
    public static KafkaConsumer<String, String> createConsumer(String server, String groupID) {
        Properties props = new Properties();
        props.put("bootstrap.servers", server);
        props.put("group.id", groupID);
        props.put("enable.auto.commit", false);
        props.put("session.timeout.ms", 60000);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(props);
    }
}
