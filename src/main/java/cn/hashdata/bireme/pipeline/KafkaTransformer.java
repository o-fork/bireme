package cn.hashdata.bireme.pipeline;

import cn.hashdata.bireme.*;
import com.codahale.metrics.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Date;
import java.util.HashMap;

/**
 * Loop through the {@code ChangeSet} and transform each change data into a {@code Row}.
 *
 * @author yuze
 */
public abstract class KafkaTransformer extends Transformer {

    @Override
    @SuppressWarnings("unchecked")
    public void fillRowSet(RowSet rowSet) throws BiremeException {
        CommitCallback callback = changeSet.callback;
        HashMap<String, Long> offsets = ((KafkaCommitCallback) callback).partitionOffset;
        Row row = null;

        for (ConsumerRecord<String, String> change : (ConsumerRecords<String, String>) changeSet.changes) {
            row = new Row();

            if (!transform(change, row)) {
                continue;
            }

            addToRowSet(row, rowSet);
            offsets.put(change.topic() + "+" + change.partition(), change.offset());
            callback.setNewestRecord(row.produceTime);
        }

        callback.setNumOfTables(rowSet.rowBucket.size());
        rowSet.callback = callback;
    }

    /**
     * Transform the change data into a {@code Row}.
     *
     * @param change the change data
     * @param row    an empty {@code Row} to store the result.
     * @return {@code true} if transform the change data successfully, {@code false} it the change
     * data is null or filtered
     * @throws BiremeException when can not get the field
     */
    public abstract boolean transform(ConsumerRecord<String, String> change, Row row) throws BiremeException;
}


