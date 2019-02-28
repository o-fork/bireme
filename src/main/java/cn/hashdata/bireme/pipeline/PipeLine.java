package cn.hashdata.bireme.pipeline;

import cn.hashdata.bireme.*;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.concurrent.*;

/**
 * {@code PipeLine} is a bridge between data source and target table. The data flow order is
 * guaranteed. A {@code PipeLine} does four things as follows:
 * <ul>
 * <li>Poll data and allocate {@link Transformer} to convert the data.</li>
 * <li>Dispatch the transformed data and insert it into {@link RowCache}.</li>
 * <li>Drive the {@code RowBatchMerger} to work</li>
 * <li>Drivet the {@code ChangeLoader} to work</li>
 * </ul>
 *
 * @author yuze
 */
public abstract class PipeLine implements Callable<PipeLine> {

    public enum PipeLineState {NORMAL, ERROR}

    public Logger logger;

    public String myName;
    public volatile PipeLineState state;
    public Exception e;
    public PipeLineStat stat;

    public Context cxt;
    public SourceConfig conf;

    public LinkedBlockingQueue<Future<RowSet>> transResult;
    private LinkedList<Transformer> localTransformer;

    private Dispatcher dispatcher;

    public ConcurrentHashMap<String, RowCache> cache;

    public PipeLine(Context cxt, SourceConfig conf, String myName) {
        this.myName = myName;
        this.state = PipeLineState.NORMAL;
        this.e = null;

        this.cxt = cxt;
        this.conf = conf;

        int queueSize = cxt.conf.transformQueueSize;

        transResult = new LinkedBlockingQueue<Future<RowSet>>(queueSize);
        localTransformer = new LinkedList<Transformer>();

        cache = new ConcurrentHashMap<String, RowCache>();

        dispatcher = new Dispatcher(cxt, this);

        for (int i = 0; i < queueSize; i++) {
            localTransformer.add(createTransformer());
        }

        // initialize statistics
        this.stat = new PipeLineStat(this);
    }

    @Override
    public PipeLine call() {
        try {
            executePipeline();
        } catch (Exception e) {
            state = PipeLineState.ERROR;
            this.e = e;

            logger.error("Execute Pipeline failed: {}", e.getMessage());
            logger.error("Stack Trace: ", e);
        }

        return this;
    }

    private PipeLine executePipeline() {
        // Poll data and start transformer
        if (!transData()) {
            return this;
        }

        // Start dispatcher, only one dispatcher for each pipeline
        if (!startDispatch()) {
            return this;
        }

        // Start merger
        if (!startMerge()) {
            return this;
        }

        checkAndCommit(); // Commit result
        return this;
    }

    private boolean transData() {
        while (transResult.remainingCapacity() != 0) {
            ChangeSet changeSet = null;

            try {
                changeSet = pollChangeSet();
            } catch (BiremeException e) {
                state = PipeLineState.ERROR;
                this.e = e;

                logger.error("Poll change set failed. Message: {}", e.getMessage());
                logger.error("Stack Trace: ", e);

                return false;
            }

            if (changeSet == null) {
                break;
            }

            Transformer trans = localTransformer.remove();
            trans.setChangeSet(changeSet);
            startTransform(trans);
            localTransformer.add(trans);
        }

        return true;
    }

    private boolean startDispatch() {
        try {
            dispatcher.dispatch();
        } catch (BiremeException e) {
            state = PipeLineState.ERROR;
            this.e = e;

            logger.error("Dispatch failed. Message: {}", e.getMessage());
            logger.error("Stack Trace: ", e);

            return false;

        } catch (InterruptedException e) {
            state = PipeLineState.ERROR;
            this.e = new BiremeException("Dispatcher failed, be interrupted", e);

            logger.info("Interrupted when getting transform result. Message: {}.", e.getMessage());
            logger.info("Stack Trace: ", e);

            return false;
        }

        return true;
    }

    private boolean startMerge() {
        for (RowCache rowCache : cache.values()) {
            if (rowCache.shouldMerge()) {
                rowCache.startMerge();
            }

            try {
                rowCache.startLoad();

            } catch (BiremeException e) {
                state = PipeLineState.ERROR;
                this.e = e;

                logger.info("Loader for {} failed. Message: {}.", rowCache.tableName, e.getMessage());
                logger.info("Stack Trace: ", e);

                return false;

            } catch (InterruptedException e) {
                state = PipeLineState.ERROR;
                this.e = new BiremeException("Get Future<Long> failed, be interrupted", e);

                logger.info("Interrupted when getting loader result for {}. Message: {}.",
                        rowCache.tableName, e.getMessage());
                logger.info("Stack Trace: ", e);

                return false;
            }
        }

        return true;
    }

    /**
     * Poll a set of change data from source and pack it to {@link ChangeSet}.
     *
     * @return a packed change set
     * @throws BiremeException Exceptions when poll data from source
     */
    public abstract ChangeSet pollChangeSet() throws BiremeException;

    /**
     * Check whether the loading operation is complete. If true, commit it.
     */
    public abstract void checkAndCommit();

    /**
     * Create a new {@link Transformer} to work parallel.
     *
     * @return a new {@code Transformer}
     */
    public abstract Transformer createTransformer();

    private void startTransform(Transformer trans) {
        ExecutorService transformerPool = cxt.transformerPool;
        Future<RowSet> result = transformerPool.submit(trans);
        transResult.add(result);
    }

    /**
     * Get the unique name for the {@code PipeLine}.
     *
     * @return the name for the {@code PipeLine}
     */
    public String getPipeLineName() {
        return conf.name;
    }

}
