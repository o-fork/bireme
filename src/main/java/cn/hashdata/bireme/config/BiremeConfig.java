/**
 * Copyright HashData. All Rights Reserved.
 */

package cn.hashdata.bireme.config;

import cn.hashdata.bireme.BiremeException;
import cn.hashdata.bireme.pipeline.SourceConfig;
import cn.hashdata.bireme.pipeline.SourceConfig.SourceType;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Configurations about bireme.
 *
 * @author yuze
 */
public class BiremeConfig {

    private static final String DEFAULT_TABLEMAP_DIR = "etc/";

    private Logger logger = LogManager.getLogger("Bireme." + BiremeConfig.class);

    private Configuration config;

    public int pipelinePoolSize;

    public int transformPoolSize;
    public int transformQueueSize;

    public int rowCacheSize;

    public int mergePoolSize;
    public int mergeInterval;
    public int batchSize;

    public int loaderConnSize;
    public int loaderTaskQueueSize;
    public int loadersCount;

    public String reporter;
    public int reportInterval;

    public String stateServerAddr;
    public int stateServerPort;

    public ConnectionConfig targetDatabase;

    public HashMap<String, SourceConfig> sourceConfig;
    public HashMap<String, String> tableMap;

    public static class ConnectionConfig {
        public String jdbcUrl;
        public String user;
        public String passwd;
    }


    private void basicConfig() {
        pipelinePoolSize = config.getInt("pipeline.thread_pool.size", 5);

        transformPoolSize = config.getInt("transform.thread_pool.size", 10);
        transformQueueSize = transformPoolSize;

        mergePoolSize = config.getInt("merge.thread_pool.size", 10);
        mergeInterval = config.getInt("merge.interval", 10000);
        batchSize = config.getInt("merge.batch.size", 50000);
        rowCacheSize = batchSize * 2;

        loaderConnSize = config.getInt("loader.conn_pool.size", 10);
        loaderTaskQueueSize = config.getInt("loader.task_queue.size", 2);

        reporter = config.getString("metrics.reporter", "console");
        reportInterval = config.getInt("metrics.reporter.console.interval", 15);

        stateServerAddr = config.getString("state.server.addr", "0.0.0.0");
        stateServerPort = config.getInt("state.server.port", 8080);

        sourceConfig = new HashMap<>();
        tableMap = new HashMap<>();
    }

    public BiremeConfig() {
        config = new PropertiesConfiguration();
        basicConfig();
    }

    /**
     * Read config file and store in {@code Config}.
     *
     * @param configFileDir the config file.
     * @throws ConfigurationException - if an error occurred when loading the configuration
     * @throws BiremeException        - wrap and throw Exception which cannot be handled
     */
    public BiremeConfig(String configFileDir) throws ConfigurationException, BiremeException {
        Configurations configs = new Configurations();
        config = configs.properties(new File(configFileDir));

        basicConfig();
        connectionConfig("target");
        dataSourceConfig();

        logConfig();
    }


    /**
     * Get the connection configuration to database.
     *
     * @param prefix "target" database
     * @throws BiremeException when url of database is null
     */
    protected void connectionConfig(String prefix) throws BiremeException {
        Configuration subConfig = new SubsetConfiguration(config, "target", ".");
        targetDatabase = new ConnectionConfig();

        targetDatabase.jdbcUrl = subConfig.getString("url");
        targetDatabase.user = subConfig.getString("user");
        targetDatabase.passwd = subConfig.getString("passwd");

        if (targetDatabase.jdbcUrl == null) {
            String message = "Please designate url for target Database.";
            throw new BiremeException(message);
        }
    }

    protected void dataSourceConfig() throws BiremeException, ConfigurationException {
        String[] sources = config.getString("data_source").replaceAll("[ \f\n\r\t]", "").split(",");
        if (sources == null || sources.length == 0) {
            String message = "Please designate at least one data source.";
            logger.fatal(message);
            throw new BiremeException(message);
        }
        for (int i = 0; i < sources.length; i++) {
            sourceConfig.put(sources[i], new SourceConfig(sources[i]));
        }

        fetchSourceAndTableMap();
    }

    /**
     * Get the {@code Provider} configuration.
     *
     * @throws BiremeException        miss some required configuration
     * @throws ConfigurationException if an error occurred when loading the configuration
     */
    protected void fetchSourceAndTableMap() throws BiremeException, ConfigurationException {
        loadersCount = 0;

        for (SourceConfig conf : sourceConfig.values()) {
            String type = config.getString(conf.name + ".type");
            if (type == null) {
                String message = "Please designate the data source type of " + conf.name;
                logger.fatal(message);
                throw new BiremeException(message);
            }

            switch (type) {
                case "maxwell":
                    fetchMaxwellConfig(conf);
                    break;

                case "debezium":
                    fetchDebeziumConfig(conf);
                    break;

                default:
                    String message = "Unrecognized type for data source " + conf.name;
                    logger.fatal(message);
                    throw new BiremeException(message);
            }

            conf.tableMap = fetchTableMap(conf.name);
        }

        if (loaderConnSize > loadersCount) {
            loaderConnSize = loadersCount;
        }
    }

    /**
     * Get DebeziumSource configuration.
     *
     * @param debeziumConf An empty {@code SourceConfig}
     * @throws BiremeException miss some required configuration
     */
    protected void fetchDebeziumConfig(SourceConfig debeziumConf) throws BiremeException {
        Configuration subConfig = new SubsetConfiguration(config, debeziumConf.name, ".");

        String prefix = subConfig.getString("namespace");
        if (prefix == null) {
            String messages = "Please designate your namespace.";
            logger.fatal(messages);
            throw new BiremeException(messages);
        }

        debeziumConf.type = SourceType.DEBEZIUM;
        debeziumConf.server = subConfig.getString("kafka.server");
        debeziumConf.topic = prefix;
        debeziumConf.groupID = subConfig.getString("kafka.groupid", "bireme");

        if (debeziumConf.server == null) {
            String message = "Please designate server for " + debeziumConf.name + ".";
            logger.fatal(message);
            throw new BiremeException(message);
        }
    }

    /**
     * Get MaxwellConfig configuration.
     *
     * @param maxwellConf an empty {@code SourceConfig}
     * @throws BiremeException miss some required configuration
     */
    protected void fetchMaxwellConfig(SourceConfig maxwellConf) throws BiremeException {
        String prefix = maxwellConf.name;
        Configuration subConfig = new SubsetConfiguration(config, prefix, ".");

        maxwellConf.type = SourceType.MAXWELL;
        maxwellConf.server = subConfig.getString("kafka.server");
        maxwellConf.topic = subConfig.getString("kafka.topic");
        maxwellConf.groupID = subConfig.getString("kafka.groupid", "bireme");

        if (maxwellConf.server == null) {
            String message = "Please designate server for " + prefix + ".";
            throw new BiremeException(message);
        }

        if (maxwellConf.topic == null) {
            String message = "Please designate topic for " + prefix + ".";
            throw new BiremeException(message);
        }
    }

    private HashMap<String, String> fetchTableMap(String dataSource) throws ConfigurationException, BiremeException {

        Configurations configs = new Configurations();
        Configuration tableConfig;

        tableConfig = configs.properties(new File(DEFAULT_TABLEMAP_DIR + dataSource + ".properties"));

        String originTable, mappedTable;
        HashMap<String, String> localTableMap = new HashMap<>();
        Iterator<String> tables = tableConfig.getKeys();

        while (tables.hasNext()) {
            originTable = tables.next();
            mappedTable = tableConfig.getString(originTable);

            if (originTable.split("\\.").length != 2 || mappedTable.split("\\.").length != 2) {
                String message = "Wrong format: " + originTable + ", " + mappedTable;
                logger.fatal(message);
                throw new BiremeException(message);
            }

            localTableMap.put(dataSource + "." + originTable, mappedTable);

            if (!tableMap.values().contains(mappedTable)) {
                loadersCount++;
            }
            tableMap.put(dataSource + "." + originTable, mappedTable);
        }

        return localTableMap;
    }

    /**
     * Print log about bireme configuration.
     */
    public void logConfig() {
        String config = "Configures: "
                + "\n\tpipeline thread pool size = " + pipelinePoolSize
                + "\n\ttransform thread pool size = " + transformPoolSize + "\n\ttransform queue size = "
                + transformQueueSize + "\n\trow cache size = " + rowCacheSize
                + "\n\tmerge thread pool size = " + mergePoolSize
                + "\n\tmerge interval = " + mergeInterval + "\n\tbatch size = " + batchSize
                + "\n\tloader connection size = " + loaderConnSize + "\n\tloader task queue size = "
                + loaderTaskQueueSize + "\n\tloaders count = " + loadersCount
                + "\n\treporter = " + reporter + "\n\treport interval = " + reportInterval
                + "\n\tstate server addr = " + stateServerAddr + "\n\tstate server port = "
                + stateServerPort + "\n\ttarget database url = " + targetDatabase.jdbcUrl;

        logger.info(config);

        StringBuilder sb = new StringBuilder();
        sb.append("Data Source: \n");
        for (SourceConfig conf : sourceConfig.values()) {
            sb.append("\tType: " + conf.type.name() + " Name: " + conf.name + "\n");
        }

        logger.info(sb.toString());
    }
}
