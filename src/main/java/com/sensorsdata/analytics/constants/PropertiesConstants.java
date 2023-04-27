package com.sensorsdata.analytics.constants;

/**
 * 所有全局常量
 * @author igarashi233
 **/
public class PropertiesConstants {

    PropertiesConstants() {
    }

    public static final String MAIN_PROCESS_PARALLELISM = "stream.process.parallelism";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";
    public static final String ENABLE_INCREMENTAL_CHECKPOINTING = "enable.incremental.checkpointing";

    //checkpoint properties
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String CHECKPOINT_TIME_OUT = "stream.checkpoint.time.out";
    public static final String CHECKPOINT_PAUSE_BETWEEN_CHECKPOINTS = "stream.checkpoint.min.pause.between.checkpoints";
    public static final String CHECKPOINT_MAX_CONCURRENT = "stream.checkpoint.max.concurrent.checkpoints";
    public static final String ENABLE_UNALIGNED_CHECKPOINTS = "stream.checkpoint.enable.unalignedcheckpoints";
    public static final String OFFSET_CHECKPOINT_PARALLELISM = "stream.offset.checkpoint.parallelism";

    public static final String CHECKPOINT_MEMORY = "memory";
    public static final String CHECKPOINT_FS = "fs";
    public static final String CHECKPOINT_ROCKETSDB = "rocksdb";

    // Kafka Consumer Config
    public static final String AUTO_OFFSET_RESET = "kafka.consumer.auto.offset.reset";
    public static final String FETCH_MAX_WAIT_MS_CONFIG = "kafka.consumer.fetch.max.wait.ms";
    public static final String FETCH_MAX_BYTES_CONFIG = "kafka.consumer.fetch.max.bytes";
    public static final String FETCH_MIN_BYTES_CONFIG = "kafka.consumer.fetch.min.bytes";
    public static final String CONSUMER_REQUEST_TIMEOUT_MS_CONFIG = "kafka.consumer.request.timeout.ms";
    public static final String CONSUMER_SECURITY_PROTOCOL = "kafka.consumer.security.protocol";
    public static final String CONSUMER_SASL_MECHANISM = "kafka.consumer.sasl.mechanism";

    //Kafka ProducerConfig
    public static final String TRANSACTION_TIMEOUT = "kafka.producer.transaction.time.out";
    public static final String BATCH_SIZE_CONFIG = "kafka.producer.batch.size.config";
    public static final String MAX_BLOCK_MS_CONFIG = "kafka.producer.max.block.ms";
    public static final String LINGER_MS_CONFIG = "kafka.producer.linger.ms";
    public static final String BUFFER_MEMORY_CONFIG = "kafka.producer.buffer.memory";
    public static final String COMPRESSION_TYPE_CONFIG = "kafka.producer.compression.type";
    public static final String PRODUCER_TOPIC = "kafka.producer.topic";
    public static final String INVALID_PRODUCER_TOPIC = "kafka.producer.invalid.topic";
    public static final String ENABLE_IDEMPOTENCE_CONFIG = "kafka.producer.enable.idempotence";
    public static final String PRODUCER_KAFKA_BROKERS = "kafka.producer.brokers";
    public static final String PRODUCER_REQUEST_TIMEOUT_MS_CONFIG = "kafka.producer.request.timeout.ms";
    public static final String PRODUCER_SECURITY_PROTOCOL = "kafka.producer.security.protocol";
    public static final String PRODUCER_SASL_MECHANISM = "kafka.producer.sasl.mechanism";
    public static final String PRODUCER_SASL_JAAS_CONFIG = "kafka.producer.sasl.jaas.config";
    public static final String PRODUCER_GROUP = "kafka.producer.group.id";

    //mysql
    public static final String MYSQL_JDBC_URL = "mysql.jdbc.url";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_USERNAME = "mysql.username";

    //error tag and reason
    public static final String ERROR_REASON = "ERROR_REASON";
    public static final String VERSION_TAG = "VERSION_TAG";

    //metadata key list
    public static final String TOPIC_PROJECT_MAP = "projectTopicMapping";
    public static final String PROPERTY_MAP = "propertyMapping";
    public static final String COUNTER = "counterList";
    public static final String SINK_PROJECT_MAP = "sinkProjectMapping";

    //record kafka related properties
    public static final String RECORD_TOPIC = "SACONSUMERTOPIC";
    public static final String RECORD_PARTITION = "SACONSUMERPARTITION";
    public static final String RECORD_OFFSET = "SACONSUMEROFFSET";
    public static final String RECORD_SERVER = "SACONSUMERSERVER";
    public static final String RECORD_GROUP = "SACONSUMERGROUP";
    public static final String RECORD_KEY = "RECORD_KEY";

    //field name
    public static final String PROJECT = "project";
}
