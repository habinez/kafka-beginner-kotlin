import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URI
import java.time.Duration
import java.util.*


fun createElasticClient(config: ElasticSearchConfigInput): RestHighLevelClient {

    val uri = URI.create(config.url)
    val credentialsProvider: CredentialsProvider = BasicCredentialsProvider()
    if (uri.userInfo!= null) {
        val auth = uri.userInfo.split(':')
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            UsernamePasswordCredentials(auth[0], auth[1])
        )
    } else if (config.key != null && config.secret != null) {
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            UsernamePasswordCredentials(config.key, config.secret)
        )
    }

    val builder: RestClientBuilder = RestClient.builder(
        HttpHost(uri.host, uri.port, uri.scheme)
    )
        .setHttpClientConfigCallback(HttpClientConfigCallback { httpClientBuilder ->
            httpClientBuilder.setDefaultCredentialsProvider(
                credentialsProvider
            )
        })

    return RestHighLevelClient(builder)
}

fun createConsumer(config: ConsumerConfigInput): KafkaConsumer<String, String> {
    val props = Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxPollRecords)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.group)
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets
    val consumer = KafkaConsumer<String, String>(props)
    consumer.subscribe(listOf(config.topic))
    return consumer
}

fun main(args: Array<String>) {
    val logger: Logger = LoggerFactory.getLogger("ElasticSearchConsumer")
    val consumer = createConsumer(config.kafka.consumer)
    val esClient = createElasticClient(config.elasticsearch)
    while (true) {
        val records = consumer.poll(Duration.ofSeconds(60))
        val recordCount = records.count()
        logger.info("Received $recordCount records")

        val bulkRequest = BulkRequest()
        for (record in records) {
            try {
                val id = JsonParser().parse(record?.value())
                    .asJsonObject
                    .get("id_str")
                    .asString
                val indexRequest = IndexRequest("Tweets").id(id).source(record.value(), XContentType.JSON)
                bulkRequest.add(indexRequest)

            } catch (exception: IllegalStateException) {

                exception.printStackTrace()
            }
        }
        if (recordCount > 0) {
            logger.info("Received $recordCount records")
            val bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
            logger.info("Committing offsets ... ")
            consumer.commitSync()
            try {
                Thread.sleep(1000)
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }
    }
    consumer.close()
}