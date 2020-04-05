package twitter.project1

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.lang.Runtime.getRuntime
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


fun createTwitterClient(
    msgQueue: BlockingQueue<String>,
    twitterKeys: Array<String>
): BasicClient? {

    //Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    val hosebirdHosts = HttpHosts(Constants.STREAM_HOST)

    var hosebirdEndpoint = StatusesFilterEndpoint()
    // Optional: set up some followings and track terms
    val terms = listOf<String>("Amazon", "kotlin", "kafka", "Corana", "virus")
    hosebirdEndpoint.trackTerms(terms)

    // These secrets should be read from a config file
    var hosebirdAuth = OAuth1(twitterKeys[0], twitterKeys[1], twitterKeys[2], twitterKeys[3])
    val client = ClientBuilder()
        .hosts(hosebirdHosts)
        .endpoint(hosebirdEndpoint)
        .authentication(hosebirdAuth)
        .processor(StringDelimitedProcessor(msgQueue))
        .build()

    client.connect()

    return client
}

fun createProducter(broker: String): KafkaProducer<String, String> {
    val properties = Properties()
    val bootstrapServers: String = broker
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString())
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
    //safe producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10000")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32")

    return KafkaProducer<String, String>(properties)
}

fun main(args: Array<String>) {
    //create twitter client
    val consumerKey = "8tRJj1lDgn3vIBgNKrAlWcefo"
    val consumerSecret = "FXuo5W61wBjW0X4NCr6svKOvLZMqtdLMfgX7rbH66StmXMmtwh"
    val accessToken = "1104808740300210176-Q8oICjvf4BWS7e7p4ANJS2wcxzaAgU"
    val accessTokenSecret = "0Bb8GH8QowofRXYc5m7srHFOU84stuMvl3866WRCSDH0Y"
    val msgQueue: BlockingQueue<String> = LinkedBlockingQueue(100000)
    var logger = LoggerFactory.getLogger("TwitterLogger")
    val producer = createProducter("mchaine:9092")
    val client = createTwitterClient(
        msgQueue,
        arrayOf(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    )
    getRuntime().addShutdownHook(
        Thread() {
            logger.info("Stopping application ...")
            logger.info("Shuttin dow client for twitter")
            client?.stop()
            logger.info("clossing producter ...")
            producer.close()
            logger.info("Done")

        }
    )

    if (client != null) {
        while (!client.isDone) {
            try {
                val message = msgQueue.poll(5, TimeUnit.SECONDS)
                if (message != null) {
                    producer.send(ProducerRecord("twitter_tweets", null, message), object : Callback {
                        override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
                            if (exception != null) {
                                exception.printStackTrace()
                            }
                        }
                    }
                    )
                }

            } catch (exception: InterruptedException) {
                exception.printStackTrace()
            }
        }
    }
}