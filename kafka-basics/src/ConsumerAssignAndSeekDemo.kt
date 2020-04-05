import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import org.apache.kafka.common.TopicPartition
import java.util.*

fun main(args: Array<String>) {
    val props = Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.consumer.topic)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.consumer.maxPollRecords)
    //create consumer
    val consumer = KafkaConsumer<String, String>(props)
    //assign
    val partition:TopicPartition = TopicPartition(config.consumer.topic, 0)
    val offsetToReadFrom = 15L
    var messageRead = 0
    val totalMessages = 5
    var continueReading = true
    consumer.assign(listOf(partition))
    consumer.seek(partition, offsetToReadFrom)

    while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        messageRead += 1
        for (record in records) {
            println("Key=${record.key()}, value = ${record.value()}")
        }
        if (messageRead >= totalMessages){
            continueReading = false
            break
        }

    }
    println("total Message=$totalMessages, message read = $messageRead")

    consumer.close()
}