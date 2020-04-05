
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

fun main(args: Array<String>) {
    val props = Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.consumer.bootstrapServer)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.consumer.maxPollRecords)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.consumer.group)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    //create consumer
    val consumer = KafkaConsumer<String, String>(props)
    consumer.subscribe(listOf("firstTopic"))

    while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        /*for (record in records) {
            println("Key=${record.key()}, value = ${record.value()}")
        }*/
        consumer.commitAsync();
    }
    consumer.close()

}