import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*


fun createProducer(config: KafkaConfigInput): KafkaProducer<String, String> {
    val properties = Properties()
    val bootstrapServers: String = config.producer.brokers
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.ACKS_CONFIG, config.producer.acks)
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, config.producer.retries)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)

    return KafkaProducer<String, String>(properties)
}

fun main(args: Array<String>) {
    val producer = createProducer(config)
    for (i in 0..1000) {
        val fakePerson = FakePerson().jsonify()
        producer.send(ProducerRecord(config.producer.topic, fakePerson))
        producer.flush()
    }
    producer.close()
}

