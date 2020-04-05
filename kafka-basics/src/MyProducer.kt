package kafkaProgramming101

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class MyProducer {
    companion object {
        fun createProducter(broker: String = "localhost:9092"): KafkaProducer<String, String> {
            val properties = Properties()
            val bootstrapServers: String = broker
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            properties.setProperty("acks", "all")
            properties.setProperty("retries", "1")
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)

            return KafkaProducer<String, String>(properties)
        }
    }
}