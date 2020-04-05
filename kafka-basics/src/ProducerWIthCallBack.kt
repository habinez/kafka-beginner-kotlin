import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory

fun main() {
    val producer = createProducer(config)
    val logger = LoggerFactory.getLogger("ProducerDemoWithCallBack")

    for (i in 0..10) {
        val fakePerson = FakePerson().jsonify()
        producer.send(
            ProducerRecord(config.consumer.topic, fakePerson), object : Callback {
                override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
                    if (exception == null) {
                        logger.info(
                            "Received new metadata :: \t",
                            "Topic: ${metadata?.topic()} \t",
                            "Partition: ${metadata?.partition()}\t",
                            "Offset: ${metadata?.offset()}\t",
                            "Timestamp: ${metadata?.timestamp()}"
                        )
                    } else {
                        logger.error(exception.message)
                    }
                }
            }
        )
        producer.flush()
    }
    producer.close()
}