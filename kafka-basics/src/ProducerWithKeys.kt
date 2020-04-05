import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {
    val producer = createProducer(config)
    val logger = LoggerFactory.getLogger("ProducerDemoWithKey")
    val fakePerson = FakePerson().jsonify()
    for (i in 0..10) {
        val fakePerson = FakePerson().toString()
        val key = "id_" + i.toString()
        //logger.info(fakePerson)
        producer.send(
            ProducerRecord("firstTopic", key, fakePerson)
        ) { metadata, exception ->
            if (exception == null) {
                logger.info(
                    "Received new metadata :: \t" +
                            "Topic: ${metadata.topic()} \t" +
                            "Partition: ${metadata.partition()}\t" +
                            "Offset: ${metadata.offset()}\t" +
                            "Timestamp: ${metadata.timestamp()} \t" +
                            "key: $key"
                )
            } else {
                logger.error(exception.message)
            }
        }.get() //block the .send() to make it synchronous -- dont do it in production

    }
    producer.flush()
    producer.close()

}