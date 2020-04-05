import com.charleskorn.kaml.Yaml
import kotlinx.serialization.Serializable
import java.io.File

@Serializable
data class Config(
    val twitter: TwitterConfigInput,
    val kafka: KafkaConfigInput,
    val elasticsearch:ElasticSearchConfigInput
)

@Serializable
data class TwitterConfigInput(
    val key: String,
    val secret: String,
    val token: String,
    val tokenSecret: String
)
@Serializable
data class ZookeeperConfigInput(val host: String, val port: String)

@Serializable
data class ProducerConfigInput(
    val brokers: String,
    val topic: String = "default_topic",
    val compression: String = "snappy",
    val batchSize: Int = 32000,
    val idempotent: Boolean = true,
    val retries: Int = 1,
    val acks: String = "all",
    val lingerMs:String = "5"
)

@Serializable
data class ConsumerConfigInput(
    val topic: String,
    val group: String,
    val bootstrapServer: String,
    val maxPollRecords:String = "1000"
)
@Serializable
data class KafkaConfigInput (
    val zookeeper: ZookeeperConfigInput,
    val producer: ProducerConfigInput,
    val consumer: ConsumerConfigInput
)

@Serializable
data class ElasticSearchConfigInput(
    val url: String,
    val key: String?,
    val secret: String?
)

private val configuration = File("configurations.yaml").readText()
val config = Yaml.default.parse(Config.serializer(), configuration)
