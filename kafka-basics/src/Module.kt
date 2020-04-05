import com.charleskorn.kaml.Yaml
import kotlinx.serialization.Serializable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.javafaker.Faker
import java.io.File
import java.util.*

data class FakePerson(
    var firstName: String?,
    var lastName: String?,
    var birthDate: Date?,
    var company: String?,
    var occupation: String?

) {
    init {
        firstName = firstName ?: Faker().name().firstName()
        lastName = lastName ?: Faker().name().lastName()
        birthDate = birthDate ?: Faker().date().birthday()
        company = company ?: Faker().company().name()
        occupation = occupation ?: Faker().job().title()
    }
    constructor() : this(null, null, null, null, null)

    override fun toString(): String {
        return jsonify()
    }
    fun jsonify(): String {
        // inspired by https://aseigneurin.github.io/2018/08/01/kafka-tutorial-1-simple-producer-in-kotlin.html
        return ObjectMapper()
            .apply {
                registerKotlinModule()
                disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                setDateFormat(StdDateFormat())
            }
            .writeValueAsString(FakePerson())
    }
}
@Serializable
data class KafkaConfigInput (
    val producer: ProducerConfigInput,
    val consumer: ConsumerConfigInput
)


@Serializable
data class ProducerConfigInput(
    val brokers: String,
    val topic: String = "default_topic",
    val compression: String = "snappy",
    val batchSize: Int = 32000,
    val idempotent: Boolean = true,
    val retries: String = "1",
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

val config = Yaml.default.parse(KafkaConfigInput.serializer(), File("configurations.yaml").readText())