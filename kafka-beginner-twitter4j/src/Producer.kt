
import twitter4j.Query
import twitter4j.Twitter
import twitter4j.TwitterException
import twitter4j.TwitterFactory
import twitter4j.conf.ConfigurationBuilder

import com.charleskorn.kaml.Yaml
import kotlinx.serialization.Serializable
import java.io.File

@Serializable
data class MyTwitterConfig(
    val key: String,
    val secret: String,
    val token: String,
    val tokenSecret: String
)
private fun createClient(): Twitter? {
    val clientBuilder = ConfigurationBuilder()
    val configuration = File("twitter_config.yaml").readText()
    val config = Yaml.default.parse(MyTwitterConfig.serializer(), configuration)
    clientBuilder.setDebugEnabled(true)
        .setOAuthConsumerKey(config.key)
        .setOAuthConsumerSecret(config.secret)
        .setOAuthAccessToken(config.token)
        .setOAuthAccessTokenSecret(config.tokenSecret)
    return TwitterFactory(clientBuilder.build()).instance
}


fun main(args:Array<String>){
    var logger = org.slf4j.LoggerFactory.getLogger("TwitterProducer")
    val twitterClient = createClient()
    try {
        val query = Query("google")
        val result = twitterClient?.search(query)
        if (result != null) {
            for (status in result.tweets) {
                //println("@ ${status.user.screenName.toString()}: ${status.text}")
                //println(status.user.screenName)
                println(status.text)
            }
        }
    } catch ( exception: TwitterException){
        exception.printStackTrace()
    }

}