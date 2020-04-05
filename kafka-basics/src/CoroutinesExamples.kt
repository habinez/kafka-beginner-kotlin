import kotlinx.coroutines.*
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    val time  = measureTimeMillis {
        synchronous()
    }
}

suspend fun longCalc(startNum: Int): Int {
    delay(1000)
    return startNum + 1
}
fun synchronous() = runBlocking {
    var x1 = longCalc(100)
    var x2 = longCalc(200)
    var x3 = longCalc(300)
    var x4 = longCalc(400)
    var sum = listOf(x1, x2, x3,x4).sum()
    println("results = $sum")


}