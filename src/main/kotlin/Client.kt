import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import test_bidi_stream.RPCBidiStreamGrpcKt
import test_bidi_stream.req
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.random.nextInt

fun main() = runBlocking {
    println("Client start")

    val channel = ManagedChannelBuilder
        .forAddress("localhost", 50051)
        .usePlaintext()
        .keepAliveTime(10, TimeUnit.MINUTES)
        .keepAliveWithoutCalls(true)
        .build()

    val requestsFlow = flow {
        val source = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

        var requestId = 0L
        while (true) {
            val size = Random.nextInt(1 .. source.length)
            val startIndex = if (size == source.length) 0 else Random.nextInt(source.length - size)
            val iterations =  Random.nextInt(1..50)

            emit(req {
                id = ++requestId
                s = source
                i = startIndex
                c = size
                n = iterations
            })
            delay(50)
        }
    }

    val stub = RPCBidiStreamGrpcKt.RPCBidiStreamCoroutineStub(channel)
    stub.go(requestsFlow).collect {
        println("${it.id} -> ${it.r}")
    }
}