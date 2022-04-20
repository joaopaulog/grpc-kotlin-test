import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import test_bidi_stream.BidiStream
import test_bidi_stream.RPCBidiStreamGrpcKt
import test_bidi_stream.res

fun main() = runBlocking {
    println("Server start")

    val service = BidiServer()

    val port = 50051
    val server: Server = ServerBuilder
        .forPort(port)
        .addService(service)
        .build()

    Runtime.getRuntime().addShutdownHook(Thread { server.shutdown() })
    server.start()
    server.awaitTermination()
}

class BidiServer : RPCBidiStreamGrpcKt.RPCBidiStreamCoroutineImplBase() {

    override fun go(requests: Flow<BidiStream.Req>): Flow<BidiStream.Res> {
        return requests.map { res {
            id = it.id
            r = it.s.substring(it.i until it.i + it.c).repeat(it.n)
        } }
    }
}