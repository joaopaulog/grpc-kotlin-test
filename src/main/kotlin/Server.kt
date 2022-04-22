import grpc.ServerLogInterceptor
import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import test_bidi_stream.BidiStream
import test_bidi_stream.RPCBidiStreamGrpcKt
import test_bidi_stream.res
import java.lang.Integer.max
import java.lang.Integer.min

class Server {
    fun run(port: Int) {
        println("Server start")

        val service = BidiServer()

        val server: Server = ServerBuilder
            .forPort(port)
            .intercept(ServerLogInterceptor())
            .addService(service)
            .build()

        Runtime.getRuntime().addShutdownHook(Thread { server.shutdown() })
        server.start()
        server.awaitTermination()
    }
}

class BidiServer : RPCBidiStreamGrpcKt.RPCBidiStreamCoroutineImplBase() {

    override fun go(requests: Flow<BidiStream.Req>): Flow<BidiStream.Res> {
        println("Created a stream.")
        return requests
            .map {
                res {
                    id = it.id
                    r = it.s
                        .substring(max(0, it.i) until min(it.i + it.c, it.s.length))
                        .repeat(max(1, it.n))
                }
            }
            .onCompletion {
                println("Closing a stream.")
            }
    }
}
