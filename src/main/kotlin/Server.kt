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
    fun run(port: Int, verbose: Boolean) {
        println("Server start")

        var serverBuilder = ServerBuilder
            .forPort(port)

        if (verbose) {
            serverBuilder = serverBuilder.intercept(ServerLogInterceptor())
        }

        val server: Server = serverBuilder
            .addService(BidiServer())
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
                        .substring(max(0, it.i) until min(max(0, it.i) + max(1, it.c), it.s.length))
                        .repeat(max(1, it.n))
                }
            }
            .onCompletion {
                println("Closing a stream.")
            }
    }
}
