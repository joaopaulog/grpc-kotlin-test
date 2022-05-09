package grpc

import io.grpc.*
import io.grpc.internal.ServerStream

class BufferedResponseInterceptor : ServerInterceptor {

    override fun <ReqT : Any?, RespT : Any?> interceptCall(call: ServerCall<ReqT, RespT>?, headers: Metadata?, next: ServerCallHandler<ReqT, RespT>?): ServerCall.Listener<ReqT> {
        return next!!.startCall(BufferedServerCall(call!!), headers)
    }

    private class BufferedServerCall<ReqT, RespT>(delegate: ServerCall<ReqT, RespT>) : ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(delegate) {

        companion object {
            private const val BUFFER_SIZE = 100 
        }

        private val stream : ServerStream
        private var counter = 0

        init {
            val field = delegate::class.java.getDeclaredField("stream")
            field.trySetAccessible()
            stream = field.get(delegate) as ServerStream
        }

        override fun sendMessage(message: RespT) {
            if (counter == BUFFER_SIZE) {
                stream.flush()
                counter = 0
            }

            counter++
            val input = methodDescriptor.streamResponse(message)
            stream.writeMessage(input)
        }

        override fun close(status: Status?, trailers: Metadata?) {
            sendMessages()
            super.close(status, trailers)
        }

        private fun sendMessages() {
            stream.flush()
        }

    }
}

