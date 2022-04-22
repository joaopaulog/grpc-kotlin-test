package grpc

import com.google.protobuf.Message
import com.google.protobuf.TextFormat
import io.grpc.*
import java.util.*

class ServerLogInterceptor : ServerInterceptor {

    companion object {
        fun toString(message: Message): String =
            "${message.descriptorForType.name}: ${TextFormat.shortDebugString(message).ifBlank { "<empty>" }}"
    }

    override fun <ReqT : Any?, RespT : Any?> interceptCall(call: ServerCall<ReqT, RespT>?, headers: Metadata?, next: ServerCallHandler<ReqT, RespT>?): ServerCall.Listener<ReqT> {
        val callId = UUID.randomUUID()

        println("gRPC Call ${call!!.methodDescriptor.fullMethodName} [Id: $callId] - Start")
        val logCall = ServerLogForwardingCall(callId, call)
        return ServerLogCallListener(callId, call.methodDescriptor, next!!.startCall(logCall, headers))
    }

    private class ServerLogForwardingCall<ReqT, RespT>(private val callId: UUID, delegate: ServerCall<ReqT, RespT>?) : ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(delegate) {

        override fun sendMessage(message: RespT) {
            if (message is Message) {
                println("gRPC Call ${methodDescriptor.fullMethodName} [Id: $callId] - Sent message ${toString(message)}")
            }
            super.sendMessage(message)
        }

        override fun close(status: Status?, trailers: Metadata?) {
            if (status != null && !status.isOk) {
                println("gRPC Call ${methodDescriptor.fullMethodName} [Id: $callId] - Error response: $status")
            }
            super.close(status, trailers)
        }
    }

    private class ServerLogCallListener<ReqT, RespT>(private val callId: UUID, private val methodDescriptor: MethodDescriptor<ReqT, RespT>, delegate: ServerCall.Listener<ReqT>?) : ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(delegate) {

        override fun onCancel() {
            println("gRPC Call ${methodDescriptor.fullMethodName} [Id: $callId] - Canceled")
            super.onCancel()
        }

        override fun onComplete() {
            println("gRPC Call ${methodDescriptor.fullMethodName} [Id: $callId] - Completed")
            super.onComplete()
        }

        override fun onMessage(message: ReqT) {
            if (message is Message) {
                println("gRPC Call ${methodDescriptor.fullMethodName} [Id: $callId] - Received message ${toString(message)}")
            }
            super.onMessage(message)
        }

        override fun onHalfClose() {
            try {
                super.onHalfClose()
            } catch (exception: Exception) {
                handleException(exception)
                throw exception
            }
        }

        override fun onReady() {
            try {
                super.onReady()
            } catch (exception: Exception) {
                handleException(exception)
                throw exception
            }
        }

        private fun handleException(exception: Exception) {
            println("gRPC Call ${methodDescriptor.fullMethodName} [Id: $callId] - Exception: $exception")
        }
    }
}
