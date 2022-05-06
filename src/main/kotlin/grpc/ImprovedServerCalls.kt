package grpc

import io.grpc.*
import io.grpc.kotlin.GrpcContextElement
import io.grpc.kotlin.ServerCalls.clientStreamingServerMethodDefinition
import io.grpc.kotlin.ServerCalls.serverStreamingServerMethodDefinition
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

object ImprovedServerCalls {

    /**
     * Creates a [ServerMethodDefinition] that implements the specified bidirectional-streaming RPC
     * method by running the specified implementation and associated implementation details within a
     * per-RPC [CoroutineScope] generated with the specified [CoroutineContext].
     *
     * When the RPC is received, this method definition will pass a [Flow] of requests from the client
     * to [implementation], and collect the returned [Flow], sending responses to the client as they
     * are emitted.
     *
     * Exceptions and cancellation are handled as in [clientStreamingServerMethodDefinition] and as
     * in [serverStreamingServerMethodDefinition].
     *
     * @param context The context of the scopes the RPC implementation will run in
     * @param descriptor The descriptor of the method being implemented
     * @param implementation The implementation of the RPC method
     */
    fun <RequestT, ResponseT> bidiStreamingServerMethodDefinition(
        context: CoroutineContext,
        descriptor: MethodDescriptor<RequestT, ResponseT>,
        implementation: (requests: Flow<RequestT>) -> Flow<ResponseT>
    ): ServerMethodDefinition<RequestT, ResponseT> {
        require(descriptor.type == MethodDescriptor.MethodType.BIDI_STREAMING) {
            "Expected a bidi streaming method descriptor but got $descriptor"
        }
        return serverMethodDefinition(context, descriptor, implementation)
    }

    /**
     * Builds a [ServerMethodDefinition] that implements the specified RPC method by running the
     * specified channel-based implementation within the specified [CoroutineScope] (and/or a
     * subscope).
     */
    private fun <RequestT, ResponseT> serverMethodDefinition(
        context: CoroutineContext,
        descriptor: MethodDescriptor<RequestT, ResponseT>,
        implementation: (Flow<RequestT>) -> Flow<ResponseT>
    ): ServerMethodDefinition<RequestT, ResponseT> =
        ServerMethodDefinition.create(
            descriptor,
            serverCallHandler(context, implementation)
        )

    /**
     * Returns a [ServerCallHandler] that implements an RPC method by running the specified
     * channel-based implementation within the specified [CoroutineScope] (and/or a subscope).
     */
    private fun <RequestT, ResponseT> serverCallHandler(
        context: CoroutineContext,
        implementation: (Flow<RequestT>) -> Flow<ResponseT>
    ): ServerCallHandler<RequestT, ResponseT> =
        ServerCallHandler {
                call, _ ->
            improvedServerCallListener(
                context
//                        + CoroutineContextServerInterceptor.COROUTINE_CONTEXT_KEY.get() // CHANGE: TODO: Not necessary right now
                        + GrpcContextElement.current(),
                call,
                implementation
            )
        }

    private fun <RequestT, ResponseT> improvedServerCallListener(
        context: CoroutineContext,
        call: ServerCall<RequestT, ResponseT>,
        implementation: (Flow<RequestT>) -> Flow<ResponseT>
    ): ServerCall.Listener<RequestT> {

        val requestsChannel = Channel<RequestT>(Channel.UNLIMITED)
        val requestFlow = createRequestsFlow(call, requestsChannel)

        val readinessSignal = Readiness { call.isReady }
        val requestProcessingJob = CoroutineScope(context).launch {

            val mutex = Mutex()

            val headersSent = AtomicBoolean(false)
            suspend fun sendHeaders() {
                if (headersSent.compareAndSet(false, true)) {
                    mutex.withLock { call.sendHeaders(Metadata()) }
                }
            }

            val failure = runCatching {
                implementation(requestFlow)
                    .buffer() // <------ IMPROVEMENT. Separate the coroutine of running the request processing, from the actual sending the response.
                    .collect {
                        sendHeaders()
                        readinessSignal.suspendUntilReady()
                        mutex.withLock { call.sendMessage(it) }
                    }
            }.exceptionOrNull()

            sendHeaders()

            val closeStatus = when (failure) {
                null -> Status.OK
                is CancellationException -> Status.CANCELLED.withCause(failure)
                else -> Status.fromThrowable(failure)
            }
            val trailers = failure?.let { Status.trailersFromThrowable(it) } ?: Metadata()

            mutex.withLock { call.close(closeStatus, trailers) }
        }

        return StreamServerCallListener(
            call,
            requestProcessingJob,
            requestsChannel,
            readinessSignal
        )
    }

    private fun <RequestT, ResponseT> createRequestsFlow(call: ServerCall<RequestT, ResponseT>, requestsChannel: ReceiveChannel<RequestT>): Flow<RequestT> {
        val requestsStarted = AtomicBoolean(false)
        return flow {
            check(requestsStarted.compareAndSet(false, true)) { "requests flow can only be collected once" }

            call.request(10)
            try {
                for (request in requestsChannel) {
                    emit(request)
                    call.request(10)
                }
            } catch (e: Exception) {
                requestsChannel.cancel(CancellationException("Exception thrown while collecting requests", e))
                call.request(1)
                throw e
            }
        }
    }

    private class StreamServerCallListener<RequestT, ResponseT>(
        private val call: ServerCall<RequestT, ResponseT>,
        private val requestProcessingJob: Job,
        private val requestsChannel: SendChannel<RequestT>,
        private val readinessSignal: Readiness,
    ) : ServerCall.Listener<RequestT>() {

        private var isReceiving : Boolean = true

        override fun onCancel() {
            requestProcessingJob.cancel("Cancellation received from client")
        }

        override fun onMessage(message: RequestT) {
            if (isReceiving) {
                requestsChannel.trySend(message)
                    .onClosed { isReceiving = false }
                    .onFailure {
                        throw Status.INTERNAL
                            .withDescription("onMessage should never be called when requestsChannel is unready")
                            .asException()
                    }
            }
            if (!isReceiving) {
                call.request(1)
            }
        }

        override fun onHalfClose() {
            requestsChannel.close()
        }

        override fun onReady() {
            readinessSignal.onReady()
        }
    }

    private class Readiness(private val isReallyReady: () -> Boolean) {
        private val channel = Channel<Unit>(Channel.CONFLATED)

        fun onReady() {
            channel.trySend(Unit)
                .onFailure { throw AssertionError("Should be impossible; a CONFLATED channel should never return false on offer") }
        }

        suspend fun suspendUntilReady() {
            while (!isReallyReady()) {
                channel.receive()
            }
        }
    }

}