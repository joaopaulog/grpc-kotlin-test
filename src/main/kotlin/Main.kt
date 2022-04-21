import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.int
import kotlinx.coroutines.runBlocking

enum class ExecutionMode { CLIENT, SERVER }

class Main : CliktCommand() {

    private val mode: ExecutionMode by option(help = "Execution mode")
        .switch(
            "--server" to  ExecutionMode.SERVER,
            "--client" to ExecutionMode.CLIENT
        ).required()

    private val port: Int by option("--port", "-p", help = "Port to use")
        .int()
        .default(50051)

    override fun run() = runBlocking {
        when (mode) {
            ExecutionMode.CLIENT -> Client().run(port)
            ExecutionMode.SERVER -> Server().run(port)
        }
    }
}

fun main(args: Array<String>) {
    Main().main(args)
}