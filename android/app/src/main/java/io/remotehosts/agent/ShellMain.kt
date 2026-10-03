package io.remotehosts.agent

import android.os.ParcelFileDescriptor
import android.os.Process
import org.json.JSONObject
import java.io.*
import java.security.MessageDigest
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/** Runs ONLY when started by the owner through authorized ADB (uid 2000), or explicitly by root. */
object ShellMain {
    @JvmStatic fun main(args: Array<String>) {
        val uid = Process.myUid()
        System.err.println("RH_HELPER_STARTED uid=$uid")
        if (args.size != 2 || (uid != 2000 && uid != 0) || !args[0].matches(Regex("rh-[0-9]+-[a-f0-9]{32}"))) {
            System.err.println("RH_HELPER_REJECTED invalid_identity_or_arguments"); return
        }
        val appUid = args[1].toIntOrNull() ?: return
        if (appUid < 10000) return
        var descriptor: ParcelFileDescriptor? = null
        var providerConnection: ShellProviderConnection? = null
        var pipeInput: InputStream? = null
        var pipeOutput: OutputStream? = null
        val worker = Executors.newSingleThreadExecutor()
        val active = AtomicReference<java.lang.Process?>()
        val busy = java.util.concurrent.Semaphore(1)
        var stage = "connect"
        try {
            stage = "binder_open"
            val bootstrap = ShellProviderConnection(args[0], appUid)
            providerConnection = bootstrap
            val fd = bootstrap.open()
            descriptor = fd
            pipeInput = ParcelFileDescriptor.AutoCloseInputStream(ParcelFileDescriptor.dup(fd.fileDescriptor))
            pipeOutput = ParcelFileDescriptor.AutoCloseOutputStream(ParcelFileDescriptor.dup(fd.fileDescriptor))
            stage = "handshake"
            val input = DataInputStream(pipeInput)
            val output = DataOutputStream(pipeOutput)
            ShellBridge.writeFrame(output, obj("protocol" to 1, "uid" to uid, "pid" to Process.myPid()))
            stage = "session"
            System.err.println("RH_HELPER_CONNECTED")
            while (true) {
                val request = ShellBridge.readFrame(input)
                val size = request.optLong("stdin_bytes", 0)
                demand(size in 0..Policy.MAX_FILE, "invalid_shell_request_size")
                busy.acquire()
                var staged: File? = null
                try {
                    if (size > 0) {
                        staged = File.createTempFile("rh-stdin-", ".tmp", File("/data/local/tmp"))
                        staged.setReadable(false, false); staged.setReadable(true, true)
                        staged.setWritable(false, false); staged.setWritable(true, true)
                        FileOutputStream(staged).use { sink ->
                            var left = size; val buffer = ByteArray(65536)
                            while (left > 0) {
                                val n = input.read(buffer, 0, minOf(left, buffer.size.toLong()).toInt())
                                demand(n > 0, "incomplete_stdin")
                                sink.write(buffer, 0, n); left -= n
                            }
                        }
                    }
                    val stdin = staged
                    worker.execute {
                        var payload: File? = null
                        try {
                            val response: JSONObject
                            if (request.optString("op") == "pull") {
                                val path = request.getString("path")
                                demand(path.startsWith('/') && !path.contains('\u0000'), "invalid_absolute_path")
                                val source = File(path)
                                demand(source.isFile && source.length() <= Policy.MAX_FILE, "source_unreadable_or_too_large")
                                payload = File.createTempFile("rh-output-", ".tmp", File("/data/local/tmp"))
                                payload!!.setReadable(false, false); payload!!.setReadable(true, true)
                                val digest = MessageDigest.getInstance("SHA-256")
                                var count = 0L
                                source.inputStream().use { src -> FileOutputStream(payload).use { dst ->
                                    val b = ByteArray(65536)
                                    while (true) { val n = src.read(b); if (n < 0) break; count += n; demand(count <= Policy.MAX_FILE, "file_too_large"); digest.update(b, 0, n); dst.write(b, 0, n) }
                                } }
                                response = obj("payload_bytes" to count, "sha256" to digest.digest().joinToString("") { "%02x".format(it) }, "exit_code" to 0)
                            } else {
                                val command = when (request.optString("op")) {
                                    "shell" -> {
                                        val text = request.getString("command")
                                        demand(text.length <= 65536 && !text.contains('\u0000'), "invalid_shell_command")
                                        listOf("/system/bin/sh", "-c", text)
                                    }
                                    "install" -> { demand(size > 0, "empty_apk"); listOf("/system/bin/cmd", "package", "install", "-r", "-S", size.toString()) }
                                    else -> throw AgentError("unsupported_shell_operation")
                                }
                                response = run(command, stdin, request.optInt("timeout_seconds", 30).coerceIn(1, 180), active)
                            }
                            response.put("id", request.getString("id")); response.put("uid", uid)
                            synchronized(output) {
                                ShellBridge.writeFrame(output, response)
                                payload?.inputStream()?.use { it.copyTo(output, 65536) }; output.flush()
                            }
                        } catch (e: Exception) {
                            runCatching { synchronized(output) { ShellBridge.writeFrame(output, Policy.error(e).put("id", request.optString("id"))) } }
                        } finally { stdin?.delete(); payload?.delete(); busy.release() }
                    }
                } catch (e: Exception) { staged?.delete(); throw e }
            }
        } catch (e: Exception) {
            // Sanitized startup diagnosis only: never print commands, tokens,
            // filenames, socket capabilities or screen contents.
            val errno = (e as? android.system.ErrnoException)?.errno ?: (e.cause as? android.system.ErrnoException)?.errno
            System.err.println("RH_HELPER_ENDED stage=$stage type=${e.javaClass.simpleName} errno=${errno ?: -1}")
            if (BuildConfig.DEBUG && stage != "session") {
                val diagnostic = (e.message ?: "").replace(Regex("rh-[0-9]+-[a-f0-9]{32}"), "<activation>").take(600)
                System.err.println("RH_DEBUG_BOOTSTRAP: $diagnostic")
            }
        } finally {
            runCatching { pipeInput?.close() }; runCatching { pipeOutput?.close() }; runCatching { descriptor?.close() }
            active.getAndSet(null)?.destroyForcibly(); worker.shutdownNow()
            runCatching { providerConnection?.close() }
        }
    }
    private fun run(command: List<String>, stdin: File?, seconds: Int, active: AtomicReference<java.lang.Process?>): JSONObject {
        val process = ProcessBuilder(command).redirectErrorStream(true).start()
        active.set(process)
        val bytes = ByteArrayOutputStream()
        val truncated = java.util.concurrent.atomic.AtomicBoolean(false)
        val reader = Thread({
            runCatching { process.inputStream.use { src ->
                val b = ByteArray(8192)
                while (true) {
                    val n = src.read(b); if (n < 0) break
                    synchronized(bytes) { val keep = minOf(n, 65536 - bytes.size()); if (keep > 0) bytes.write(b, 0, keep); if (keep < n) truncated.set(true) }
                }
            } }
        }, "rh-shell-output").apply { isDaemon = true; start() }
        val writer = Thread({ runCatching { process.outputStream.use { dst -> stdin?.inputStream()?.use { it.copyTo(dst, 65536) } } } }, "rh-shell-input").apply { isDaemon = true; start() }
        try {
            val done = process.waitFor(seconds.toLong(), TimeUnit.SECONDS)
            if (!done) { process.destroyForcibly(); process.waitFor(2, TimeUnit.SECONDS) }
            reader.join(1000); writer.join(1000)
            val output = synchronized(bytes) { bytes.toString("UTF-8") }
            return obj("exit_code" to if (done) process.exitValue() else 124, "output" to output,
                "output_truncated" to (truncated.get() || reader.isAlive), "timed_out" to !done,
                "state" to if (done) "exited" else "outcome_unknown", "automatic_replay" to false)
        } finally {
            active.compareAndSet(process, null)
            if (process.isAlive) process.destroyForcibly()
            runCatching { process.inputStream.close() }; runCatching { process.outputStream.close() }
        }
    }
}
