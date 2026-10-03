package io.remotehosts.agent

import android.content.Context
import android.os.ParcelFileDescriptor
import android.os.Process
import android.system.Os
import android.system.OsConstants
import android.system.StructTimeval
import org.json.JSONObject
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.io.FileOutputStream
import java.security.MessageDigest

/** Binder authenticates the caller before transferring an unnamed socket FD.
 * There is no TCP listener, named socket, shared credential file, or SELinux change. */
class ShellBridge(private val context: Context) {
    private class Channel(val descriptor: ParcelFileDescriptor, val peerUid: Int) {
        val input = DataInputStream(ParcelFileDescriptor.AutoCloseInputStream(ParcelFileDescriptor.dup(descriptor.fileDescriptor)))
        val output = DataOutputStream(ParcelFileDescriptor.AutoCloseOutputStream(ParcelFileDescriptor.dup(descriptor.fileDescriptor)))
        @Volatile var authenticated = false
        @Volatile var closed = false
        fun timeout(milliseconds: Long) {
            val value = StructTimeval.fromMillis(milliseconds)
            Os.setsockoptTimeval(descriptor.fileDescriptor, OsConstants.SOL_SOCKET, OsConstants.SO_RCVTIMEO, value)
            Os.setsockoptTimeval(descriptor.fileDescriptor, OsConstants.SOL_SOCKET, OsConstants.SO_SNDTIMEO, value)
        }
        @Synchronized fun close() {
            if (closed) return
            closed = true; authenticated = false
            // shutdown, not just close: wake reads in other threads immediately.
            runCatching { Os.shutdown(descriptor.fileDescriptor, OsConstants.SHUT_RDWR) }
            runCatching { input.close() }; runCatching { output.close() }; runCatching { descriptor.close() }
        }
    }
    @Volatile private var channel: Channel? = null
    @Volatile var socketName: String? = null; private set
    @Volatile var uid: Int? = null; private set
    private val commandLock = Any()
    @Synchronized fun open() {
        if (socketName != null) return
        demand(controls(context).getBoolean("shell_enabled", false), "shell_not_enabled_locally")
        socketName = "rh-${Process.myUid()}-${nonce().take(32)}"
    }
    /** Called only by BridgeProvider after a real Binder UID check. */
    @Synchronized fun attach(name: String, peerUid: Int): ParcelFileDescriptor {
        demand(peerUid == 2000 || peerUid == 0, "shell_peer_uid_rejected")
        demand(controls(context).getBoolean("shell_enabled", false), "shell_not_enabled_locally")
        demand(name == socketName && socketName != null, "activation_expired")
        demand(channel == null, "helper_already_attached")
        val pair = ParcelFileDescriptor.createSocketPair()
        val candidate = try { Channel(pair[0], peerUid).also { it.timeout(10000) } }
            catch (e: Exception) { pair.forEach { runCatching { it.close() } }; throw e }
        channel = candidate
        Thread({
            try {
                val hello = readFrame(candidate.input)
                demand(hello.optInt("protocol") == 1 && hello.optInt("uid", -1) == peerUid, "shell_handshake_rejected")
                synchronized(this@ShellBridge) {
                    demand(channel === candidate && socketName == name && controls(context).getBoolean("shell_enabled", false), "activation_revoked")
                    candidate.authenticated = true; uid = peerUid
                }
                AgentRuntime.changed()
            } catch (_: Exception) {
                discard(candidate)
            }
        }, "rh-shell-handshake").apply { isDaemon = true; start() }
        return pair[1]
    }
    @Synchronized private fun discard(candidate: Channel) {
        if (channel === candidate) {
            channel = null; uid = null; socketName = null
            if (controls(context).getBoolean("shell_enabled", false)) open()
        }
        candidate.close(); AgentRuntime.changed()
    }
    fun connected(): Boolean = channel?.let { it.authenticated && !it.closed } == true
    fun execute(request: JSONObject, inputFile: File? = null, outputFile: File? = null): JSONObject = synchronized(commandLock) {
        demand(controls(context).getBoolean("shell_enabled", false), "shell_not_enabled_locally")
        val current = channel?.takeIf { it.authenticated && !it.closed }
            ?: throw AgentError("shell_activation_required", "在手机允许 shell 后，通过已授权的 ADB 激活内置辅助进程。")
        val ident = nonce().take(24)
        request.put("id", ident)
        val timeout = request.optInt("timeout_seconds", 30).coerceIn(1, 180)
        request.put("timeout_seconds", timeout)
        val size = inputFile?.length() ?: 0
        demand(size <= Policy.MAX_FILE, "file_too_large")
        request.put("stdin_bytes", size)
        try {
            current.timeout((timeout + 15L) * 1000)
            val out = current.output
            writeFrame(out, request)
            inputFile?.inputStream()?.use { it.copyTo(out, 65536) }
            out.flush()
            val incoming = current.input
            val result = readFrame(incoming)
            demand(result.optString("id") == ident, "shell_response_identity_mismatch")
            val payload = result.optLong("payload_bytes", 0)
            demand(payload in 0..Policy.MAX_FILE && (payload == 0L || outputFile != null), "invalid_shell_payload")
            if (payload > 0 || (outputFile != null && result.has("payload_bytes"))) {
                val file = outputFile ?: throw AgentError("missing_output_file")
                val digest = MessageDigest.getInstance("SHA-256")
                FileOutputStream(file).use { sink ->
                    val buffer = ByteArray(65536)
                    var left = payload
                    while (left > 0) {
                        val n = incoming.read(buffer, 0, minOf(buffer.size.toLong(), left).toInt())
                        demand(n > 0, "incomplete_shell_payload")
                        sink.write(buffer, 0, n); digest.update(buffer, 0, n); left -= n
                    }
                    sink.fd.sync()
                }
                demand(digest.digest().joinToString("") { "%02x".format(it) } == result.getString("sha256"), "shell_payload_checksum_mismatch")
            }
            result
        } catch (e: Exception) {
            discard(current); outputFile?.delete()
            if (e is AgentError) throw e
            throw AgentError("shell_connection_lost_outcome_unknown", "shell 连接中断；操作不会自动重复，请先检查手机当前状态。")
        }
    }
    /** Deliberately not under commandLock: Stop interrupts in-flight reads. */
    @Synchronized fun close() {
        socketName = null; uid = null
        val previous = channel; channel = null
        previous?.close(); AgentRuntime.changed()
    }
    fun activationCommand(): String {
        val name = socketName ?: throw AgentError("start_service_and_enable_shell_first")
        val p = context.packageName
        return "adb shell 'CLASSPATH=\$(pm path $p | sed -n \"s/^package://p\" | head -n 1) nohup app_process /system/bin io.remotehosts.agent.ShellMain $name ${Process.myUid()} </dev/null >/dev/null 2>&1 &'"
    }
    companion object {
        fun readFrame(input: DataInputStream): JSONObject {
            val n = input.readInt()
            demand(n in 2..(160 * 1024), "invalid_bridge_frame_size")
            val bytes = ByteArray(n); input.readFully(bytes)
            return JSONObject(String(bytes, Charsets.UTF_8))
        }
        fun writeFrame(out: DataOutputStream, value: JSONObject) {
            val bytes = value.toString().toByteArray()
            demand(bytes.size <= 160 * 1024, "bridge_response_budget_exceeded")
            out.writeInt(bytes.size); out.write(bytes); out.flush()
        }
    }
}
