package io.remotehosts.agent

import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.net.URI
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.UUID

fun obj(vararg pairs: Pair<String, Any?>): JSONObject = JSONObject().apply {
    for ((k, v) in pairs) put(k, v ?: JSONObject.NULL)
}
fun arr(items: Iterable<*>): JSONArray = JSONArray().apply { items.forEach { put(it) } }
fun sha256(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
fun nonce(): String = ByteArray(32).also { SecureRandom().nextBytes(it) }.joinToString("") { "%02x".format(it) }
fun now(): Long = System.currentTimeMillis() / 1000
class AgentError(val code: String, override val message: String = code) : Exception(message)
fun demand(condition: Boolean, code: String, message: String = code) { if (!condition) throw AgentError(code, message) }
fun canonicalJson(v: Any?): String = when (v) {
    is JSONObject -> v.keys().asSequence().toList().sorted().joinToString(",", "{", "}") { JSONObject.quote(it) + ":" + canonicalJson(v.get(it)) }
    is JSONArray -> (0 until v.length()).joinToString(",", "[", "]") { canonicalJson(v.get(it)) }
    null, JSONObject.NULL -> "null"
    is String -> JSONObject.quote(v)
    is Boolean, is Number -> v.toString()
    else -> throw AgentError("invalid_json_value")
}
object Policy {
    const val MAX_FILE = 64L * 1024 * 1024
    const val MAX_OUTPUT = 96 * 1024
    const val MAX_JSON = 512 * 1024
    fun origin(value: String): String {
        val text = value.trim().trimEnd('/')
        val u = try { URI(text) } catch (_: Exception) { throw AgentError("invalid_gateway_origin") }
        demand(u.scheme == "https" && !u.host.isNullOrEmpty() && u.rawUserInfo == null &&
            u.rawQuery == null && u.rawFragment == null && u.rawPath.isNullOrEmpty() &&
            (u.port == -1 || u.port in 1..65535), "https_origin_required")
        return text
    }
    fun uuid(value: String): String {
        val id = try { UUID.fromString(value) } catch (_: Exception) { throw AgentError("invalid_uuid") }
        demand(id.toString() == value.lowercase(), "invalid_uuid")
        return id.toString()
    }
    fun packageName(value: String): String {
        demand(value.length in 3..255 && Regex("[A-Za-z][A-Za-z0-9_]*(\\.[A-Za-z][A-Za-z0-9_]*)+").matches(value), "invalid_package_name")
        return value
    }
    fun relative(value: String): String {
        demand(value.isNotEmpty() && value.length <= 1024 && !value.startsWith('/') && !value.contains('\\') &&
            !value.contains('\u0000') && value.split('/').all { it.isNotEmpty() && it != "." && it != ".." }, "unsafe_relative_path")
        return value
    }
    /** Exposed files never include configuration, the database, or private app state. */
    fun within(root: File, relative: String): File {
        val r = root.canonicalFile
        val p = File(r, relative(relative))
        var node: File? = p
        while (node != null && node != r) {
            demand(node.canonicalFile == node.absoluteFile, "symlink_path_rejected")
            node = node.parentFile
        }
        demand(p.canonicalPath.startsWith(r.path + File.separator), "path_outside_workspace")
        return p
    }
    fun shellQuote(text: String): String {
        demand(!text.contains('\u0000'), "nul_in_argument")
        return "'" + text.replace("'", "'\\''") + "'"
    }
    fun source(url: String, gateway: String): URI {
        val u = try { URI(url) } catch (_: Exception) { throw AgentError("invalid_source_url") }
        val own = URI(gateway)
        val port = if (u.port == -1) 443 else u.port
        val ownPort = if (own.port == -1) 443 else own.port
        val host = u.host?.lowercase() ?: ""
        val owned = host == own.host.lowercase() && port == ownPort && u.path.startsWith("/files/")
        val hosted = port == 443 && (host == "oaiusercontent.com" || host.endsWith(".oaiusercontent.com") || host.endsWith(".blob.core.windows.net"))
        demand(u.scheme == "https" && u.rawUserInfo == null && u.rawFragment == null && (owned || hosted), "untrusted_file_source")
        return u
    }
    fun error(t: Throwable): JSONObject {
        val e = (t as? java.util.concurrent.ExecutionException)?.cause ?: t
        val code = when (e) {
            is AgentError -> e.code
            is SecurityException -> "android_permission_denied"
            is HttpFailure -> "gateway_http_${e.status}"
            is org.json.JSONException -> "invalid_protocol_field"
            is android.system.ErrnoException -> "android_filesystem_errno_${e.errno}"
            is javax.net.ssl.SSLException -> "tls_connection_failed"
            is java.net.SocketTimeoutException -> "network_timeout_outcome_unknown"
            is java.io.IOException -> "io_failure_outcome_unknown"
            is java.util.concurrent.TimeoutException -> "operation_timeout_outcome_unknown"
            is InterruptedException -> "operation_interrupted_outcome_unknown"
            else -> "android_operation_failed"
        }
        return obj("error" to code, "state" to if (code.contains("outcome_unknown")) "outcome_unknown" else "failed",
            "message" to (if (e is AgentError) e.message else code), "automatic_replay" to false,
            "cause_type" to e.javaClass.simpleName)
    }
}
data class GatewayConfig(val gatewayUrl: String, val deviceId: String, val deviceToken: String, val name: String) {
    fun identity(): String = sha256((gatewayUrl + "\n" + deviceId).toByteArray())
    fun json(): JSONObject = obj("gateway_url" to gatewayUrl, "device_id" to deviceId, "device_token" to deviceToken, "name" to name)
    companion object {
        fun parse(v: JSONObject): GatewayConfig {
            val token = v.getString("device_token")
            demand(token.length in 32..512 && token.all { it.code in 33..126 }, "invalid_device_token")
            return GatewayConfig(Policy.origin(v.getString("gateway_url")), Policy.uuid(v.getString("device_id")), token,
                v.optString("name", "Android").take(80))
        }
    }
}
