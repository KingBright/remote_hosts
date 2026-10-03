package io.remotehosts.agent

import org.json.JSONObject
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.ConcurrentHashMap

class HttpFailure(val status: Int) : Exception("HTTP_$status")
class Transport(val config: GatewayConfig) {
    private val connections = ConcurrentHashMap.newKeySet<HttpURLConnection>()
    @Volatile private var closed = false
    fun connection(url: String, method: String, authenticated: Boolean = true): HttpURLConnection {
        demand(!closed, "transport_stopped")
        if (authenticated) demand(url.startsWith(config.gatewayUrl + "/"), "credential_origin_mismatch")
        val c = URL(url).openConnection() as HttpURLConnection
        c.instanceFollowRedirects = false
        c.connectTimeout = 10000
        c.readTimeout = 30000
        c.requestMethod = method
        c.setRequestProperty("Accept", "application/json")
        c.setRequestProperty("User-Agent", "RemoteHosts-Android/${BuildConfig.VERSION_NAME}")
        if (authenticated) c.setRequestProperty("Authorization", "Bearer ${config.deviceToken}")
        connections.add(c)
        if (closed) { c.disconnect(); throw AgentError("transport_stopped") }
        return c
    }
    fun release(c: HttpURLConnection) { connections.remove(c); c.disconnect() }
    fun json(path: String, body: JSONObject? = null): JSONObject {
        val c = connection(config.gatewayUrl + path, if (body == null) "GET" else "POST")
        try {
            if (body != null) {
                val bytes = body.toString().toByteArray()
                demand(bytes.size <= Policy.MAX_JSON, "request_budget_exceeded")
                c.doOutput = true
                c.setFixedLengthStreamingMode(bytes.size)
                c.setRequestProperty("Content-Type", "application/json")
                c.outputStream.use { it.write(bytes) }
            }
            val status = c.responseCode
            if (status !in 200..299) throw HttpFailure(status)
            val bytes = c.inputStream.use { bounded(it, Policy.MAX_JSON) }
            return if (bytes.isEmpty()) obj() else JSONObject(String(bytes, Charsets.UTF_8))
        } finally { release(c) }
    }
    fun close() { closed = true; connections.forEach { it.disconnect() }; connections.clear() }
    companion object {
        fun bounded(input: InputStream, limit: Int): ByteArray {
            val out = ByteArrayOutputStream()
            val buffer = ByteArray(16384)
            while (true) {
                val n = input.read(buffer)
                if (n < 0) break
                demand(out.size() + n <= limit, "response_budget_exceeded")
                out.write(buffer, 0, n)
            }
            return out.toByteArray()
        }
    }
}
