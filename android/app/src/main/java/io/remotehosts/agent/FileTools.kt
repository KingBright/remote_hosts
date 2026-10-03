package io.remotehosts.agent

import android.content.Context
import android.system.Os
import org.json.JSONObject
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream
import java.security.MessageDigest

class FileTools(private val context: Context, private val transport: Transport) {
    private fun limit(args: JSONObject): Long = args.optLong("max_bytes", Policy.MAX_FILE).also { demand(it in 1..Policy.MAX_FILE, "file_limit_exceeded") }
    companion object {
        private val publicationLock = Any()
        /** All app writers share this lock; Android app-private data has one UID.
         * Android denies hard-link creation here, so publish by atomic rename,
         * rechecking the expected version under the same writer lock. */
        fun publish(root: File, path: String, temporary: File, expected: String) = synchronized(publicationLock) {
            val destination = Policy.within(root, path)
            demand(temporary.isFile && temporary.canonicalFile == temporary.absoluteFile, "invalid_staged_file")
            demand(version(destination) == expected, "version_conflict")
            Os.rename(temporary.path, destination.path)
        }
        fun digest(file: File): String {
            val h = MessageDigest.getInstance("SHA-256")
            file.inputStream().use { input -> val b = ByteArray(65536); while (true) { val n = input.read(b); if (n < 0) break; h.update(b, 0, n) } }
            return h.digest().joinToString("") { "%02x".format(it) }
        }
        fun copy(input: InputStream, output: FileOutputStream, max: Long): Long {
            val buffer = ByteArray(65536); var count = 0L
            while (true) { if (Thread.currentThread().isInterrupted) throw InterruptedException(); val n = input.read(buffer); if (n < 0) break; count += n; demand(count <= max, "file_limit_exceeded"); output.write(buffer, 0, n) }
            output.fd.sync(); return count
        }
        fun version(file: File): String = if (!file.exists()) "absent" else { demand(file.isFile && file.length() <= Policy.MAX_FILE, "invalid_existing_file"); digest(file) }
    }
    fun upload(root: File, args: JSONObject, id: String): JSONObject {
        val path = args.getString("path"); val file = Policy.within(root, path)
        val expected = args.optString("expected_version", "absent")
        demand(expected == "absent" || expected.matches(Regex("[0-9a-f]{64}")), "invalid_expected_version")
        demand(version(file) == expected, "version_conflict")
        val max = limit(args); demand(root.usableSpace > max + 64L * 1024 * 1024, "storage_reserve_insufficient")
        val source = transport.json("/device/file-source/$id")
        val url = Policy.source(source.getString("download_url"), transport.config.gatewayUrl)
        file.parentFile!!.mkdirs(); Policy.within(root, path)
        val tmp = File(file.parentFile, ".rh-upload-$id.tmp"); demand(tmp.createNewFile(), "staging_conflict")
        val c = transport.connection(url.toString(), "GET", false)
        try {
            demand(c.responseCode == 200, "file_source_rejected")
            val declared = c.contentLengthLong; demand(declared <= max, "file_limit_exceeded")
            val size = c.inputStream.use { input -> FileOutputStream(tmp).use { copy(input, it, max) } }
            if (declared >= 0) demand(declared == size, "incomplete_file")
            val sha = digest(tmp)
            if (args.has("sha256")) demand(args.getString("sha256") == sha, "sha256_mismatch")
            demand(Policy.within(root, path) == file && version(file) == expected, "version_conflict")
            publish(root, path, tmp, expected)
            return obj("path" to path, "size" to size, "sha256" to sha, "version" to sha, "state" to "completed", "atomic" to true)
        } finally { transport.release(c); tmp.delete() }
    }
    fun download(root: File, args: JSONObject, id: String): JSONObject {
        val path = args.getString("path"); val source = Policy.within(root, path); val max = limit(args)
        demand(source.isFile && source.length() <= max, "file_unavailable_or_too_large")
        demand(context.cacheDir.usableSpace >= source.length() + 64L * 1024 * 1024, "storage_reserve_insufficient")
        val snapshot = File.createTempFile("rh-download-", ".tmp", context.cacheDir)
        try {
            source.inputStream().use { input -> FileOutputStream(snapshot).use { copy(input, it, max) } }
            val sha = digest(snapshot); val size = snapshot.length()
            if (args.has("expected_version")) demand(args.getString("expected_version") == sha, "version_conflict")
            val c = transport.connection(transport.config.gatewayUrl + "/device/files/$id", "POST")
            try {
                c.doOutput = true; c.setFixedLengthStreamingMode(size)
                c.setRequestProperty("Content-Type", "application/octet-stream")
                c.setRequestProperty("x-file-size", size.toString()); c.setRequestProperty("x-file-sha256", sha)
                c.outputStream.use { out -> snapshot.inputStream().use { it.copyTo(out, 65536) } }
                demand(c.responseCode in 200..299, "gateway_file_rejected")
                val receipt = JSONObject(String(c.inputStream.use { Transport.bounded(it, 16384) }, Charsets.UTF_8))
                demand(receipt.getString("sha256") == sha && receipt.getLong("size") == size, "gateway_file_receipt_mismatch")
            } finally { transport.release(c) }
            return obj("artifact_id" to id, "path" to path, "file_name" to source.name, "size" to size, "sha256" to sha, "state" to "completed")
        } finally { snapshot.delete() }
    }
    fun list(root: File, args: JSONObject): JSONObject {
        val glob = args.optString("glob", "**/*")
        demand(glob.length <= 512 && !glob.contains(".."), "invalid_glob")
        val regex = glob.replace(".", "\\.").replace("**/", "\u0001").replace("**", "\u0002").replace("*", "[^/]*").replace("?", "[^/]").replace("\u0001", "(.*/)?").replace("\u0002", ".*").toRegex()
        val files = mutableListOf<String>(); var scanned = 0; val limit = args.optInt("limit", 100).coerceIn(1, 500)
        for (f in root.walkTopDown().maxDepth(12).onEnter { it.canonicalFile == it.absoluteFile }) {
            if (++scanned > 10000) break
            if (f.isFile && f.canonicalFile == f.absoluteFile) {
                val p = f.relativeTo(root).invariantSeparatorsPath
                if (regex.matches(p)) { files += p; if (files.size == limit) break }
            }
        }
        return obj("files" to arr(files), "truncated" to (files.size == limit || scanned > 10000), "next_cursor" to null)
    }
    fun read(root: File, args: JSONObject): JSONObject {
        val requests = args.getJSONArray("requests"); demand(requests.length() in 1..20, "request_count_exceeded")
        var budget = args.optInt("max_bytes", 32768).coerceIn(1024, 65536)
        val ranges = mutableListOf<JSONObject>()
        for (i in 0 until requests.length()) {
            val q = requests.getJSONObject(i); val path = q.getString("path"); val f = Policy.within(root, path)
            demand(f.isFile && f.length() <= 1024 * 1024, "text_file_unavailable_or_too_large")
            val version = digest(f); if (q.has("expected_version")) demand(q.getString("expected_version") == version, "version_conflict")
            val lines = f.readText().split('\n'); val start = q.getInt("start_line"); val end = q.getInt("end_line")
            demand(start >= 1 && end >= start && q.optInt("line_byte_offset", 0) == 0, "invalid_range")
            val picked = mutableListOf<String>(); var index = start - 1
            while (index < lines.size && index < end) { val text = lines[index] + "\n"; if (text.toByteArray().size > budget) break; budget -= text.toByteArray().size; picked += text; index++ }
            val more = index < minOf(end, lines.size)
            ranges += obj("path" to path, "version" to version, "start_line" to start, "end_line" to index, "text" to picked.joinToString(""), "total_lines" to lines.size, "truncated" to more, "next_line" to if (more) index + 1 else null)
        }
        return obj("ranges" to arr(ranges))
    }
    fun pruneScreenshots(root: File) {
        val dir = File(root, "screenshots"); var size = 0L; var count = 0
        dir.listFiles()?.filter { it.name.matches(Regex("[0-9a-f-]{36}\\.png")) && it.isFile && it.canonicalFile == it.absoluteFile }
            ?.sortedByDescending { it.lastModified() }?.forEach {
                count++; size += it.length()
                // Keep fresh captures even during a burst, but refuse new captures at the hard cap in Dispatcher.
                if (it.lastModified() < System.currentTimeMillis() - 3600000 && (count > 24 || size > 64L * 1024 * 1024 || it.lastModified() < System.currentTimeMillis() - 86400000)) it.delete()
            }
    }
}
