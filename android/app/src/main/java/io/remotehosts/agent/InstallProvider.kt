package io.remotehosts.agent

import android.content.ContentProvider
import android.content.ContentValues
import android.content.Context
import android.database.Cursor
import android.database.MatrixCursor
import android.net.Uri
import android.os.ParcelFileDescriptor
import android.provider.OpenableColumns
import java.io.File

/** URI grants name one immutable private snapshot, never an arbitrary filesystem path. */
class InstallProvider : ContentProvider() {
    override fun onCreate() = true
    private fun file(uri: Uri): File {
        val key = uri.lastPathSegment ?: throw SecurityException()
        demand(key.matches(Regex("[0-9a-f]{64}\\.apk")), "invalid_install_uri")
        val file = File(requireNotNull(context).cacheDir, "installs/$key")
        demand(file.isFile && file.lastModified() >= System.currentTimeMillis() - 3600000, "install_grant_expired")
        return file
    }
    override fun getType(uri: Uri) = "application/vnd.android.package-archive"
    override fun openFile(uri: Uri, mode: String): ParcelFileDescriptor { demand(mode == "r", "read_only_install_grant"); return ParcelFileDescriptor.open(file(uri), ParcelFileDescriptor.MODE_READ_ONLY) }
    override fun query(uri: Uri, projection: Array<out String>?, selection: String?, selectionArgs: Array<out String>?, sortOrder: String?): Cursor {
        val f = file(uri); return MatrixCursor(arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE)).apply { addRow(arrayOf<Any>("application.apk", f.length())) }
    }
    override fun insert(uri: Uri, values: ContentValues?): Uri? = throw SecurityException()
    override fun update(uri: Uri, values: ContentValues?, selection: String?, selectionArgs: Array<out String>?) = throw SecurityException()
    override fun delete(uri: Uri, selection: String?, selectionArgs: Array<out String>?) = throw SecurityException()
    companion object {
        fun grant(context: Context, source: File): Uri {
            val dir = File(context.cacheDir, "installs"); dir.mkdirs()
            dir.listFiles()?.filter { it.lastModified() < System.currentTimeMillis() - 3600000 }?.forEach { it.delete() }
            demand((dir.listFiles()?.sumOf { it.length() } ?: 0) + source.length() <= Policy.MAX_FILE * 2, "install_cache_limit")
            val snapshot = File(dir, nonce() + ".apk")
            source.inputStream().use { input -> java.io.FileOutputStream(snapshot).use { FileTools.copy(input, it, Policy.MAX_FILE) } }
            return Uri.Builder().scheme("content").authority(context.packageName + ".files").appendPath(snapshot.name).build()
        }
    }
}
