package io.remotehosts.agent

import android.content.ContentProvider
import android.content.ContentValues
import android.database.Cursor
import android.database.MatrixCursor
import android.net.Uri
import android.os.Binder
import android.os.Process
import android.os.ParcelFileDescriptor

/** Shell-only activation metadata and one authenticated, unnamed duplex FD. */
class BridgeProvider : ContentProvider() {
    override fun onCreate() = true
    override fun query(uri: Uri, projection: Array<out String>?, selection: String?, selectionArgs: Array<out String>?, sortOrder: String?): Cursor {
        val caller = Binder.getCallingUid()
        if (caller != 2000 && caller != 0 && caller != Process.myUid()) throw SecurityException("shell_or_owner_uid_required")
        val ctx = context ?: throw SecurityException("unavailable")
        if (uri.path != "/status" || !controls(ctx).getBoolean("shell_enabled", false)) throw SecurityException("local_shell_consent_required")
        val bridge = AgentRuntime.bridge ?: throw SecurityException("start_agent_first")
        val name = bridge.socketName ?: throw SecurityException("bridge_not_listening")
        return MatrixCursor(arrayOf("socket", "uid", "connected", "apk")).apply {
            addRow(arrayOf(name, Process.myUid(), if (bridge.connected()) 1 else 0, ctx.applicationInfo.sourceDir))
        }
    }
    override fun openFile(uri: Uri, mode: String): ParcelFileDescriptor {
        val caller = Binder.getCallingUid()
        if (caller != 2000 && caller != 0) throw SecurityException("shell_uid_required")
        val parts = uri.pathSegments
        if (mode != "rw" || parts.size != 2 || parts[0] != "connect") throw SecurityException("invalid_bridge_endpoint")
        val bridge = AgentRuntime.bridge ?: throw SecurityException("start_agent_first")
        return bridge.attach(parts[1], caller)
    }
    override fun getType(uri: Uri) = "vnd.android.cursor.item/vnd.remotehosts.bridge"
    override fun insert(uri: Uri, values: ContentValues?): Uri? = throw SecurityException("read_only")
    override fun update(uri: Uri, values: ContentValues?, selection: String?, selectionArgs: Array<out String>?) = throw SecurityException("read_only")
    override fun delete(uri: Uri, selection: String?, selectionArgs: Array<out String>?) = throw SecurityException("read_only")
}
