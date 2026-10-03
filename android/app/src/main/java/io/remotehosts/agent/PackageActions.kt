package io.remotehosts.agent

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import org.json.JSONObject
import java.io.File

class PackageActions(private val context: Context) {
    @Suppress("DEPRECATION") fun apps(args: JSONObject): JSONObject {
        val list = context.packageManager.getInstalledPackages(0).sortedBy { it.packageName }
        val offset = args.optInt("offset", 0); val limit = args.optInt("limit", 80).coerceIn(1, 150)
        demand(offset in 0..list.size, "invalid_offset")
        val items = list.drop(offset).take(limit).map { obj("package" to it.packageName, "version" to it.versionName, "version_code" to it.longVersionCode) }
        return obj("apps" to arr(items), "total" to list.size, "next_offset" to if (offset + items.size < list.size) offset + items.size else null)
    }
    fun launch(args: JSONObject): JSONObject {
        val name = Policy.packageName(args.getString("package"))
        val intent = context.packageManager.getLaunchIntentForPackage(name) ?: throw AgentError("launch_activity_unavailable")
        // AccessibilityService is user-enabled and can launch a requested activity without a hidden overlay.
        val service = AccessService.instance ?: throw AgentError("accessibility_permission_required")
        service.startActivity(intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
        return obj("accepted" to true, "package" to name, "verified" to false, "next_action" to "android observe")
    }
    @Suppress("DEPRECATION") fun install(root: File, args: JSONObject): JSONObject {
        val path = args.getString("path"); val file = Policy.within(root, path)
        demand(file.isFile && file.length() in 1..Policy.MAX_FILE, "apk_unavailable")
        val digest = FileTools.digest(file)
        if (args.has("sha256")) demand(args.getString("sha256") == digest, "sha256_mismatch")
        val archive = context.packageManager.getPackageArchiveInfo(file.path, 0) ?: throw AgentError("invalid_apk")
        val pkg = Policy.packageName(archive.packageName)
        demand(pkg != context.packageName, "self_update_requires_local_install")
        val bridge = AgentRuntime.bridge
        if (bridge?.connected() == true) {
            val result = bridge.execute(obj("op" to "install", "timeout_seconds" to 180), file)
            val success = result.optInt("exit_code", -1) == 0 && result.optString("output").contains("Success")
            val installed = try { context.packageManager.getPackageInfo(pkg, 0) } catch (_: PackageManager.NameNotFoundException) { null }
            return result.put("package", pkg).put("sha256", digest).put("installed", success && installed?.longVersionCode == archive.longVersionCode)
        }
        demand(context.packageManager.canRequestPackageInstalls(), "allow_install_unknown_apps_in_settings")
        val uri = InstallProvider.grant(context, file)
        val intent = Intent(Intent.ACTION_INSTALL_PACKAGE).setDataAndType(uri, "application/vnd.android.package-archive")
            .addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_ACTIVITY_NEW_TASK)
        val pending = PendingIntent.getActivity(context, 200, intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
        val nm = context.getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(NotificationChannel("installs", "应用安装确认", NotificationManager.IMPORTANCE_HIGH))
        demand(nm.areNotificationsEnabled(), "notification_permission_required_for_install_confirmation")
        nm.notify(200, Notification.Builder(context, "installs").setSmallIcon(R.drawable.ic_agent).setContentTitle("确认安装应用")
            .setContentText(pkg).setContentIntent(pending).setAutoCancel(true).build())
        return obj("state" to "awaiting_user_confirmation", "installed" to false, "package" to pkg, "sha256" to digest, "message" to "Tap the phone notification to confirm installation.")
    }
}
