package io.remotehosts.agent

import android.content.AttributionSource
import android.content.pm.ProviderInfo
import android.net.Uri
import android.os.Binder
import android.os.Build
import android.os.IBinder
import android.os.ParcelFileDescriptor
import android.os.Process
import java.lang.reflect.InvocationTargetException

/** Uses the same external-provider acquisition path as Android's `content`
 * shell command. A shell process is not an ActivityManager-registered app.
 * All attribution uses the real process UID; no permission policy is changed. */
internal class ShellProviderConnection(private val activation: String, private val appUid: Int) : AutoCloseable {
    private val authority = BuildConfig.APPLICATION_ID + ".bridge"
    private val user = appUid / 100000
    private val token = Binder()
    private val managerType = Class.forName("android.app.IActivityManager")
    private val manager = Class.forName("android.app.ActivityManager").getMethod("getService").invoke(null)
    private var acquired = false
    fun open(): ParcelFileDescriptor {
        val uid = Process.myUid()
        demand(uid == 2000 || uid == 0, "shell_uid_required")
        try {
            val holder = managerType.getMethod("getContentProviderExternal", String::class.java,
                Int::class.javaPrimitiveType, IBinder::class.java, String::class.java)
                .invoke(manager, authority, user, token, "remote-hosts")
                ?: throw AgentError("bridge_provider_missing")
            acquired = true
            val info = holder.javaClass.getField("info").get(holder) as ProviderInfo
            demand(info.applicationInfo.uid == appUid && info.packageName == BuildConfig.APPLICATION_ID,
                "app_peer_uid_mismatch")
            val provider = holder.javaClass.getField("provider").get(holder)
                ?: throw AgentError("bridge_provider_missing")
            val uri = Uri.parse("content://$authority/connect/$activation")
            val method = Class.forName("android.content.IContentProvider").methods.firstOrNull { m ->
                m.name == "openFile" && m.parameterTypes.count { it == Uri::class.java } == 1 &&
                    m.parameterTypes.all { it == String::class.java || it == Uri::class.java ||
                        it == IBinder::class.java || it.name == "android.os.ICancellationSignal" ||
                        it.name == "android.content.AttributionSource" }
            } ?: throw AgentError("unsupported_android_provider_signature")
            val types = method.parameterTypes
            val uriIndex = types.indexOf(Uri::class.java)
            val callingPackage = if (uid == 0) "root" else "com.android.shell"
            val arguments = Array<Any?>(types.size) { index ->
                when {
                    types[index] == Uri::class.java -> uri
                    types[index].name == "android.content.AttributionSource" -> {
                        demand(Build.VERSION.SDK_INT >= 31, "unsupported_android_attribution")
                        attribution(uid, callingPackage)
                    }
                    types[index] == String::class.java && index == 0 -> callingPackage
                    types[index] == String::class.java && index < uriIndex -> null // API 30 attribution tag
                    types[index] == String::class.java && index == uriIndex + 1 -> "rw"
                    types[index] == IBinder::class.java || types[index].name == "android.os.ICancellationSignal" -> null
                    else -> throw AgentError("unsupported_android_provider_signature")
                }
            }
            return method.invoke(provider, *arguments) as? ParcelFileDescriptor
                ?: throw AgentError("bridge_descriptor_missing")
        } catch (e: InvocationTargetException) {
            close()
            throw (e.targetException as? Exception ?: e)
        } catch (e: Exception) { close(); throw e }
    }
    @android.annotation.TargetApi(31)
    private fun attribution(uid: Int, packageName: String): Any =
        AttributionSource.Builder(uid).setPackageName(packageName).build()
    override fun close() {
        if (!acquired) return
        acquired = false
        runCatching {
            managerType.getMethod("removeContentProviderExternalAsUser", String::class.java,
                IBinder::class.java, Int::class.javaPrimitiveType).invoke(manager, authority, token, user)
        }
    }
}
