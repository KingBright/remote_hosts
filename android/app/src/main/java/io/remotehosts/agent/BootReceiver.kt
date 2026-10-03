package io.remotehosts.agent

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

class BootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action !in listOf(Intent.ACTION_BOOT_COMPLETED, Intent.ACTION_MY_PACKAGE_REPLACED)) return
        if (controls(context).getBoolean("start_at_boot", false) && controls(context).getBoolean("remote_enabled", false)) {
            // A platform may reject background startup. Never attempt a hidden bypass.
            runCatching { context.startForegroundService(Intent(context, AgentService::class.java)) }
        }
    }
}
