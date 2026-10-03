package io.remotehosts.agent

import android.app.Activity
import android.app.Instrumentation
import android.os.Bundle
import java.util.UUID

/** Dependency-free instrumentation executed against the real Android SQLite/Keystore runtime. */
class AgentInstrumentation : Instrumentation() {
    private var fixtureMode = false
    override fun onCreate(arguments: Bundle?) { fixtureMode = arguments?.getString("mode") == "fixture"; super.onCreate(arguments); start() }
    override fun onStart() {
        if (fixtureMode) { fixture(); return }
        val report = Bundle(); var passed = 0
        try {
            val c = GatewayConfig("https://integration.invalid", UUID.randomUUID().toString(), nonce(), "isolated test")
            val store = ConfigStore(targetContext); val old = store.read()
            try { store.write(c); check(store.read() == c); passed++
                check(!java.io.File(targetContext.noBackupFilesDir, "gateway-config.enc").readText().contains(c.deviceToken)); passed++
            } finally { if (old != null) store.write(old) else store.erase() }
            val db = Journal(targetContext, c)
            val job = obj("id" to UUID.randomUUID().toString(), "device_id" to c.deviceId, "owner" to "fixture", "tool" to "terminal_exec", "arguments" to obj("command" to "android status"))
            try {
                db.recover(); check(db.claim(job) == null); passed++
                db.recover(); check(db.claim(job)?.optString("error") == "outcome_unknown"); passed++
                val changed = org.json.JSONObject(job.toString()).put("owner", "different")
                var denied = false; try { db.claim(changed) } catch (_: AgentError) { denied = true }; check(denied); passed++
                check(db.pending().size == 1); db.ack(job.getString("id")); check(db.pending().isEmpty()); passed++
                check(db.claim(job)?.optString("state") == "already_completed"); passed++
                val ws = c.deviceId + ":" + UUID.randomUUID(); db.putWorkspace(ws, "fixture", targetContext.filesDir)
                denied = false; try { db.workspace(ws, "other") } catch (_: AgentError) { denied = true }; check(denied); passed++
            } finally { val path = db.databaseName; db.close(); targetContext.deleteDatabase(path) }
            report.putString("stream", "\nRemote Hosts Android: $passed runtime checks passed\n")
            report.putInt("passed", passed); finish(Activity.RESULT_OK, report)
        } catch (e: Throwable) { report.putString("stream", "\nFAILED after $passed checks: ${e.javaClass.simpleName}: ${e.message}\n"); finish(Activity.RESULT_CANCELED, report) }
    }
    private fun fixture() {
        val report = Bundle()
        try {
            check(android.os.Build.FINGERPRINT.contains("generic") || android.os.Build.MODEL.contains("sdk")) { "emulator only" }
            val configFile = java.io.File(targetContext.filesDir, "integration-config.json")
            val c = GatewayConfig.parse(org.json.JSONObject(configFile.readText()))
            check(java.net.URI(c.gatewayUrl).host == "localhost")
            ConfigStore(targetContext).write(c); configFile.delete()
            controls(targetContext).edit().putBoolean("remote_enabled",true).putBoolean("shell_enabled",true).commit()
            startActivitySync(android.content.Intent(targetContext, MainActivity::class.java).addFlags(android.content.Intent.FLAG_ACTIVITY_NEW_TASK))
            targetContext.startForegroundService(android.content.Intent(targetContext,AgentService::class.java))
            val stop = java.io.File(targetContext.filesDir,"integration-stop")
            stop.delete()
            report.putString("stream","\nFIXTURE_READY\n"); sendStatus(0,report)
            val end = android.os.SystemClock.elapsedRealtime()+900000
            while (!stop.exists() && android.os.SystemClock.elapsedRealtime()<end) Thread.sleep(250)
            stop.delete()
            controls(targetContext).edit().putBoolean("remote_enabled",false).putBoolean("shell_enabled",false).commit()
            targetContext.stopService(android.content.Intent(targetContext,AgentService::class.java))
            ConfigStore(targetContext).erase()
            report.putString("stream","\nFIXTURE_STOPPED\n"); finish(Activity.RESULT_OK,report)
        } catch(e:Throwable) { report.putString("stream","\nFIXTURE_FAILED: "+e.javaClass.simpleName+": "+e.message+"\n"); finish(Activity.RESULT_CANCELED,report) }
    }
}
