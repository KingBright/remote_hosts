package io.remotehosts.agent

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import org.json.JSONObject
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

object AgentRuntime {
    @Volatile var running = false
    @Volatile var bridge: ShellBridge? = null
    @Volatile var state = "已停止"
    @Volatile var lastContact = 0L
    @Volatile var currentOperation: String? = null
    val revision = AtomicLong()
    fun changed() { revision.incrementAndGet() }
}

class AgentService : Service() {
    private val workers = Executors.newFixedThreadPool(3)
    private val receiptWake = java.util.concurrent.Semaphore(0)
    private var transport: Transport? = null
    private var journal: Journal? = null
    @Volatile private var alive = false
    private var started = false
    private var session = nonce()
    private lateinit var config: GatewayConfig
    override fun onBind(intent: Intent?): IBinder? = null
    override fun onCreate() { super.onCreate(); notification("正在连接") }
    private fun notification(text: String) {
        val nm = getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(NotificationChannel("connection", "远程连接状态", NotificationManager.IMPORTANCE_LOW))
        val open = PendingIntent.getActivity(this, 1, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
        val stop = PendingIntent.getService(this, 2, Intent(this, AgentService::class.java).setAction("STOP"), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
        val n = Notification.Builder(this, "connection").setSmallIcon(R.drawable.ic_agent).setContentTitle("Remote Hosts · 远程控制已开启")
            .setContentText(text).setContentIntent(open).setOngoing(true).setOnlyAlertOnce(true)
            .addAction(Notification.Action.Builder(null, "停止远程控制", stop).build()).build()
        if (Build.VERSION.SDK_INT >= 34) startForeground(101, n, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE) else startForeground(101, n)
    }
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == "STOP") { controls(this).edit().putBoolean("remote_enabled", false).apply(); stopSelf(); return START_NOT_STICKY }
        if (!controls(this).getBoolean("remote_enabled", false)) { stopSelf(); return START_NOT_STICKY }
        if (started) { updateBridge(); return START_STICKY }
        try {
            config = ConfigStore(this).read() ?: throw AgentError("missing_configuration")
            val db = Journal(this, config); db.recover(); journal = db
            val net = Transport(config); transport = net
            alive = true; started = true; AgentRuntime.running = true; AgentRuntime.lastContact = 0; AgentRuntime.state = "正在连接"; AgentRuntime.changed()
            AgentRuntime.bridge = ShellBridge(this); updateBridge()
            val dispatcher = Dispatcher(this, config, db, net)
            workers.submit { poll(net, db, dispatcher) }
            workers.submit { heartbeat(net) }
            workers.submit { receipts(net, db) }
        } catch (e: Exception) {
            AgentRuntime.state = "启动失败：" + Policy.error(e).optString("error"); AgentRuntime.changed(); stopSelf()
        }
        return START_STICKY
    }
    private fun updateBridge() { if (controls(this).getBoolean("shell_enabled", false)) AgentRuntime.bridge?.open() else AgentRuntime.bridge?.close() }
    private fun hello(): JSONObject {
        val features = mutableListOf("android_commands_v1", "android_durable_receipts_v1", "android_files_v1")
        if (AccessService.instance != null) features += "android_accessibility_v1"
        if (AgentRuntime.bridge?.connected() == true) features += "android_shell_bridge_v1"
        return obj("version" to BuildConfig.VERSION_NAME, "wire_protocol" to 2, "platform" to "android", "arch" to Build.SUPPORTED_ABIS.firstOrNull(),
            "home_dir" to exposedRoot(this, config).path, "session" to session, "roots" to arr(listOf(exposedRoot(this, config).path)),
            "allow_write" to true, "allow_exec" to true, "runtime_features" to obj("protocol" to 1, "names" to arr(features)),
            "active_operations" to arr(listOfNotNull(AgentRuntime.currentOperation)), "poll_wait_ms" to 20000)
    }
    private fun contact() { AgentRuntime.lastContact = System.currentTimeMillis(); AgentRuntime.state = "已连接"; AgentRuntime.changed() }
    private fun pause(milliseconds: Long): Boolean {
        if (!alive) return false
        try { Thread.sleep(milliseconds); return alive } catch (_: InterruptedException) { return false }
    }
    private fun failure(e: Exception, attempt: Int): Boolean {
        if (!alive) return false
        val auth = e is HttpFailure && e.status in listOf(401, 403)
        AgentRuntime.state = if (auth) "认证被拒绝，请检查设备配置" else "连接暂不可用，正在重连"
        AgentRuntime.changed()
        return pause(if (auth) 30000 else (1000L shl attempt.coerceAtMost(5)) + (Math.random() * 700).toLong())
    }
    private fun poll(net: Transport, db: Journal, dispatcher: Dispatcher) {
        var attempt = 0
        while (alive) {
            try {
                val health = net.json("/healthz")
                demand(health.optInt("wire_protocol") == 2, "gateway_wire_incompatible")
                break
            } catch (e: Exception) { if (!failure(e, attempt++)) return }
        }
        while (alive) {
            try {
                val result = net.json("/device/poll", hello())
                demand(result.has("job"), "invalid_gateway_response"); contact(); attempt = 0
                if (result.isNull("job")) continue
                val job = result.getJSONObject("job")
                demand(job.getString("device_id") == config.deviceId, "wrong_device")
                if (!alive) break
                if (db.claim(job) != null) continue
                val id = job.getString("id"); AgentRuntime.currentOperation = id
                val wake = getSystemService(PowerManager::class.java).newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "RemoteHosts:operation")
                wake.acquire(210000)
                try {
                    val completed = try { dispatcher.perform(job) } catch (e: Exception) { Policy.error(e) }
                    db.finish(id, completed); receiptWake.release()
                } finally { if (wake.isHeld) wake.release(); AgentRuntime.currentOperation = null }
            } catch (e: Exception) { if (!failure(e, attempt++)) break }
        }
    }
    private fun heartbeat(net: Transport) {
        while (alive) {
            try { net.json("/device/heartbeat", hello()); contact() } catch (_: Exception) { /* Poll owns user-facing connection diagnostics. */ }
            if (!pause(12000)) return
        }
    }
    private fun receipts(net: Transport, db: Journal) {
        var attempt = 0; var cycles = 0
        while (alive) {
            try {
                for ((id, saved) in db.pending()) {
                    if (!alive) return
                    net.json("/device/result", obj("operation_id" to id, "result" to saved)); db.ack(id); contact(); attempt = 0
                }
                if (++cycles % 100 == 0) db.prune()
                receiptWake.tryAcquire(30, TimeUnit.SECONDS); receiptWake.drainPermits()
            } catch (e: Exception) { if (!failure(e, attempt++)) return }
        }
    }
    override fun onDestroy() {
        alive = false; AgentRuntime.running = false; AgentRuntime.currentOperation = null
        transport?.close(); AgentRuntime.bridge?.close(); AgentRuntime.bridge = null
        workers.shutdownNow()
        val db = journal
        Thread({ try { if (workers.awaitTermination(35, TimeUnit.SECONDS)) db?.close() } catch (_: Exception) {} }, "rh-shutdown").start()
        if (!AgentRuntime.state.startsWith("启动失败")) AgentRuntime.state = "已停止"
        AgentRuntime.changed(); stopForeground(STOP_FOREGROUND_REMOVE); super.onDestroy()
    }
}
