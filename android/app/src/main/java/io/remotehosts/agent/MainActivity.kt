package io.remotehosts.agent

import android.Manifest
import android.app.Activity
import android.app.AlertDialog
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.provider.Settings
import android.text.InputType
import android.view.View
import android.view.WindowInsets
import android.view.WindowManager
import android.widget.Button
import android.widget.CheckBox
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.Executors

/** Native single-page setup. No account, gateway, or token is compiled into the APK. */
class MainActivity : Activity() {
    private lateinit var body: LinearLayout
    private lateinit var gateway: EditText
    private lateinit var device: EditText
    private lateinit var token: EditText
    private lateinit var name: EditText
    private lateinit var status: TextView
    private lateinit var capabilities: TextView
    private lateinit var toggle: Button
    private lateinit var shell: CheckBox
    private val handler = Handler(Looper.getMainLooper())
    private val io = Executors.newSingleThreadExecutor()
    private val refresh = object : Runnable { override fun run() { redraw(); handler.postDelayed(this, 1000) } }
    private var revision = Long.MIN_VALUE
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.addFlags(WindowManager.LayoutParams.FLAG_SECURE)
        val scroll = ScrollView(this).apply { setBackgroundColor(Color.rgb(246, 248, 252)); isFillViewport = true }
        body = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(dp(22), dp(22), dp(22), dp(30)) }
        scroll.addView(body)
        scroll.setOnApplyWindowInsetsListener { view, insets ->
            val system = insets.getInsets(WindowInsets.Type.systemBars() or WindowInsets.Type.displayCutout())
            view.setPadding(system.left, system.top, system.right, system.bottom); insets
        }
        setContentView(scroll)
        label("Remote Hosts", 29, true)
        label("安卓被控端 · ${BuildConfig.VERSION_NAME}", 13)
        status = label("已停止", 20, true)
        capabilities = label("", 13)
        label("连接配置", 19, true)
        label("使用服务端为这台手机签发的独立设备配置。APK 不包含任何服务地址、账号或密钥。", 13)
        gateway = field("服务地址", "https://your-gateway.example")
        device = field("设备 ID", "服务端签发的 UUID")
        token = field("设备令牌", "独立、可撤销的设备令牌", true)
        name = field("本机备注", Build.MODEL)
        button("导入设备配置 JSON") { picker(11, "application/json") }
        button("保存配置") { guarded { save(); toast("已加密保存到此手机") } }
        toggle = button("开启远程控制") {
            if (AgentRuntime.running) stopRemote() else AlertDialog.Builder(this)
                .setTitle("开启远程控制？")
                .setMessage("你授权的服务将能够读取共享文件，并在启用无障碍后读取屏幕、截图、点击和输入文字。shell 权限需要另外允许并激活。\n\n连接期间会显示常驻通知。随时点击停止即可断开；不会绕过锁屏、安全窗口或系统授权。")
                .setNegativeButton("取消", null).setPositiveButton("允许并开启") { _, _ -> guarded { save(); controls(this).edit().putBoolean("remote_enabled", true).apply(); startForegroundService(Intent(this, AgentService::class.java)); askNotifications() } }.show()
        }
        label("权限与能力", 19, true)
        button("启用读屏与操作：无障碍设置") { startActivity(Intent(Settings.ACTION_ACCESSIBILITY_SETTINGS)) }
        button("通知权限") { askNotifications(); if (Build.VERSION.SDK_INT < 33 || checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED) startActivity(Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS).putExtra(Settings.EXTRA_APP_PACKAGE, packageName)) }
        button("允许安装应用：系统设置") { startActivity(Intent(Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES, Uri.parse("package:$packageName"))) }
        shell = CheckBox(this).apply {
            text = "允许激活 ADB / shell 辅助进程"; textSize = 15f
            isChecked = controls(this@MainActivity).getBoolean("shell_enabled", false)
            setOnCheckedChangeListener { _, allowed ->
                controls(this@MainActivity).edit().putBoolean("shell_enabled", allowed).apply()
                if (AgentRuntime.running) startService(Intent(this@MainActivity, AgentService::class.java))
                AgentRuntime.changed()
            }
        }; body.addView(shell)
        label("无需另装插件。允许后，用已授权的电脑执行激活命令。重启手机或停止服务后需重新激活。当前版本不内置无线 ADB 配对。", 12)
        button("复制本次 shell 激活命令") { guarded {
            val command = AgentRuntime.bridge?.activationCommand() ?: throw AgentError("请先开启远程控制并允许 shell")
            getSystemService(ClipboardManager::class.java).setPrimaryClip(ClipData.newPlainText("ADB 激活命令（不含令牌）", command)); toast("已复制，请在已授权电脑上执行")
        } }
        val boot = CheckBox(this).apply {
            text = "重启后尝试恢复已开启的连接"; textSize = 14f
            isChecked = controls(this@MainActivity).getBoolean("start_at_boot", false)
            setOnCheckedChangeListener { _, value -> controls(this@MainActivity).edit().putBoolean("start_at_boot", value).apply() }
        }; body.addView(boot)
        label("系统或厂商可能限制后台启动；锁屏和重启后的权限可用性分别显示，不会伪装成已授权。", 12)
        label("共享文件", 19, true)
        label("仅暴露本应用的共享目录。连接密钥、运行数据库和其他应用私有目录不会作为文件工作区开放。", 13)
        button("将文件导入共享目录") { picker(12, "*/*") }
        button("停止并清除连接密钥") {
            AlertDialog.Builder(this).setTitle("清除本机连接配置？").setMessage("不会删除手机其他数据。也可在服务端撤销这台设备的令牌。")
                .setNegativeButton("取消", null).setPositiveButton("清除") { _, _ -> stopRemote(); ConfigStore(this).erase(); gateway.setText(""); device.setText(""); token.setText(""); toast("本机密钥已清除") }.show()
        }
        label("无广告 · 无统计 SDK · HTTPS 连接 · 本机可随时停止", 12)
        guarded { ConfigStore(this).read()?.let { showConfig(it) } }
    }
    private fun dp(value: Int) = (resources.displayMetrics.density * value).toInt()
    private fun label(text: String, size: Int, strong: Boolean = false): TextView = TextView(this).apply {
        this.text = text; textSize = size.toFloat(); setTextColor(if (strong) Color.rgb(24, 39, 65) else Color.rgb(74, 88, 109))
        if (strong) setTypeface(typeface, android.graphics.Typeface.BOLD)
        setPadding(0, dp(if (strong) 17 else 7), 0, dp(6)); body.addView(this)
    }
    private fun field(title: String, hint: String, password: Boolean = false): EditText {
        label(title, 13)
        return EditText(this).apply {
            this.hint = hint; textSize = 15f; setSingleLine(true)
            inputType = if (password) InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_PASSWORD else InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_FLAG_NO_SUGGESTIONS
            importantForAutofill = View.IMPORTANT_FOR_AUTOFILL_NO; setSelectAllOnFocus(false)
            body.addView(this, LinearLayout.LayoutParams(-1, dp(52)))
        }
    }
    private fun button(text: String, action: () -> Unit): Button = Button(this).apply {
        this.text = text; isAllCaps = false; textSize = 15f
        body.addView(this, LinearLayout.LayoutParams(-1, -2).apply { topMargin = dp(7) })
        setOnClickListener { action() }
    }
    private fun showConfig(c: GatewayConfig) { gateway.setText(c.gatewayUrl); device.setText(c.deviceId); token.setText(c.deviceToken); name.setText(c.name) }
    private fun save() {
        demand(!AgentRuntime.running, "请先停止连接，再更改配置")
        val config = GatewayConfig.parse(obj("gateway_url" to gateway.text.toString(), "device_id" to device.text.toString().trim(), "device_token" to token.text.toString().trim(), "name" to name.text.toString().ifBlank { Build.MODEL }))
        ConfigStore(this).write(config)
    }
    private fun stopRemote() { controls(this).edit().putBoolean("remote_enabled", false).apply(); stopService(Intent(this, AgentService::class.java)); AgentRuntime.changed() }
    private fun askNotifications() { if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 40) }
    private fun picker(code: Int, mime: String) { startActivityForResult(Intent(Intent.ACTION_OPEN_DOCUMENT).setType(mime).addCategory(Intent.CATEGORY_OPENABLE), code) }
    @Deprecated("Platform callback retained to avoid an extra activity framework dependency")
    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        super.onActivityResult(requestCode, resultCode, data)
        val uri = data?.data ?: return
        if (resultCode != RESULT_OK) return
        io.submit {
            try {
                if (requestCode == 11) {
                    demand(!AgentRuntime.running, "请先停止连接，再导入配置")
                    val text = contentResolver.openInputStream(uri)?.use { String(Transport.bounded(it, 8192), Charsets.UTF_8) } ?: throw AgentError("无法读取配置")
                    val c = GatewayConfig.parse(org.json.JSONObject(text))
                    ConfigStore(this).write(c)
                    runOnUiThread { showConfig(c); toast("配置已导入并加密保存") }
                } else if (requestCode == 12) {
                    val c = ConfigStore(this).read() ?: throw AgentError("请先保存配置")
                    val root = exposedRoot(this, c); val folder = File(root, "imports").apply { mkdirs() }
                    demand(root.usableSpace > Policy.MAX_FILE + 64L * 1024 * 1024, "存储空间不足")
                    val tmp = File.createTempFile("rh-import-", ".tmp", cacheDir)
                    try {
                        contentResolver.openInputStream(uri)?.use { input -> FileOutputStream(tmp).use { FileTools.copy(input, it, Policy.MAX_FILE) } } ?: throw AgentError("无法读取文件")
                        var displayName = "imported-file"
                        contentResolver.query(uri, arrayOf(android.provider.OpenableColumns.DISPLAY_NAME), null, null, null)?.use { cursor -> if (cursor.moveToFirst()) displayName = cursor.getString(0) ?: displayName }
                        displayName = displayName.replace(Regex("[^A-Za-z0-9._-]"), "_").takeLast(100).ifBlank { "imported-file" }
                        val target = File(folder, nonce().take(8) + "-" + displayName)
                        FileTools.publish(root, "imports/${target.name}", tmp, "absent")
                        runOnUiThread { toast("已导入：imports/${target.name}") }
                    } finally { tmp.delete() }
                }
            } catch (e: Exception) { runOnUiThread { showError(e) } }
        }
    }
    private fun redraw() {
        val rev = AgentRuntime.revision.get()
        if (rev == revision) return
        revision = rev
        status.text = AgentRuntime.state
        capabilities.text = "读屏与操作：${if (AccessService.instance != null) "已授权" else "未授权"}\n" +
            "shell：${if (AgentRuntime.bridge?.connected() == true) "已激活（UID ${AgentRuntime.bridge?.uid}）" else "未激活"}"
        toggle.text = if (AgentRuntime.running) "停止远程控制" else "开启远程控制"
        listOf(gateway, device, token, name).forEach { it.isEnabled = !AgentRuntime.running }
    }
    private fun guarded(block: () -> Unit) { try { block() } catch (e: Exception) { showError(e) } }
    private fun showError(e: Exception) { AlertDialog.Builder(this).setTitle("操作未完成").setMessage(Policy.error(e).optString("message")).setPositiveButton("关闭", null).show() }
    private fun toast(text: String) { Toast.makeText(this, text, Toast.LENGTH_LONG).show() }
    override fun onResume() { super.onResume(); revision = Long.MIN_VALUE; handler.post(refresh) }
    override fun onPause() { handler.removeCallbacks(refresh); super.onPause() }
    override fun onDestroy() { io.shutdownNow(); super.onDestroy() }
}
