package io.remotehosts.agent

import android.accessibilityservice.AccessibilityService
import android.accessibilityservice.GestureDescription
import android.graphics.Bitmap
import android.graphics.Path
import android.graphics.Rect
import android.os.Bundle
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import android.view.Display
import android.view.accessibility.AccessibilityEvent
import android.view.accessibility.AccessibilityNodeInfo
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit

class AccessService : AccessibilityService() {
    data class NodeRef(val path: List<Int>, val signature: String)
    data class Observation(val id: String, val at: Long, val window: Int, val pkg: String, val nodes: List<NodeRef>)
    private val handler = Handler(Looper.getMainLooper())
    private val pictures = Executors.newSingleThreadExecutor()
    private var observation: Observation? = null
    @Volatile private var windowEpoch = 0L
    override fun onServiceConnected() { instance = this; AgentRuntime.changed() }
    override fun onAccessibilityEvent(event: AccessibilityEvent?) {
        // Only an invalidation counter, never keystrokes or page contents.
        if (event?.eventType == AccessibilityEvent.TYPE_WINDOW_STATE_CHANGED || event?.eventType == AccessibilityEvent.TYPE_WINDOWS_CHANGED) windowEpoch++
    }
    override fun onInterrupt() { observation = null }
    override fun onDestroy() { if (instance === this) instance = null; observation = null; pictures.shutdownNow(); super.onDestroy(); AgentRuntime.changed() }
    private fun enabled() { demand(controls(this).getBoolean("remote_enabled", false) && AgentRuntime.running, "remote_control_stopped_locally") }
    private fun <T> main(block: () -> T): T {
        if (Looper.myLooper() == Looper.getMainLooper()) return block()
        val task = FutureTask { enabled(); block() }
        handler.post(task)
        try { return task.get(5, TimeUnit.SECONDS) }
        catch (e: Exception) { handler.removeCallbacks(task); task.cancel(false); throw e }
    }
    private fun signature(n: AccessibilityNodeInfo): String {
        val b = Rect(); n.getBoundsInScreen(b)
        return listOf(n.windowId.toString(), n.packageName?.toString() ?: "", n.className?.toString() ?: "",
            n.viewIdResourceName ?: "", if (n.isPassword) "<password>" else n.text?.toString()?.take(200) ?: "",
            n.contentDescription?.toString()?.take(200) ?: "", b.flattenToString(), n.isPassword.toString()).joinToString("\u001f")
    }
    fun dump(): JSONObject = main {
        enabled()
        // We intentionally do not subscribe to text-change/keystroke events.
        // The framework cache can therefore outlive a changed label. Refresh
        // on explicit observation instead of continuously collecting contents.
        val freshCache = Build.VERSION.SDK_INT >= 33 && clearCache()
        val root = rootInActiveWindow ?: throw AgentError("screen_tree_unavailable", "当前窗口没有可读取的无障碍节点，或手机处于锁屏状态。")
        val refs = mutableListOf<NodeRef>()
        val nodes = JSONArray()
        var estimate = 0
        var truncated = false
        val epoch = windowEpoch
        val deadline = SystemClock.elapsedRealtime() + 3000
        fun visit(n: AccessibilityNodeInfo, path: List<Int>, depth: Int) {
            if (refs.size >= 300 || estimate > 58000 || depth > 35 || SystemClock.elapsedRealtime() >= deadline) { truncated = true; return }
            // API 30-32 have no public cache invalidation API; refresh only the
            // bounded nodes we actually return. Never silently return stale data.
            if (!freshCache && !n.refresh()) { truncated = true; return }
            val bounds = Rect(); n.getBoundsInScreen(bounds)
            val item = obj("id" to refs.size, "class" to n.className?.toString(), "resource_id" to n.viewIdResourceName,
                "text" to if (n.isPassword) "<redacted>" else n.text?.toString()?.take(180),
                "description" to if (n.isPassword) null else n.contentDescription?.toString()?.take(180),
                "bounds" to arr(listOf(bounds.left, bounds.top, bounds.right, bounds.bottom)), "clickable" to n.isClickable,
                "editable" to n.isEditable, "scrollable" to n.isScrollable, "enabled" to n.isEnabled,
                "visible" to n.isVisibleToUser, "password" to n.isPassword)
            refs += NodeRef(path, signature(n)); nodes.put(item); estimate += item.toString().toByteArray().size
            for (i in 0 until n.childCount.coerceAtMost(300)) {
                if (refs.size >= 300 || estimate > 58000) { truncated = true; break }
                val child = n.getChild(i) ?: continue
                try { visit(child, path + i, depth + 1) } finally { child.recycle() }
            }
        }
        try {
            visit(root, emptyList(), 0)
            val id = nonce().take(24)
            observation = Observation(id, SystemClock.elapsedRealtime(), root.windowId, root.packageName?.toString() ?: "", refs)
            obj("observation_id" to id, "observed_at_ms" to System.currentTimeMillis(), "valid_for_ms" to 30000,
                "package" to root.packageName?.toString(), "window_id" to root.windowId, "window_epoch" to epoch,
                "display" to displayInfo(), "nodes" to nodes, "truncated" to truncated,
                "password_text_redacted" to true, "actions_require_observation_id" to true)
        } finally { root.recycle() }
    }
    private fun displayInfo(): JSONObject {
        val manager = getSystemService(android.hardware.display.DisplayManager::class.java)
        val display = manager.getDisplay(Display.DEFAULT_DISPLAY)
        val metrics = android.util.DisplayMetrics(); display.getRealMetrics(metrics)
        return obj("width" to metrics.widthPixels, "height" to metrics.heightPixels, "rotation" to display.rotation)
    }
    private fun current(args: JSONObject): Pair<Observation, AccessibilityNodeInfo> {
        val seen = observation ?: throw AgentError("observation_required", "先执行 android observe，再使用返回的 observation_id 操作。")
        demand(args.optString("observation_id") == seen.id && SystemClock.elapsedRealtime() - seen.at <= 30000, "stale_observation")
        val root = rootInActiveWindow ?: throw AgentError("screen_tree_unavailable")
        if (root.windowId != seen.window || root.packageName?.toString() != seen.pkg) { root.recycle(); throw AgentError("screen_changed_observe_again") }
        return seen to root
    }
    fun nodeAction(kind: String, args: JSONObject): JSONObject = main {
        val (seen, root) = current(args)
        var node = root
        try {
            val index = args.getInt("node_id")
            demand(index in seen.nodes.indices, "unknown_node_id")
            val ref = seen.nodes[index]
            for (childIndex in ref.path) {
                val next = node.getChild(childIndex) ?: throw AgentError("node_changed_observe_again")
                node.recycle(); node = next
            }
            demand(node.refresh() && signature(node) == ref.signature && node.isEnabled && node.isVisibleToUser, "node_changed_observe_again")
            val accepted = when (kind) {
                "click" -> node.performAction(AccessibilityNodeInfo.ACTION_CLICK)
                "long_click" -> node.performAction(AccessibilityNodeInfo.ACTION_LONG_CLICK)
                "text" -> {
                    demand(node.isEditable, "node_not_editable")
                    val text = args.getString("text"); demand(text.length <= 8192, "text_too_long")
                    node.performAction(AccessibilityNodeInfo.ACTION_SET_TEXT, Bundle().apply { putCharSequence(AccessibilityNodeInfo.ACTION_ARGUMENT_SET_TEXT_CHARSEQUENCE, text) })
                }
                "scroll" -> node.performAction(if (args.optString("direction", "forward") == "backward") AccessibilityNodeInfo.ACTION_SCROLL_BACKWARD else AccessibilityNodeInfo.ACTION_SCROLL_FORWARD)
                else -> throw AgentError("unsupported_node_action")
            }
            demand(accepted, "android_rejected_node_action")
            observation = null
            obj("action" to kind, "accepted" to true, "verified" to false, "next_action" to "android observe")
        } finally { node.recycle() }
    }
    fun gesture(kind: String, args: JSONObject): JSONObject {
        val future = CompletableFuture<Boolean>()
        main {
            val (_, root) = current(args); root.recycle()
            val display = displayInfo(); val width = display.getInt("width"); val height = display.getInt("height")
            fun coord(key: String, upper: Int): Float {
                val v = args.getDouble(key)
                demand(v.isFinite() && v >= 0 && v < upper, "gesture_outside_display")
                return v.toFloat()
            }
            val x = coord("x", width); val y = coord("y", height)
            val path = Path().apply { moveTo(x, y); if (kind == "swipe") lineTo(coord("to_x", width), coord("to_y", height)) }
            val duration = args.optLong("duration_ms", if (kind == "swipe") 400 else 80)
            demand(duration in 30..5000, "invalid_gesture_duration")
            val gesture = GestureDescription.Builder().addStroke(GestureDescription.StrokeDescription(path, 0, duration)).build()
            val submitted = dispatchGesture(gesture, object : GestureResultCallback() {
                override fun onCompleted(gestureDescription: GestureDescription?) { future.complete(true) }
                override fun onCancelled(gestureDescription: GestureDescription?) { future.complete(false) }
            }, handler)
            demand(submitted, "gesture_rejected")
            observation = null
        }
        demand(future.get(7, TimeUnit.SECONDS), "gesture_cancelled")
        return obj("action" to kind, "gesture_completed" to true, "verified" to false, "next_action" to "android observe")
    }
    fun global(action: String): JSONObject = main {
        val code = when (action) {
            "back" -> GLOBAL_ACTION_BACK; "home" -> GLOBAL_ACTION_HOME; "recents" -> GLOBAL_ACTION_RECENTS
            "notifications" -> GLOBAL_ACTION_NOTIFICATIONS; "quick_settings" -> GLOBAL_ACTION_QUICK_SETTINGS
            "lock" -> GLOBAL_ACTION_LOCK_SCREEN
            else -> throw AgentError("unsupported_global_action")
        }
        demand(performGlobalAction(code), "global_action_rejected")
        observation = null
        obj("action" to action, "accepted" to true, "verified" to false, "next_action" to "android observe")
    }
    fun screenshot(root: File, operation: String): JSONObject {
        val observed = dump()
        val directory = File(root, "screenshots").apply { mkdirs() }
        val file = File(directory, "${Policy.uuid(operation)}.png")
        demand(!file.exists(), "screenshot_path_exists")
        val future = CompletableFuture<JSONObject>()
        main {
            takeScreenshot(Display.DEFAULT_DISPLAY, pictures, object : TakeScreenshotCallback {
                override fun onSuccess(result: ScreenshotResult) {
                    val hardware = result.hardwareBuffer
                    try {
                        if (future.isCancelled) return
                        enabled()
                        val source = Bitmap.wrapHardwareBuffer(hardware, result.colorSpace) ?: throw AgentError("screenshot_bitmap_unavailable")
                        val bitmap = try { source.copy(Bitmap.Config.ARGB_8888, false) } finally { source.recycle() }
                        try {
                            FileOutputStream(file).use { output -> demand(bitmap.compress(Bitmap.CompressFormat.PNG, 100, output), "screenshot_encode_failed"); output.fd.sync() }
                            val receipt = obj("path" to "screenshots/${file.name}", "width" to bitmap.width, "height" to bitmap.height,
                                "size" to file.length(), "observation_id" to observed.getString("observation_id"), "package" to observed.opt("package"),
                                "captured_at_ms" to System.currentTimeMillis(), "secure_windows_bypassed" to false)
                            if (!future.complete(receipt)) file.delete()
                        } finally { bitmap.recycle() }
                    } catch (e: Exception) { file.delete(); future.completeExceptionally(e) }
                    finally { hardware.close() }
                }
                override fun onFailure(errorCode: Int) { future.completeExceptionally(AgentError("screenshot_denied_$errorCode", "系统拒绝截图，可能是安全窗口、锁屏或请求间隔过短。")) }
            })
        }
        try { return future.get(10, TimeUnit.SECONDS) }
        catch (e: Exception) { future.cancel(false); throw e }
    }
    companion object { @Volatile var instance: AccessService? = null; private set }
}
