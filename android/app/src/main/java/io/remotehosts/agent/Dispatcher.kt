package io.remotehosts.agent

import android.content.Context
import android.os.Build
import org.json.JSONObject
import java.io.File
import java.util.UUID

class Dispatcher(private val context: Context, private val config: GatewayConfig, private val journal: Journal, private val transport: Transport) {
    private val root = exposedRoot(context, config)
    private val files = FileTools(context, transport)
    private val packages = PackageActions(context)
    fun perform(job: JSONObject): JSONObject {
        demand(AgentRuntime.running && controls(context).getBoolean("remote_enabled", false), "stopped_locally")
        demand(job.getString("device_id") == config.deviceId, "wrong_device")
        val args = job.getJSONObject("arguments"); val owner = job.getString("owner"); val tool = job.getString("tool"); val id = Policy.uuid(job.getString("id"))
        if (tool == "workspace_open") {
            demand(args.getString("device_id") == config.deviceId && File(args.getString("root")).canonicalFile == root, "root_not_authorized")
            val ws = config.deviceId + ":" + UUID.randomUUID()
            journal.putWorkspace(ws, owner, root)
            return obj("workspace" to obj("id" to ws, "device_id" to config.deviceId, "root" to root.path), "allow_write" to true,
                "allow_exec" to true, "agent_version" to BuildConfig.VERSION_NAME, "file_transfer" to true,
                "terminal_authority" to "Android command dispatcher; android help lists commands; shell requires explicit local activation")
        }
        val ws = args.getString("workspace_id")
        demand(journal.workspace(ws, owner).canonicalFile == root, "workspace_root_changed")
        return when (tool) {
            "terminal_exec" -> terminal(job, args, ws, owner, id)
            "terminal_read" -> {
                val saved = journal.terminal(args.getString("terminal_id"), ws, owner)
                val raw = saved.getString("output").toByteArray(); val cursor = args.optInt("cursor", 0)
                demand(cursor in 0..raw.size, "invalid_terminal_cursor")
                var end = minOf(raw.size, cursor + args.optInt("max_bytes", 32768).coerceIn(1024, 65536))
                while (end < raw.size && end > cursor && (raw[end].toInt() and 0xc0) == 0x80) end--
                saved.put("output", String(raw, cursor, end - cursor, Charsets.UTF_8)).put("cursor", end).put("has_more", end < raw.size)
                    .put("raw_cursor_start", cursor).put("next_action", if (end < raw.size) "terminal_read" else JSONObject.NULL)
            }
            "terminal_cancel" -> {
                val saved = journal.terminal(args.getString("terminal_id"), ws, owner)
                obj("state" to "already_exited", "terminal_id" to saved.getString("terminal_id"), "cancelled" to false)
            }
            "workspace_context" -> obj("workspace" to obj("id" to ws, "root" to root.path), "runtime" to status(), "journal" to journal.counts(),
                "supported_tools" to arr(listOf("workspace_open", "workspace_context", "terminal_exec", "terminal_read", "terminal_cancel", "code_list", "code_read", "file_upload", "file_download")),
                "workspace_gc_supported" to false, "help" to "terminal_exec command: android help")
            "file_upload" -> files.upload(root, args, id)
            "file_download" -> files.download(root, args, id)
            "code_list" -> files.list(root, args)
            "code_read" -> files.read(root, args)
            else -> throw AgentError("unsupported_android_tool", "Use terminal_exec with android help. This APK does not advertise desktop code-editing or resumable-transfer features.")
        }
    }
    private fun terminal(job: JSONObject, args: JSONObject, ws: String, owner: String, id: String): JSONObject {
        demand(!args.optBoolean("pty", false), "android_pty_not_supported")
        val created = now()
        val value = try { command(args.getString("command"), id) } catch (e: Exception) { Policy.error(e) }
        val output = value.toString() + "\n"
        demand(output.toByteArray().size <= Policy.MAX_OUTPUT, "terminal_output_budget_exceeded")
        val unknown = value.optString("state").contains("unknown") || value.optString("error").contains("unknown")
        val exit: Int? = if (unknown) null else if (value.has("error")) 1 else value.optInt("exit_code", 0)
        val terminal = obj("id" to id, "workspace_id" to ws, "state" to if (unknown) "runtime_lost" else "exited", "exit_code" to exit,
            "output_truncated" to false, "created_at" to created, "updated_at" to now(), "process_id" to null,
            "working_directory" to root.path, "pty" to false, "log_format" to 1, "output_complete" to true)
        val result = obj("terminal_id" to id, "state" to terminal.getString("state"), "terminal" to terminal, "output" to output,
            "output_stream" to "combined", "output_view" to "full", "cursor" to output.toByteArray().size, "raw_cursor_start" to 0,
            "cursor_format" to "sanitized_utf8_v1", "has_more" to false, "pty" to false, "next_action" to null)
        if (unknown) result.put("error", "outcome_unknown").put("automatic_replay", false)
        journal.saveTerminal(id, ws, owner, result)
        return result
    }
    private fun screen(): AccessService = AccessService.instance ?: throw AgentError("accessibility_permission_required", "在手机上启用 Remote Hosts 无障碍服务后重试。")
    private fun bridge(): ShellBridge = AgentRuntime.bridge?.takeIf { it.connected() } ?: throw AgentError("shell_activation_required")
    fun status(): JSONObject = obj("platform" to "android", "android_sdk" to Build.VERSION.SDK_INT, "android_version" to Build.VERSION.RELEASE,
        "manufacturer" to Build.MANUFACTURER, "model" to Build.MODEL, "agent_version" to BuildConfig.VERSION_NAME,
        "remote_enabled" to controls(context).getBoolean("remote_enabled", false), "accessibility" to (AccessService.instance != null),
        "shell_enabled_locally" to controls(context).getBoolean("shell_enabled", false), "shell_connected" to (AgentRuntime.bridge?.connected() == true),
        "shell_uid" to AgentRuntime.bridge?.uid, "shared_root" to root.path, "max_file_bytes" to Policy.MAX_FILE,
        "permissions_are_independent" to true, "transfer_resume" to false, "continuous_recording" to false)
    private fun command(text: String, id: String): JSONObject {
        demand(text.length <= 65536 && text.startsWith("android "), "android_command_required", "Use android help; arbitrary desktop shell text is not executed on the phone.")
        val command = text.removePrefix("android ").trim()
        val space = command.indexOf(' '); val name = if (space < 0) command else command.substring(0, space)
        val args = if (space < 0) obj() else JSONObject(command.substring(space + 1).trim())
        return when (name) {
            "help" -> obj("protocol" to 1, "syntax" to "android VERB {JSON arguments}", "commands" to obj(
                "status" to "Local capability and permission states", "observe" to "Read visible UI tree; returns observation_id + node IDs (30s lifetime)",
                "click / long_click" to "{observation_id,node_id}", "text" to "{observation_id,node_id,text}", "scroll" to "{observation_id,node_id,direction:forward|backward}",
                "tap" to "{observation_id,x,y,duration_ms?}", "swipe" to "{observation_id,x,y,to_x,to_y,duration_ms?}",
                "screenshot" to "PNG in shared workspace; use file_download(path) afterwards", "back / home / recents / notifications / quick_settings / lock" to "No arguments",
                "apps" to "{offset?:0,limit?:80}", "launch" to "{package}", "install" to "{path,sha256?}; shell bridge or local notification confirmation",
                "shell" to "{command,timeout_seconds?:30}; requires locally activated ADB/shell bridge", "pull" to "{source:absolute_shell_readable_path,path:shared_relative_destination}; requires shell bridge"),
                "safety" to "Actions are not replayed after uncertain interruption. Re-observe to verify. System security boundaries remain enforced.")
            "status" -> status()
            "observe" -> screen().dump()
            "click", "long_click", "text", "scroll" -> screen().nodeAction(name, args)
            "tap", "swipe" -> screen().gesture(name, args)
            "back", "home", "recents", "notifications", "quick_settings", "lock" -> screen().global(name)
            "screenshot" -> {
                files.pruneScreenshots(root)
                demand((File(root, "screenshots").listFiles()?.sumOf { it.length() } ?: 0) < 128L * 1024 * 1024 && root.usableSpace > 128L * 1024 * 1024, "screenshot_storage_limit")
                screen().screenshot(root, id)
            }
            "apps" -> packages.apps(args)
            "launch" -> packages.launch(args)
            "install" -> packages.install(root, args)
            "shell" -> bridge().execute(obj("op" to "shell", "command" to args.getString("command"), "timeout_seconds" to args.optInt("timeout_seconds", 30)))
            "pull" -> {
                val file = Policy.within(root, args.getString("path")); demand(!file.exists(), "destination_exists")
                file.parentFile!!.mkdirs(); demand(root.usableSpace > Policy.MAX_FILE + 64L * 1024 * 1024, "storage_reserve_insufficient")
                val tmp = File(file.parentFile, ".rh-pull-$id.tmp"); demand(tmp.createNewFile(), "staging_conflict")
                try {
                    val result = bridge().execute(obj("op" to "pull", "path" to args.getString("source")), outputFile = tmp)
                    demand(result.optInt("exit_code", -1) == 0 && result.has("sha256"), "shell_file_read_failed")
                    demand(Policy.within(root, args.getString("path")) == file, "destination_changed")
                    FileTools.publish(root, args.getString("path"), tmp, "absent")
                    result.put("path", args.getString("path")).put("size", file.length())
                } finally { tmp.delete() }
            }
            else -> throw AgentError("unknown_android_command")
        }
    }
}
