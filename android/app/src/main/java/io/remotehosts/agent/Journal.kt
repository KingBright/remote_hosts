package io.remotehosts.agent

import android.content.ContentValues
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import org.json.JSONObject
import java.io.File

/** A side effect is journaled BEFORE execution. Network retries only resend a saved result. */
class Journal(context: Context, val config: GatewayConfig) : SQLiteOpenHelper(context, "agent-${config.identity().take(24)}.db", null, 1) {
    init { setWriteAheadLoggingEnabled(true) }
    override fun onConfigure(db: SQLiteDatabase) { db.execSQL("PRAGMA synchronous=FULL") }
    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL("CREATE TABLE operations(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,owner TEXT NOT NULL,state TEXT NOT NULL,result TEXT,acked INTEGER NOT NULL DEFAULT 0,created INTEGER NOT NULL,updated INTEGER NOT NULL)")
        db.execSQL("CREATE INDEX operation_delivery ON operations(acked,state,updated)")
        db.execSQL("CREATE TABLE workspaces(id TEXT PRIMARY KEY,owner TEXT NOT NULL,root TEXT NOT NULL)")
        db.execSQL("CREATE TABLE terminals(id TEXT PRIMARY KEY,workspace TEXT NOT NULL,owner TEXT NOT NULL,result TEXT NOT NULL,created INTEGER NOT NULL)")
    }
    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) { throw AgentError("unsupported_journal_version") }
    @Synchronized fun recover() {
        val result = obj("error" to "outcome_unknown", "state" to "outcome_unknown", "automatic_replay" to false,
            "message" to "Agent stopped during this operation. Observe current state; never automatically repeat it.")
        writableDatabase.execSQL("UPDATE operations SET state='done',result=?,updated=? WHERE state='running'", arrayOf<Any>(result.toString(), now()))
        prune()
    }
    /** null means newly claimed; an object means already completed/uncertain and MUST NOT execute. */
    @Synchronized fun claim(job: JSONObject): JSONObject? {
        Policy.uuid(job.getString("id"))
        demand(job.getString("device_id") == config.deviceId, "wrong_device")
        val hash = sha256(canonicalJson(job).toByteArray())
        val db = writableDatabase
        db.beginTransaction()
        try {
            db.rawQuery("SELECT fingerprint,state,result,acked FROM operations WHERE id=?", arrayOf(job.getString("id"))).use { c ->
                if (c.moveToFirst()) {
                    demand(c.getString(0) == hash, "operation_fingerprint_conflict")
                    val saved = if (!c.isNull(2)) JSONObject(c.getString(2)) else obj("state" to "already_completed", "operation_id" to job.getString("id"), "result_owner" to "gateway")
                    db.setTransactionSuccessful()
                    return saved
                }
            }
            db.rawQuery("SELECT COUNT(*) FROM operations WHERE acked=0", null).use { it.moveToFirst(); demand(it.getInt(0) < 512, "receipt_backlog_limit") }
            val v = ContentValues().apply {
                put("id", job.getString("id")); put("fingerprint", hash); put("owner", job.getString("owner"))
                put("state", "running"); put("created", now()); put("updated", now())
            }
            db.insertOrThrow("operations", null, v)
            db.setTransactionSuccessful()
            return null
        } finally { db.endTransaction() }
    }
    @Synchronized fun finish(id: String, result: JSONObject) {
        val body = result.toString()
        demand(body.toByteArray().size <= 240 * 1024, "receipt_budget_exceeded")
        writableDatabase.execSQL("UPDATE operations SET state='done',result=?,updated=? WHERE id=? AND state='running'", arrayOf<Any>(body, now(), id))
    }
    @Synchronized fun pending(): List<Pair<String, JSONObject>> {
        val results = mutableListOf<Pair<String, JSONObject>>()
        readableDatabase.rawQuery("SELECT id,result FROM operations WHERE acked=0 AND state='done' ORDER BY updated LIMIT 16", null).use { c ->
            while (c.moveToNext()) results += c.getString(0) to JSONObject(c.getString(1))
        }
        return results
    }
    @Synchronized fun ack(id: String) {
        writableDatabase.execSQL("UPDATE operations SET acked=1,result=NULL,updated=? WHERE id=? AND state='done'", arrayOf<Any>(now(), id))
    }
    @Synchronized fun counts(): JSONObject {
        val result = obj()
        readableDatabase.rawQuery("SELECT state,acked,COUNT(*) FROM operations GROUP BY state,acked", null).use { c ->
            while (c.moveToNext()) result.put(c.getString(0) + if (c.getInt(1) == 1) "_acked" else "_pending", c.getInt(2))
        }
        return result
    }
    @Synchronized fun workspace(id: String, owner: String): File {
        readableDatabase.rawQuery("SELECT owner,root FROM workspaces WHERE id=?", arrayOf(id)).use { c ->
            demand(c.moveToFirst(), "workspace_not_found")
            demand(c.getString(0) == owner && id.startsWith(config.deviceId + ":"), "workspace_owner_mismatch")
            return File(c.getString(1))
        }
    }
    @Synchronized fun putWorkspace(id: String, owner: String, root: File) {
        val v = ContentValues().apply { put("id", id); put("owner", owner); put("root", root.path) }
        writableDatabase.insertOrThrow("workspaces", null, v)
    }
    @Synchronized fun saveTerminal(id: String, ws: String, owner: String, result: JSONObject) {
        val v = ContentValues().apply { put("id", id); put("workspace", ws); put("owner", owner); put("result", result.toString()); put("created", now()) }
        writableDatabase.insertOrThrow("terminals", null, v)
    }
    @Synchronized fun terminal(id: String, ws: String, owner: String): JSONObject {
        readableDatabase.rawQuery("SELECT workspace,owner,result FROM terminals WHERE id=?", arrayOf(Policy.uuid(id))).use { c ->
            demand(c.moveToFirst(), "terminal_unavailable_or_expired")
            demand(c.getString(0) == ws && c.getString(1) == owner, "terminal_owner_mismatch")
            return JSONObject(c.getString(2))
        }
    }
    @Synchronized fun prune() {
        // Unacknowledged and interrupted operations are NEVER removed automatically.
        writableDatabase.execSQL("DELETE FROM operations WHERE acked=1 AND updated<?", arrayOf<Any>(now() - 86400 * 7))
        writableDatabase.execSQL("DELETE FROM terminals WHERE id IN (SELECT t.id FROM terminals t JOIN operations o ON o.id=t.id WHERE o.acked=1 ORDER BY t.created DESC LIMIT -1 OFFSET 200)")
        writableDatabase.execSQL("DELETE FROM terminals WHERE created<? AND id NOT IN (SELECT id FROM operations WHERE acked=0)", arrayOf<Any>(now() - 86400))
    }
}
