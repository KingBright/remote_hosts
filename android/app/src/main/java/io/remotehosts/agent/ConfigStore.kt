package io.remotehosts.agent

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.AtomicFile
import java.io.File
import java.nio.ByteBuffer
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import org.json.JSONObject

class ConfigStore(private val context: Context) {
    private val file = AtomicFile(File(context.noBackupFilesDir, "gateway-config.enc"))
    private fun key(): SecretKey {
        val ks = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }
        val alias = "remote-hosts-gateway-v1"
        val existing = ks.getKey(alias, null)
        if (existing is SecretKey) return existing
        return KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore").apply {
            init(KeyGenParameterSpec.Builder(alias, KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM).setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256).build())
        }.generateKey()
    }
    @Synchronized fun read(): GatewayConfig? {
        if (!file.baseFile.exists()) return null
        try {
            val bytes = file.readFully()
            demand(bytes.size in 30..8192 && bytes[0] == 1.toByte(), "invalid_config_envelope")
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            cipher.init(Cipher.DECRYPT_MODE, key(), GCMParameterSpec(128, bytes.copyOfRange(1, 13)))
            cipher.updateAAD(context.packageName.toByteArray())
            return GatewayConfig.parse(JSONObject(String(cipher.doFinal(bytes.copyOfRange(13, bytes.size)), Charsets.UTF_8)))
        } catch (_: Exception) { throw AgentError("config_unreadable", "配置无法解密，请重新导入设备配置。") }
    }
    @Synchronized fun write(config: GatewayConfig) {
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, key())
        cipher.updateAAD(context.packageName.toByteArray())
        val encrypted = cipher.doFinal(config.json().toString().toByteArray())
        val bytes = ByteBuffer.allocate(1 + cipher.iv.size + encrypted.size).put(1.toByte()).put(cipher.iv).put(encrypted).array()
        val stream = file.startWrite()
        try { stream.write(bytes); file.finishWrite(stream) } catch (e: Exception) { file.failWrite(stream); throw e }
    }
    fun erase() { file.delete() }
}
fun controls(context: Context) = context.getSharedPreferences("local-controls", Context.MODE_PRIVATE)
fun exposedRoot(context: Context, config: GatewayConfig): File = File(context.filesDir, "shared/${config.identity().take(20)}").apply { mkdirs() }.canonicalFile
