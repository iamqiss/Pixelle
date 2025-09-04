package xyz.sonet.app.utils

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

class KeychainUtils(private val context: Context) {
    
    private val keyStore = KeyStore.getInstance("AndroidKeyStore").apply {
        load(null)
    }
    
    companion object {
        private const val KEY_ALIAS = "sonet_auth_token"
        private const val TRANSFORMATION = "AES/GCM/NoPadding"
    }
    
    fun storeAuthToken(token: String) {
        try {
            val secretKey = getOrCreateSecretKey()
            val cipher = Cipher.getInstance(TRANSFORMATION)
            cipher.init(Cipher.ENCRYPT_MODE, secretKey)
            
            val encryptedData = cipher.doFinal(token.toByteArray())
            val combined = cipher.iv + encryptedData
            
            // Store in SharedPreferences (in production, use EncryptedSharedPreferences)
            context.getSharedPreferences("sonet_secure", Context.MODE_PRIVATE)
                .edit()
                .putString("auth_token", android.util.Base64.encodeToString(combined, android.util.Base64.DEFAULT))
                .apply()
                
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
    
    fun getAuthToken(): String? {
        return try {
            val encryptedData = context.getSharedPreferences("sonet_secure", Context.MODE_PRIVATE)
                .getString("auth_token", null) ?: return null
            
            val combined = android.util.Base64.decode(encryptedData, android.util.Base64.DEFAULT)
            val iv = combined.copyOfRange(0, 12)
            val encrypted = combined.copyOfRange(12, combined.size)
            
            val secretKey = getOrCreateSecretKey()
            val cipher = Cipher.getInstance(TRANSFORMATION)
            val spec = GCMParameterSpec(128, iv)
            cipher.init(Cipher.DECRYPT_MODE, secretKey, spec)
            
            String(cipher.doFinal(encrypted))
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }
    
    fun clearAuthToken() {
        try {
            context.getSharedPreferences("sonet_secure", Context.MODE_PRIVATE)
                .edit()
                .remove("auth_token")
                .apply()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    // Refresh token helpers
    fun storeAuthRefreshToken(token: String) {
        try {
            val secretKey = getOrCreateSecretKey()
            val cipher = Cipher.getInstance(TRANSFORMATION)
            cipher.init(Cipher.ENCRYPT_MODE, secretKey)
            val encryptedData = cipher.doFinal(token.toByteArray())
            val combined = cipher.iv + encryptedData
            context.getSharedPreferences("sonet_secure", Context.MODE_PRIVATE)
                .edit()
                .putString("refresh_token", android.util.Base64.encodeToString(combined, android.util.Base64.DEFAULT))
                .apply()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    fun getAuthRefreshToken(): String? {
        return try {
            val encryptedData = context.getSharedPreferences("sonet_secure", Context.MODE_PRIVATE)
                .getString("refresh_token", null) ?: return null
            val combined = android.util.Base64.decode(encryptedData, android.util.Base64.DEFAULT)
            val iv = combined.copyOfRange(0, 12)
            val encrypted = combined.copyOfRange(12, combined.size)
            val secretKey = getOrCreateSecretKey()
            val cipher = Cipher.getInstance(TRANSFORMATION)
            val spec = GCMParameterSpec(128, iv)
            cipher.init(Cipher.DECRYPT_MODE, secretKey, spec)
            String(cipher.doFinal(encrypted))
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }

    fun clearAuthRefreshToken() {
        try {
            context.getSharedPreferences("sonet_secure", Context.MODE_PRIVATE)
                .edit()
                .remove("refresh_token")
                .apply()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
    
    private fun getOrCreateSecretKey(): SecretKey {
        return if (keyStore.containsAlias(KEY_ALIAS)) {
            keyStore.getKey(KEY_ALIAS, null) as SecretKey
        } else {
            createSecretKey()
        }
    }
    
    private fun createSecretKey(): SecretKey {
        val keyGenerator = KeyGenerator.getInstance(
            KeyProperties.KEY_ALGORITHM_AES,
            "AndroidKeyStore"
        )
        
        val keyGenSpec = KeyGenParameterSpec.Builder(
            KEY_ALIAS,
            KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
        )
            .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
            .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
            .setUserAuthenticationRequired(false)
            .build()
        
        keyGenerator.init(keyGenSpec)
        return keyGenerator.generateKey()
    }
}