package com.sigmo.nfc

import android.app.KeyguardManager
import android.content.Context
import android.os.Build
import androidx.biometric.BiometricManager
import androidx.core.content.ContextCompat

data class DeviceSecurityState(
    val available: Boolean,
    val statusLabel: String,
    val hint: String
)

object DeviceSecuritySupport {
    private const val DEFAULT_HINT =
        "Ative biometria, facial ou a senha do aparelho nas configuracoes do Android para autorizar pagamentos por aproximacao."

    fun getState(context: Context): DeviceSecurityState {
        val hasSecureLock = hasSecureLockScreen(context)
        val hasBiometric = hasStrongBiometric(context)
        val available = hasSecureLock || hasBiometric

        val label = when {
            hasBiometric && hasSecureLock -> "Biometria e bloqueio ativos"
            hasBiometric -> "Biometria ativa"
            hasSecureLock -> "Senha do aparelho ativa"
            else -> "Protecao nao configurada"
        }

        val hint = if (available) {
            "Os pagamentos por aproximacao serao confirmados com a protecao do seu aparelho."
        } else {
            DEFAULT_HINT
        }

        return DeviceSecurityState(
            available = available,
            statusLabel = label,
            hint = hint
        )
    }

    fun helpMessage(): String = DEFAULT_HINT

    private fun hasSecureLockScreen(context: Context): Boolean {
        val keyguardManager = ContextCompat.getSystemService(context, KeyguardManager::class.java)
        return keyguardManager?.isDeviceSecure == true
    }

    private fun hasStrongBiometric(context: Context): Boolean {
        val biometricManager = BiometricManager.from(context)
        val authenticators = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            BiometricManager.Authenticators.BIOMETRIC_STRONG
        } else {
            0
        }

        val result = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            biometricManager.canAuthenticate(authenticators)
        } else {
            biometricManager.canAuthenticate()
        }

        return result == BiometricManager.BIOMETRIC_SUCCESS
    }
}
