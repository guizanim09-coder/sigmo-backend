package com.sigmo.nfc

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import kotlin.concurrent.thread

abstract class BaseActivity : AppCompatActivity() {
    protected lateinit var sessionStore: SessionStore
    protected lateinit var deviceIdentityStore: DeviceIdentityStore
    protected lateinit var apiClient: ApiClient

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        sessionStore = SessionStore(applicationContext)
        deviceIdentityStore = DeviceIdentityStore(applicationContext)
        apiClient = ApiClient(sessionStore, deviceIdentityStore)
    }

    protected fun ensureLoggedIn(): Boolean {
        if (sessionStore.getToken().isBlank()) {
            goToLogin(intent?.dataString)
            return false
        }
        return true
    }

    protected fun goToLogin(pendingDeepLink: String? = null) {
        startActivity(
            Intent(this, LoginActivity::class.java).apply {
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK)
                pendingDeepLink
                    ?.trim()
                    ?.takeIf { it.isNotBlank() }
                    ?.let { putExtra(EXTRA_PENDING_DEEP_LINK, it) }
            }
        )
        finish()
    }

    protected fun goToHome(pendingDeepLink: String? = null) {
        startActivity(
            Intent(this, HomeActivity::class.java).apply {
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK)
                pendingDeepLink
                    ?.trim()
                    ?.takeIf { it.isNotBlank() }
                    ?.let {
                        action = Intent.ACTION_VIEW
                        data = Uri.parse(it)
                    }
            }
        )
        finish()
    }

    protected fun showDeviceSecurityHelp() {
        AlertDialog.Builder(this)
            .setTitle("Protecao do aparelho")
            .setMessage(DeviceSecuritySupport.helpMessage())
            .setPositiveButton("Entendi", null)
            .show()
    }

    protected fun <T> runAsync(
        task: () -> T,
        onSuccess: (T) -> Unit,
        onError: (Throwable) -> Unit = { handleThrowable(it) }
    ) {
        thread(start = true) {
            try {
                val result = task()
                runOnUiThread {
                    if (!isDestroyed && !isFinishing) {
                        onSuccess(result)
                    }
                }
            } catch (throwable: Throwable) {
                runOnUiThread {
                    if (!isDestroyed && !isFinishing) {
                        onError(throwable)
                    }
                }
            }
        }
    }

    protected fun refreshUser(
        onSuccess: (User) -> Unit = {},
        onError: (Throwable) -> Unit = { handleThrowable(it) }
    ) {
        runAsync(
            task = { apiClient.getMe() },
            onSuccess = { user ->
                sessionStore.updateUser(user)
                onSuccess(user)
            },
            onError = onError
        )
    }

    protected fun handleThrowable(throwable: Throwable): Boolean {
        return when (throwable) {
            is ApiException -> handleApiError(throwable.apiError)
            else -> {
                Toast.makeText(
                    this,
                    throwable.message ?: "Nao foi possivel concluir a operacao agora",
                    Toast.LENGTH_LONG
                ).show()
                true
            }
        }
    }

    private fun handleApiError(apiError: ApiError): Boolean {
        if (apiError.statusCode == 401) {
            sessionStore.clear()
            NfcTapStateStore.clear(applicationContext)
            Toast.makeText(this, "Sua sessao expirou. Entre novamente.", Toast.LENGTH_LONG).show()
            goToLogin()
            return true
        }

        if (apiError.isAccountBanned) {
            sessionStore.mergeBanState(apiError)
            AlertDialog.Builder(this)
                .setTitle("Conta banida")
                .setMessage(apiError.error)
                .setCancelable(false)
                .setPositiveButton("Entendi") { _, _ ->
                    goToHome()
                }
                .show()
            return true
        }

        Toast.makeText(this, apiError.error, Toast.LENGTH_LONG).show()
        return true
    }

    companion object {
        const val EXTRA_PENDING_DEEP_LINK = "pending_deep_link"
    }
}
