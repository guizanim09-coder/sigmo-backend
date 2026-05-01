package com.sigmo.nfc

import android.os.Bundle
import androidx.core.widget.doAfterTextChanged
import com.sigmo.nfc.databinding.ActivityLoginBinding

class LoginActivity : BaseActivity() {
    private lateinit var binding: ActivityLoginBinding
    private var pendingDeepLink: String = ""

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        pendingDeepLink = intent?.getStringExtra(EXTRA_PENDING_DEEP_LINK).orEmpty().trim()

        if (sessionStore.hasSession()) {
            goToHome(pendingDeepLink.takeIf { it.isNotBlank() })
            return
        }

        binding = ActivityLoginBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.loginButton.setOnClickListener { submitLogin() }
        binding.emailInput.doAfterTextChanged { hideInlineError() }
        binding.passwordInput.doAfterTextChanged { hideInlineError() }
    }

    private fun submitLogin() {
        val email = binding.emailInput.text?.toString().orEmpty().trim()
        val password = binding.passwordInput.text?.toString().orEmpty()

        if (email.isBlank() || password.isBlank()) {
            showInlineError("Preencha email e senha para entrar")
            return
        }

        setLoading(true)
        runAsync(
            task = { apiClient.login(email, password) },
            onSuccess = { auth ->
                sessionStore.saveAuth(auth)
                goToHome(pendingDeepLink.takeIf { it.isNotBlank() })
            },
            onError = { throwable ->
                setLoading(false)
                if (!handleThrowable(throwable)) {
                    showInlineError("Nao foi possivel entrar agora")
                } else if (throwable is ApiException) {
                    showInlineError(throwable.apiError.error)
                }
            }
        )
    }

    private fun setLoading(isLoading: Boolean) {
        binding.loginButton.isEnabled = !isLoading
        binding.emailInputLayout.isEnabled = !isLoading
        binding.passwordInputLayout.isEnabled = !isLoading
        if (isLoading) {
            binding.loginButton.text = "Entrando..."
            binding.loadingBar.show()
        } else {
            binding.loginButton.text = "Entrar na Sigmo"
            binding.loadingBar.hide()
        }
    }

    private fun showInlineError(message: String) {
        binding.errorText.text = message
        binding.errorText.show()
    }

    private fun hideInlineError() {
        binding.errorText.hide()
    }
}
