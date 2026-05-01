package com.sigmo.nfc

import android.content.Intent
import android.content.res.ColorStateList
import android.net.Uri
import android.os.Bundle
import androidx.appcompat.app.AlertDialog
import androidx.core.content.ContextCompat
import com.sigmo.nfc.databinding.ActivityHomeBinding

class HomeActivity : BaseActivity() {
    private lateinit var binding: ActivityHomeBinding
    private var balanceVisible = true
    private var lastUser: User? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        if (!ensureLoggedIn()) return

        balanceVisible = getSharedPreferences("sigmo_mobile_ui", MODE_PRIVATE)
            .getBoolean("balance_visible", true)

        binding = ActivityHomeBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.receiveNfcButton.setOnClickListener {
            startActivity(Intent(this, ReceiveNfcActivity::class.java))
        }
        binding.payNfcButton.setOnClickListener {
            startActivity(Intent(this, PayNfcActivity::class.java))
        }
        binding.refreshButton.setOnClickListener {
            loadUser(showRefreshing = true)
        }
        binding.logoutButton.setOnClickListener {
            NfcTapStateStore.clear(applicationContext)
            sessionStore.clear()
            goToLogin()
        }
        binding.toggleBalanceButton.setOnClickListener {
            balanceVisible = !balanceVisible
            getSharedPreferences("sigmo_mobile_ui", MODE_PRIVATE)
                .edit()
                .putBoolean("balance_visible", balanceVisible)
                .apply()
            renderUser(lastUser)
        }
        binding.securityHelpButton.setOnClickListener {
            showDeviceSecurityHelp()
        }

        renderUser(sessionStore.getUser())
        handleDeepLink(intent?.data)
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        handleDeepLink(intent.data)
    }

    override fun onResume() {
        super.onResume()
        loadUser(showRefreshing = false)
    }

    private fun handleDeepLink(data: Uri?) {
        if (data == null) return

        when (data.host.orEmpty()) {
            "tap-receive" -> {
                val chargeId = data.getQueryParameter("chargeId").orEmpty().trim()
                if (chargeId.isBlank()) return

                intent?.data = null
                startActivity(
                    Intent(this, ReceiveNfcActivity::class.java).apply {
                        putExtra(ReceiveNfcActivity.EXTRA_CHARGE_ID, chargeId)
                    }
                )
            }
            "card-claim" -> {
                val cardId = data.getQueryParameter("cardId").orEmpty().trim()
                val claimToken = data.getQueryParameter("claimToken").orEmpty().trim()
                if (cardId.isBlank() || claimToken.isBlank()) return
                intent?.data = null
                claimCard(cardId, claimToken)
            }
        }
    }

    private fun claimCard(cardId: String, claimToken: String) {
        binding.refreshButton.isEnabled = false
        binding.refreshButton.text = "Liberando..."

        runAsync(
            task = { apiClient.claimCard(cardId, claimToken) },
            onSuccess = { user ->
                sessionStore.updateUser(user)
                renderUser(user)
                binding.refreshButton.isEnabled = true
                binding.refreshButton.text = "Atualizar"

                val cardLabel = user.activeCard?.label?.ifBlank { "Cartao Sigmo" } ?: "Cartao Sigmo"
                AlertDialog.Builder(this)
                    .setTitle("Cartao liberado")
                    .setMessage("$cardLabel agora esta pronto para pagamentos por aproximacao neste aparelho.")
                    .setPositiveButton("Continuar", null)
                    .show()
            },
            onError = { throwable ->
                binding.refreshButton.isEnabled = true
                binding.refreshButton.text = "Atualizar"
                handleThrowable(throwable)
            }
        )
    }

    private fun loadUser(showRefreshing: Boolean) {
        if (showRefreshing) {
            binding.refreshButton.isEnabled = false
            binding.refreshButton.text = "Atualizando..."
        }

        refreshUser(
            onSuccess = { user ->
                lastUser = user
                renderUser(user)
                binding.refreshButton.isEnabled = true
                binding.refreshButton.text = "Atualizar"
            },
            onError = { throwable ->
                binding.refreshButton.isEnabled = true
                binding.refreshButton.text = "Atualizar"
                handleThrowable(throwable)
            }
        )
    }

    private fun renderUser(user: User?) {
        lastUser = user
        val securityState = DeviceSecuritySupport.getState(this)
        binding.toggleBalanceButton.text = if (balanceVisible) "Ocultar" else "Mostrar"

        if (user == null) {
            binding.greetingText.text = "Sua conta Sigmo"
            binding.emailText.text = "Entre novamente para carregar seus dados."
            binding.cardUserText.text = "Nenhum cartao liberado"
            binding.cardIdText.text = "----"
            binding.cardFooterText.text = "Liberte um cartao na area Cartoes do app web"
            binding.balanceValueText.text = if (balanceVisible) formatCurrency(0.0) else "R$ *****"
            binding.cardLimitValueText.text = formatCurrency(0.0)
            binding.cardContextValueText.text = "-"
            binding.pixStatusValueText.text = "-"
            binding.deviceSecurityValueText.text = securityState.statusLabel
            binding.securityHelpButton.visibility =
                if (securityState.available) android.view.View.GONE else android.view.View.VISIBLE
            return
        }

        val card = user.activeCard
        val ownerLabel = card?.owner?.nome?.ifBlank { card.owner?.email.orEmpty() }
            ?.ifBlank { user.nome.ifBlank { user.email } }
            ?: user.nome.ifBlank { user.email }
        val displayedBalance = card?.availableToSpend ?: 0.0
        val displayedLimit = card?.spendingLimit ?: 0.0

        binding.greetingText.text = "${firstName(user.nome)}, sua carteira Sigmo"
        binding.emailText.text = user.email
        binding.cardUserText.text = card?.label?.ifBlank { "Cartao Sigmo" } ?: "Nenhum cartao liberado"
        binding.cardIdText.text = (card?.id ?: user.id).takeLast(4).padStart(4, '0')
        binding.balanceValueText.text =
            if (balanceVisible) formatCurrency(displayedBalance) else "R$ *****"
        binding.cardLimitValueText.text = formatCurrency(displayedLimit)
        binding.cardContextValueText.text = ownerLabel
        binding.pixStatusValueText.text = if (user.pixDesbloqueado) "Disponivel" else "Bloqueado"
        binding.deviceSecurityValueText.text = securityState.statusLabel
        binding.securityHelpButton.visibility =
            if (securityState.available) android.view.View.GONE else android.view.View.VISIBLE

        if (user.contaBanida) {
            binding.accountStatusChip.text = "Conta banida"
            binding.accountStatusChip.backgroundTintList = ColorStateList.valueOf(
                ContextCompat.getColor(this, R.color.sigmo_danger)
            )
            binding.accountHintText.text =
                "Saldo congelado e movimentacoes indisponiveis por banimento permanente."
            binding.cardFooterText.text = "Carteira congelada por tentativa de fraude"
            binding.receiveNfcButton.isEnabled = false
            binding.payNfcButton.isEnabled = false
            return
        }

        binding.accountStatusChip.text = if (card?.cardType == "additional") {
            "Cartao adicional"
        } else {
            "Conta ativa"
        }
        binding.accountStatusChip.backgroundTintList = ColorStateList.valueOf(
            ContextCompat.getColor(this, R.color.sigmo_primary_dark)
        )

        when {
            card == null -> {
                binding.accountHintText.text =
                    "Use a area Cartoes no app web para definir um limite e liberar um cartao neste aparelho."
                binding.cardFooterText.text = "Sem cartao liberado para pagar por aproximacao"
                binding.receiveNfcButton.isEnabled = true
                binding.payNfcButton.isEnabled = false
            }
            !securityState.available -> {
                binding.accountHintText.text =
                    "Ative biometria, facial ou a senha do aparelho para autorizar pagamentos por aproximacao."
                binding.cardFooterText.text = "Limite definido no web: ${formatCurrency(displayedLimit)}"
                binding.receiveNfcButton.isEnabled = true
                binding.payNfcButton.isEnabled = false
            }
            else -> {
                binding.accountHintText.text =
                    "O app paga por aproximacao usando apenas o saldo disponivel e o limite liberado na area Cartoes do web."
                binding.cardFooterText.text =
                    "Saldo do cartao agora: ${formatCurrency(displayedBalance)} de ${formatCurrency(displayedLimit)}"
                binding.receiveNfcButton.isEnabled = true
                binding.payNfcButton.isEnabled = displayedBalance > 0
            }
        }
    }
}
