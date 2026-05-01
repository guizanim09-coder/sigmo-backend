package com.sigmo.nfc

import android.content.res.ColorStateList
import android.nfc.NfcAdapter
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import androidx.appcompat.app.AlertDialog
import androidx.core.content.ContextCompat
import com.sigmo.nfc.databinding.ActivityReceiveNfcBinding
import kotlin.concurrent.thread

class ReceiveNfcActivity : BaseActivity() {
    private lateinit var binding: ActivityReceiveNfcBinding
    private val handler = Handler(Looper.getMainLooper())

    private var currentSession: NfcSession? = null
    private var currentCharge: TapCharge? = null
    private var isPolling = false
    private var pollEnabled = false
    private var sessionClosedManually = false
    private var successDialogShown = false
    private var chargeId: String = ""

    private val pollRunnable = object : Runnable {
        override fun run() {
            pollSession()
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        if (!ensureLoggedIn()) return

        chargeId = intent.getStringExtra(EXTRA_CHARGE_ID).orEmpty().trim()

        binding = ActivityReceiveNfcBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.backButton.setOnClickListener { finish() }
        binding.renewButton.setOnClickListener { createSession() }
        binding.closeButton.setOnClickListener { closeSessionAndFinish() }

        renderUser(sessionStore.getUser())

        if (!canReceiveByNfc()) {
            renderUnavailableState(
                "Este celular nao suporta receber por aproximacao com HCE."
            )
            return
        }

        val user = sessionStore.getUser()
        if (user?.contaBanida == true) {
            renderUnavailableState("Conta banida. O recebimento por aproximacao ficou indisponivel.")
            return
        }

        createSession()
    }

    override fun onStart() {
        super.onStart()
        pollEnabled = true
        schedulePoll(1200)
    }

    override fun onStop() {
        super.onStop()
        pollEnabled = false
        handler.removeCallbacks(pollRunnable)
    }

    override fun onDestroy() {
        super.onDestroy()
        handler.removeCallbacks(pollRunnable)
        val session = currentSession
        if (isFinishing && session != null && session.status == "pending" && !sessionClosedManually) {
            NfcTapStateStore.clear(applicationContext)
            thread(start = true) {
                runCatching { apiClient.cancelReceiveSession(session.id) }
            }
        }
    }

    private fun renderUser(user: User?) {
        val userLabel = user?.let { it.nome.ifBlank { it.email } } ?: "Recebedor"
        binding.receiverNameText.text = userLabel
        binding.receiverEmailText.text = user?.email ?: "Conta Sigmo"
    }

    private fun createSession() {
        sessionClosedManually = false
        binding.renewButton.isEnabled = false
        binding.renewButton.text = "Preparando..."
        binding.progressBar.show()

        runAsync(
            task = {
                if (chargeId.isNotBlank()) {
                    apiClient.armTapCharge(chargeId)
                } else {
                    null to apiClient.createReceiveSession()
                }
            },
            onSuccess = { result ->
                val charge = result.first
                val session = result.second
                binding.progressBar.hide()
                binding.renewButton.isEnabled = true
                binding.renewButton.text = if (chargeId.isNotBlank()) "Reativar aproximacao" else "Gerar nova sessao"
                currentCharge = charge
                currentSession = session
                successDialogShown = false
                NfcTapStateStore.saveActiveSession(applicationContext, session)
                renderSession(session)
                schedulePoll(1200)
            },
            onError = { throwable ->
                binding.progressBar.hide()
                binding.renewButton.isEnabled = true
                binding.renewButton.text = "Tentar novamente"
                handleThrowable(throwable)
            }
        )
    }

    private fun closeSessionAndFinish() {
        val session = currentSession
        sessionClosedManually = true
        NfcTapStateStore.clear(applicationContext)

        if (session == null || session.status != "pending") {
            finish()
            return
        }

        binding.closeButton.isEnabled = false
        binding.closeButton.text = "Encerrando..."
        runAsync(
            task = { apiClient.cancelReceiveSession(session.id) },
            onSuccess = {
                finish()
            },
            onError = { throwable ->
                binding.closeButton.isEnabled = true
                binding.closeButton.text = "Encerrar sessao"
                handleThrowable(throwable)
            }
        )
    }

    private fun pollSession() {
        val sessionId = currentSession?.id ?: return
        if (!pollEnabled || isPolling || currentSession?.status != "pending") return

        isPolling = true
        runAsync(
            task = { apiClient.getReceiveSession(sessionId) },
            onSuccess = { session ->
                isPolling = false
                currentSession = session
                currentCharge = session.charge ?: currentCharge
                renderSession(session)
                when (session.status) {
                    "pending" -> schedulePoll(1800)
                    "consumed" -> {
                        NfcTapStateStore.clear(applicationContext)
                        refreshUser()
                        maybeShowSuccessDialog(session)
                    }
                    else -> NfcTapStateStore.clear(applicationContext)
                }
            },
            onError = { throwable ->
                isPolling = false
                handleThrowable(throwable)
                schedulePoll(2500)
            }
        )
    }

    private fun maybeShowSuccessDialog(session: NfcSession) {
        if (successDialogShown) return
        successDialogShown = true

        val payerName = session.payer?.nome?.ifBlank { session.payer?.email.orEmpty() } ?: "outra conta"
        AlertDialog.Builder(this)
            .setTitle("Pagamento recebido")
            .setMessage(
                "${formatCurrency(session.amount)} entrou na sua conta pela aproximacao NFC.\n\nPagador: $payerName"
            )
            .setPositiveButton("Entendi", null)
            .show()
    }

    private fun renderSession(session: NfcSession) {
        binding.sessionStatusChip.text = humanizeSessionStatus(session.status)
        binding.sessionStatusChip.backgroundTintList = ColorStateList.valueOf(
            ContextCompat.getColor(
                this,
                when (session.status) {
                    "consumed" -> R.color.sigmo_primary_dark
                    "expired" -> R.color.sigmo_warning
                    "cancelled" -> R.color.sigmo_surface_alt
                    else -> R.color.sigmo_primary
                }
            )
        )

        val charge = session.charge ?: currentCharge
        val amountToShow = session.fixedAmount.takeIf { it > 0 } ?: charge?.amount ?: session.amount
        val codeToShow = charge?.publicCode?.ifBlank { null } ?: session.id.takeLast(10)

        binding.sessionStateValueText.text = humanizeSessionStatus(session.status)
        binding.expiresValueText.text = formatDateTime(session.expiresAt)
        binding.readCountValueText.text = session.readCount.toString()
        binding.sessionCodeValueText.text = codeToShow
        binding.supportText.text = when (session.status) {
            "pending" -> {
                if (charge != null) {
                    "Cobranca pronta no valor de ${formatCurrency(amountToShow)}. Agora o pagador so precisa encostar e autorizar no aparelho."
                } else {
                    "Aproxime o celular pagador por 1 a 2 segundos para concluir a leitura."
                }
            }
            "consumed" -> "Pagamento liquidado com sucesso no backend da Sigmo."
            "expired" -> "A sessao expirou. Gere uma nova para voltar a receber por aproximacao."
            else -> "A sessao foi encerrada. Gere uma nova quando quiser receber novamente."
        }

        val payerLabel = session.payer?.let { it.nome.ifBlank { it.email } } ?: "-"
        binding.payerValueText.text = payerLabel
        binding.amountValueText.text = if (amountToShow > 0) formatCurrency(amountToShow) else "-"
    }

    private fun renderUnavailableState(message: String) {
        binding.sessionStatusChip.text = "Indisponivel"
        binding.sessionStatusChip.backgroundTintList = ColorStateList.valueOf(
            ContextCompat.getColor(this, R.color.sigmo_danger)
        )
        binding.sessionStateValueText.text = "Indisponivel"
        binding.supportText.text = message
        binding.renewButton.isEnabled = false
        binding.closeButton.text = "Voltar"
        binding.closeButton.setOnClickListener { finish() }
    }

    private fun schedulePoll(delayMs: Long) {
        handler.removeCallbacks(pollRunnable)
        if (pollEnabled && currentSession?.status == "pending") {
            handler.postDelayed(pollRunnable, delayMs)
        }
    }

    private fun canReceiveByNfc(): Boolean {
        val adapter = NfcAdapter.getDefaultAdapter(this) ?: return false
        val hasHce = packageManager.hasSystemFeature("android.hardware.nfc.hce")
        return adapter.isEnabled && hasHce
    }

    companion object {
        const val EXTRA_CHARGE_ID = "charge_id"
    }
}
