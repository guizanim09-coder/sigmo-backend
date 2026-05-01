package com.sigmo.nfc

import android.content.res.ColorStateList
import android.nfc.NfcAdapter
import android.nfc.Tag
import android.nfc.tech.IsoDep
import android.os.Build
import android.os.Bundle
import androidx.appcompat.app.AlertDialog
import androidx.biometric.BiometricManager
import androidx.biometric.BiometricPrompt
import androidx.core.content.ContextCompat
import com.sigmo.nfc.databinding.ActivityPayNfcBinding
import java.util.concurrent.atomic.AtomicBoolean

class PayNfcActivity : BaseActivity(), NfcAdapter.ReaderCallback {
    private lateinit var binding: ActivityPayNfcBinding
    private var nfcAdapter: NfcAdapter? = null
    private val tagReadInProgress = AtomicBoolean(false)

    private var resolvedSession: NfcSession? = null
    private var resolvedPayload: String = ""
    private var paymentInProgress = false
    private var readerEnabled = false
    private lateinit var biometricPrompt: BiometricPrompt

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        if (!ensureLoggedIn()) return

        binding = ActivityPayNfcBinding.inflate(layoutInflater)
        setContentView(binding.root)

        nfcAdapter = NfcAdapter.getDefaultAdapter(this)
        biometricPrompt = createBiometricPrompt()

        binding.backButton.setOnClickListener { finish() }
        binding.scanAgainButton.setOnClickListener { resetResolvedSession(resumeReader = true) }
        binding.payButton.setOnClickListener { requestPaymentAuthorization() }

        renderUser(sessionStore.getUser())

        val user = sessionStore.getUser()
        val securityState = DeviceSecuritySupport.getState(this)

        when {
            user?.contaBanida == true -> {
                renderUnavailableState("Conta banida. O pagamento por aproximacao ficou bloqueado.")
                return
            }
            user?.activeCard == null -> {
                renderUnavailableState(
                    "Nenhum cartao foi liberado neste aparelho. Libere um cartao na area Cartoes do app web."
                )
                binding.scanAgainButton.text = "Voltar"
                binding.scanAgainButton.isEnabled = true
                binding.scanAgainButton.setOnClickListener { finish() }
                return
            }
            !securityState.available -> {
                renderUnavailableState(DeviceSecuritySupport.helpMessage())
                binding.scanAgainButton.text = "Ver requisitos"
                binding.scanAgainButton.isEnabled = true
                binding.scanAgainButton.setOnClickListener { showDeviceSecurityHelp() }
                return
            }
            nfcAdapter == null || nfcAdapter?.isEnabled != true -> {
                renderUnavailableState("Ative o NFC deste aparelho para pagar por aproximacao.")
                return
            }
        }

        resetResolvedSession(resumeReader = false)
    }

    override fun onResume() {
        super.onResume()
        renderUser(sessionStore.getUser())
        if (resolvedSession == null && !paymentInProgress) {
            enableReaderMode()
        }
    }

    override fun onPause() {
        super.onPause()
        disableReaderMode()
    }

    override fun onTagDiscovered(tag: Tag?) {
        if (tag == null || !tagReadInProgress.compareAndSet(false, true)) return

        var isoDep: IsoDep? = null
        try {
            isoDep = IsoDep.get(tag) ?: throw IllegalStateException("Celular recebedor nao respondeu por NFC")
            isoDep.connect()
            isoDep.timeout = 2000
            val response = isoDep.transceive(NfcProtocol.buildSelectAidCommand())
            val payload = NfcProtocol.parsePayloadFromResponse(response)

            runOnUiThread {
                disableReaderMode()
                resolvePayload(payload)
            }
        } catch (throwable: Throwable) {
            runOnUiThread {
                binding.scanStatusText.text =
                    throwable.message ?: "Nao foi possivel ler a sessao de aproximacao"
                binding.progressBar.hide()
                enableReaderMode()
            }
        } finally {
            runCatching { isoDep?.close() }
            tagReadInProgress.set(false)
        }
    }

    private fun resolvePayload(payload: String) {
        binding.progressBar.show()
        binding.scanStatusText.text = "Sessao encontrada. Validando com a Sigmo..."
        binding.payButton.isEnabled = false

        runAsync(
            task = { apiClient.resolveSession(payload) },
            onSuccess = { session ->
                binding.progressBar.hide()
                resolvedPayload = payload
                resolvedSession = session
                val receiverLabel = session.receiver?.let { it.nome.ifBlank { it.email } } ?: "Recebedor"
                val amount = session.fixedAmount.takeIf { it > 0 } ?: session.charge?.amount ?: 0.0

                binding.scanStatusText.text = if (amount > 0) {
                    "Cobranca confirmada. Agora autorize com a protecao do aparelho."
                } else {
                    "Sessao sem cobranca definida para pagamento no app."
                }
                binding.receiverNameText.text = receiverLabel
                binding.receiverEmailText.text = session.receiver?.email ?: "-"
                binding.chargeAmountText.text = if (amount > 0) formatCurrency(amount) else "-"
                binding.sessionStatusChip.text = "Pronto para autorizar"
                binding.sessionStatusChip.backgroundTintList = ColorStateList.valueOf(
                    ContextCompat.getColor(this, R.color.sigmo_primary_dark)
                )
                binding.payButton.isEnabled = amount > 0
            },
            onError = { throwable ->
                binding.progressBar.hide()
                handleThrowable(throwable)
                resetResolvedSession(resumeReader = true)
            }
        )
    }

    private fun requestPaymentAuthorization() {
        val session = resolvedSession ?: return
        val amount = session.fixedAmount.takeIf { it > 0 } ?: session.charge?.amount ?: 0.0
        val card = sessionStore.getUser()?.activeCard

        if (amount <= 0) {
            binding.scanStatusText.text = "Esta aproximacao nao trouxe uma cobranca valida."
            return
        }

        if (card == null) {
            renderUnavailableState("Nenhum cartao foi liberado neste aparelho.")
            return
        }

        if (amount > card.availableToSpend) {
            binding.scanStatusText.text =
                "A cobranca excede o limite disponivel deste cartao: ${formatCurrency(card.availableToSpend)}."
            return
        }

        paymentInProgress = true
        binding.payButton.isEnabled = false
        binding.payButton.text = "Aguardando autorizacao..."
        binding.scanAgainButton.isEnabled = false
        binding.progressBar.show()

        biometricPrompt.authenticate(buildPromptInfo(amount))
    }

    private fun sendPayment() {
        runAsync(
            task = { apiClient.payNfc(resolvedPayload, authMethod = "device_auth") },
            onSuccess = { response ->
                paymentInProgress = false
                binding.progressBar.hide()
                binding.payButton.text = "Pagamento confirmado"
                binding.scanAgainButton.isEnabled = true

                response.user?.let { sessionStore.updateUser(it) }
                renderUser(sessionStore.getUser())

                val amount = resolvedSession?.fixedAmount?.takeIf { it > 0 }
                    ?: resolvedSession?.charge?.amount
                    ?: 0.0
                val receiverLabel = resolvedSession?.receiver?.let { it.nome.ifBlank { it.email } } ?: "outra conta"

                AlertDialog.Builder(this)
                    .setTitle("Pagamento enviado")
                    .setMessage("${formatCurrency(amount)} foi transferido para $receiverLabel.")
                    .setPositiveButton("Voltar para a carteira") { _, _ ->
                        goToHome()
                    }
                    .setCancelable(false)
                    .show()
            },
            onError = { throwable ->
                paymentInProgress = false
                binding.progressBar.hide()
                binding.payButton.isEnabled = true
                binding.payButton.text = "Autorizar pagamento"
                binding.scanAgainButton.isEnabled = true
                handleThrowable(throwable)
                if (throwable is ApiException) {
                    if (
                        throwable.apiError.code.startsWith("NFC_") ||
                        throwable.apiError.code.startsWith("TAP_CHARGE_") ||
                        throwable.apiError.code.startsWith("CARD_") ||
                        throwable.apiError.code == "SELF_TRANSFER_NOT_ALLOWED"
                    ) {
                        resetResolvedSession(resumeReader = true)
                    }
                }
            }
        )
    }

    private fun renderUser(user: User?) {
        val card = user?.activeCard
        val securityState = DeviceSecuritySupport.getState(this)

        binding.balanceValueText.text = formatCurrency(card?.availableToSpend ?: 0.0)
        binding.accountChip.text = when {
            user?.contaBanida == true -> "Conta banida"
            card?.cardType == "additional" -> "Cartao adicional"
            else -> "Cartao ativo"
        }
        binding.accountChip.backgroundTintList = ColorStateList.valueOf(
            ContextCompat.getColor(
                this,
                if (user?.contaBanida == true) R.color.sigmo_danger else R.color.sigmo_primary_dark
            )
        )
        binding.cardLimitText.text = formatCurrency(card?.spendingLimit ?: 0.0)
        binding.cardOwnerText.text =
            card?.owner?.nome?.ifBlank { card.owner?.email.orEmpty() } ?: "Sem titular liberado"
        binding.cardSecurityText.text = securityState.statusLabel
    }

    private fun resetResolvedSession(resumeReader: Boolean) {
        resolvedSession = null
        resolvedPayload = ""
        binding.scanStatusText.text = "Aproxime do celular recebedor para iniciar o pagamento."
        binding.receiverNameText.text = "Aguardando leitura"
        binding.receiverEmailText.text = "Encoste os aparelhos para capturar a sessao."
        binding.chargeAmountText.text = "-"
        binding.sessionStatusChip.text = "Buscando sessao"
        binding.sessionStatusChip.backgroundTintList = ColorStateList.valueOf(
            ContextCompat.getColor(this, R.color.sigmo_surface_alt)
        )
        binding.payButton.isEnabled = false
        binding.payButton.text = "Autorizar pagamento"
        if (resumeReader) {
            enableReaderMode()
        }
    }

    private fun renderUnavailableState(message: String) {
        binding.scanStatusText.text = message
        binding.sessionStatusChip.text = "Indisponivel"
        binding.sessionStatusChip.backgroundTintList = ColorStateList.valueOf(
            ContextCompat.getColor(this, R.color.sigmo_danger)
        )
        binding.payButton.isEnabled = false
        binding.scanAgainButton.isEnabled = false
    }

    private fun buildPromptInfo(amount: Double): BiometricPrompt.PromptInfo {
        val builder = BiometricPrompt.PromptInfo.Builder()
            .setTitle("Autorize o pagamento Sigmo")
            .setSubtitle("Confirme ${formatCurrency(amount)} com a protecao deste aparelho")
            .setConfirmationRequired(false)

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            builder.setAllowedAuthenticators(
                BiometricManager.Authenticators.BIOMETRIC_STRONG or
                    BiometricManager.Authenticators.DEVICE_CREDENTIAL
            )
        } else {
            builder.setDeviceCredentialAllowed(true)
        }

        return builder.build()
    }

    private fun createBiometricPrompt(): BiometricPrompt {
        return BiometricPrompt(
            this,
            ContextCompat.getMainExecutor(this),
            object : BiometricPrompt.AuthenticationCallback() {
                override fun onAuthenticationSucceeded(result: BiometricPrompt.AuthenticationResult) {
                    sendPayment()
                }

                override fun onAuthenticationFailed() {
                    binding.scanStatusText.text =
                        "Nao foi possivel validar sua identidade. Tente novamente."
                }

                override fun onAuthenticationError(errorCode: Int, errString: CharSequence) {
                    paymentInProgress = false
                    binding.progressBar.hide()
                    binding.payButton.isEnabled = resolvedSession != null
                    binding.payButton.text = "Autorizar pagamento"
                    binding.scanAgainButton.isEnabled = true

                    if (
                        errorCode == BiometricPrompt.ERROR_USER_CANCELED ||
                        errorCode == BiometricPrompt.ERROR_NEGATIVE_BUTTON ||
                        errorCode == BiometricPrompt.ERROR_CANCELED
                    ) {
                        binding.scanStatusText.text =
                            "Autorizacao cancelada. Encoste novamente quando quiser pagar."
                        return
                    }

                    binding.scanStatusText.text = errString.toString()
                }
            }
        )
    }

    private fun enableReaderMode() {
        if (readerEnabled) return
        val adapter = nfcAdapter ?: return
        adapter.enableReaderMode(
            this,
            this,
            NfcAdapter.FLAG_READER_NFC_A or
                NfcAdapter.FLAG_READER_SKIP_NDEF_CHECK or
                NfcAdapter.FLAG_READER_NO_PLATFORM_SOUNDS,
            null
        )
        readerEnabled = true
    }

    private fun disableReaderMode() {
        if (!readerEnabled) return
        nfcAdapter?.disableReaderMode(this)
        readerEnabled = false
    }
}
