package com.sigmo.nfc

import org.json.JSONObject

data class SessionParticipant(
    val id: String,
    val nome: String,
    val email: String
)

data class ActiveCard(
    val id: String,
    val ownerUserId: String,
    val holderUserId: String,
    val cardType: String,
    val label: String,
    val status: String,
    val spendingLimit: Double,
    val availableToSpend: Double,
    val deviceBound: Boolean,
    val boundAt: String?,
    val lastUsedAt: String?,
    val appLink: String,
    val owner: SessionParticipant?,
    val holder: SessionParticipant?
)

data class User(
    val id: String,
    val nome: String,
    val email: String,
    val saldo: Double,
    val criadoEm: String?,
    val statusConta: String,
    val contaBanida: Boolean,
    val contaBanidaEm: String?,
    val motivoBanimento: String,
    val bonusBoasVindas: Double,
    val bonusBoasVindasConcedidoEm: String?,
    val pixDesbloqueado: Boolean,
    val valorRecebidoViaPix: Double,
    val valorMinimoDesbloqueioPix: Double,
    val activeCard: ActiveCard?
)

data class MobileAuthResponse(
    val token: String,
    val tokenType: String,
    val tokenTtl: Long,
    val user: User,
    val serverTime: String?
)

data class TapCharge(
    val id: String,
    val publicCode: String,
    val status: String,
    val amount: Double,
    val description: String,
    val expiresAt: String?,
    val paidAt: String?,
    val cancelledAt: String?,
    val appLink: String,
    val receiver: SessionParticipant?
)

data class NfcSession(
    val id: String,
    val status: String,
    val protocolVersion: Int,
    val publicToken: String,
    val nonce: String,
    val payload: String,
    val expiresAt: String?,
    val consumedAt: String?,
    val cancelledAt: String?,
    val amount: Double,
    val readCount: Int,
    val lastReadAt: String?,
    val receiver: SessionParticipant?,
    val payer: SessionParticipant?,
    val ttlSeconds: Int,
    val canPay: Boolean,
    val financialTransactionId: String,
    val fixedAmount: Double,
    val requiresDeviceAuth: Boolean,
    val confirmationMode: String,
    val charge: TapCharge?
)

data class NfcPaymentResponse(
    val code: String,
    val message: String,
    val saldoAtual: Double,
    val receiver: SessionParticipant?,
    val session: NfcSession?,
    val charge: TapCharge?,
    val card: ActiveCard?,
    val user: User?
)

data class ApiError(
    val statusCode: Int,
    val code: String,
    val error: String,
    val statusConta: String,
    val contaBanida: Boolean,
    val contaBanidaEm: String?,
    val motivoBanimento: String,
    val saldo: Double?,
    val valorRecebidoViaPix: Double?,
    val valorMinimoDesbloqueioPix: Double?,
    val cardLimit: Double?,
    val availableToSpend: Double?
) {
    val isAccountBanned: Boolean
        get() = contaBanida || code == "ACCOUNT_BANNED" || code == "ACCOUNT_BANNED_FRAUD"
}

class ApiException(val apiError: ApiError) : Exception(apiError.error)

private fun JSONObject.optStringSafe(key: String): String {
    return if (has(key) && !isNull(key)) optString(key, "") else ""
}

private fun JSONObject.optNullableString(key: String): String? {
    val value = optStringSafe(key)
    return value.ifBlank { null }
}

private fun JSONObject.optDoubleSafe(key: String): Double {
    val raw = opt(key)
    return when (raw) {
        null -> 0.0
        is Number -> raw.toDouble()
        else -> raw.toString().replace(",", ".").toDoubleOrNull() ?: 0.0
    }
}

private fun JSONObject.optIntSafe(key: String): Int {
    val raw = opt(key)
    return when (raw) {
        null -> 0
        is Number -> raw.toInt()
        else -> raw.toString().toIntOrNull() ?: 0
    }
}

private fun JSONObject.optLongSafe(key: String): Long {
    val raw = opt(key)
    return when (raw) {
        null -> 0L
        is Number -> raw.toLong()
        else -> raw.toString().toLongOrNull() ?: 0L
    }
}

private fun JSONObject.optBooleanSafe(key: String): Boolean {
    val raw = opt(key)
    return when (raw) {
        null -> false
        is Boolean -> raw
        is Number -> raw.toInt() != 0
        else -> raw.toString().equals("true", ignoreCase = true)
    }
}

private fun JSONObject.optObjectSafe(key: String): JSONObject? {
    return if (has(key) && !isNull(key)) optJSONObject(key) else null
}

fun parseParticipant(json: JSONObject?): SessionParticipant? {
    if (json == null) return null

    return SessionParticipant(
        id = json.optStringSafe("id"),
        nome = json.optStringSafe("nome"),
        email = json.optStringSafe("email")
    )
}

fun parseActiveCard(json: JSONObject?): ActiveCard? {
    if (json == null) return null

    return ActiveCard(
        id = json.optStringSafe("id"),
        ownerUserId = json.optStringSafe("ownerUserId"),
        holderUserId = json.optStringSafe("holderUserId"),
        cardType = json.optStringSafe("cardType").ifBlank { "primary" },
        label = json.optStringSafe("label"),
        status = json.optStringSafe("status").ifBlank { "active" },
        spendingLimit = json.optDoubleSafe("spendingLimit"),
        availableToSpend = json.optDoubleSafe("availableToSpend"),
        deviceBound = json.optBooleanSafe("deviceBound"),
        boundAt = json.optNullableString("boundAt"),
        lastUsedAt = json.optNullableString("lastUsedAt"),
        appLink = json.optStringSafe("appLink"),
        owner = parseParticipant(json.optObjectSafe("owner")),
        holder = parseParticipant(json.optObjectSafe("holder"))
    )
}

fun activeCardToJson(card: ActiveCard?): JSONObject? {
    if (card == null) return null

    return JSONObject()
        .put("id", card.id)
        .put("ownerUserId", card.ownerUserId)
        .put("holderUserId", card.holderUserId)
        .put("cardType", card.cardType)
        .put("label", card.label)
        .put("status", card.status)
        .put("spendingLimit", card.spendingLimit)
        .put("availableToSpend", card.availableToSpend)
        .put("deviceBound", card.deviceBound)
        .put("boundAt", card.boundAt)
        .put("lastUsedAt", card.lastUsedAt)
        .put("appLink", card.appLink)
        .put("owner", participantToJson(card.owner))
        .put("holder", participantToJson(card.holder))
}

fun participantToJson(participant: SessionParticipant?): JSONObject? {
    if (participant == null) return null

    return JSONObject()
        .put("id", participant.id)
        .put("nome", participant.nome)
        .put("email", participant.email)
}

fun parseUser(json: JSONObject): User {
    return User(
        id = json.optStringSafe("id"),
        nome = json.optStringSafe("nome"),
        email = json.optStringSafe("email"),
        saldo = json.optDoubleSafe("saldo"),
        criadoEm = json.optNullableString("criadoEm"),
        statusConta = json.optStringSafe("statusConta").ifBlank { "ativa" },
        contaBanida = json.optBooleanSafe("contaBanida"),
        contaBanidaEm = json.optNullableString("contaBanidaEm"),
        motivoBanimento = json.optStringSafe("motivoBanimento"),
        bonusBoasVindas = json.optDoubleSafe("bonusBoasVindas"),
        bonusBoasVindasConcedidoEm = json.optNullableString("bonusBoasVindasConcedidoEm"),
        pixDesbloqueado = json.optBooleanSafe("pixDesbloqueado"),
        valorRecebidoViaPix = json.optDoubleSafe("valorRecebidoViaPix"),
        valorMinimoDesbloqueioPix = json.optDoubleSafe("valorMinimoDesbloqueioPix"),
        activeCard = parseActiveCard(json.optObjectSafe("activeCard"))
    )
}

fun userToJson(user: User): JSONObject {
    return JSONObject()
        .put("id", user.id)
        .put("nome", user.nome)
        .put("email", user.email)
        .put("saldo", user.saldo)
        .put("criadoEm", user.criadoEm)
        .put("statusConta", user.statusConta)
        .put("contaBanida", user.contaBanida)
        .put("contaBanidaEm", user.contaBanidaEm)
        .put("motivoBanimento", user.motivoBanimento)
        .put("bonusBoasVindas", user.bonusBoasVindas)
        .put("bonusBoasVindasConcedidoEm", user.bonusBoasVindasConcedidoEm)
        .put("pixDesbloqueado", user.pixDesbloqueado)
        .put("valorRecebidoViaPix", user.valorRecebidoViaPix)
        .put("valorMinimoDesbloqueioPix", user.valorMinimoDesbloqueioPix)
        .put("activeCard", activeCardToJson(user.activeCard))
}

fun parseMobileAuthResponse(json: JSONObject): MobileAuthResponse {
    return MobileAuthResponse(
        token = json.optStringSafe("token"),
        tokenType = json.optStringSafe("tokenType"),
        tokenTtl = json.optLongSafe("tokenTtl"),
        user = parseUser(json.optJSONObject("user") ?: JSONObject()),
        serverTime = json.optNullableString("serverTime")
    )
}

fun parseTapCharge(json: JSONObject?): TapCharge? {
    if (json == null) return null

    return TapCharge(
        id = json.optStringSafe("id"),
        publicCode = json.optStringSafe("publicCode"),
        status = json.optStringSafe("status"),
        amount = json.optDoubleSafe("amount"),
        description = json.optStringSafe("description"),
        expiresAt = json.optNullableString("expiresAt"),
        paidAt = json.optNullableString("paidAt"),
        cancelledAt = json.optNullableString("cancelledAt"),
        appLink = json.optStringSafe("appLink"),
        receiver = parseParticipant(json.optObjectSafe("receiver"))
    )
}

fun parseNfcSession(json: JSONObject): NfcSession {
    return NfcSession(
        id = json.optStringSafe("id"),
        status = json.optStringSafe("status"),
        protocolVersion = json.optIntSafe("protocolVersion"),
        publicToken = json.optStringSafe("publicToken"),
        nonce = json.optStringSafe("nonce"),
        payload = json.optStringSafe("payload"),
        expiresAt = json.optNullableString("expiresAt"),
        consumedAt = json.optNullableString("consumedAt"),
        cancelledAt = json.optNullableString("cancelledAt"),
        amount = json.optDoubleSafe("amount"),
        readCount = json.optIntSafe("readCount"),
        lastReadAt = json.optNullableString("lastReadAt"),
        receiver = parseParticipant(json.optObjectSafe("receiver")),
        payer = parseParticipant(json.optObjectSafe("payer")),
        ttlSeconds = json.optIntSafe("ttlSeconds"),
        canPay = json.optBooleanSafe("canPay"),
        financialTransactionId = json.optStringSafe("financialTransactionId"),
        fixedAmount = json.optDoubleSafe("fixedAmount"),
        requiresDeviceAuth = json.optBooleanSafe("requiresDeviceAuth"),
        confirmationMode = json.optStringSafe("confirmationMode"),
        charge = parseTapCharge(json.optObjectSafe("charge"))
    )
}

fun parseNfcPaymentResponse(json: JSONObject): NfcPaymentResponse {
    return NfcPaymentResponse(
        code = json.optStringSafe("code"),
        message = json.optStringSafe("message"),
        saldoAtual = json.optDoubleSafe("saldoAtual"),
        receiver = parseParticipant(json.optObjectSafe("receiver")),
        session = json.optObjectSafe("session")?.let { parseNfcSession(it) },
        charge = parseTapCharge(json.optObjectSafe("charge")),
        card = parseActiveCard(json.optObjectSafe("card")),
        user = json.optObjectSafe("user")?.let { parseUser(it) }
    )
}

fun parseApiError(statusCode: Int, json: JSONObject?): ApiError {
    val safeJson = json ?: JSONObject()
    return ApiError(
        statusCode = statusCode,
        code = safeJson.optStringSafe("code").ifBlank { "HTTP_$statusCode" },
        error = safeJson.optStringSafe("error").ifBlank {
            safeJson.optStringSafe("message").ifBlank { "Erro na comunicacao com a Sigmo" }
        },
        statusConta = safeJson.optStringSafe("statusConta"),
        contaBanida = safeJson.optBooleanSafe("contaBanida"),
        contaBanidaEm = safeJson.optNullableString("contaBanidaEm"),
        motivoBanimento = safeJson.optStringSafe("motivoBanimento"),
        saldo = if (safeJson.has("saldo") && !safeJson.isNull("saldo")) safeJson.optDoubleSafe("saldo") else null,
        valorRecebidoViaPix = if (safeJson.has("valorRecebidoViaPix") && !safeJson.isNull("valorRecebidoViaPix")) {
            safeJson.optDoubleSafe("valorRecebidoViaPix")
        } else {
            null
        },
        valorMinimoDesbloqueioPix = if (
            safeJson.has("valorMinimoDesbloqueioPix") &&
            !safeJson.isNull("valorMinimoDesbloqueioPix")
        ) {
            safeJson.optDoubleSafe("valorMinimoDesbloqueioPix")
        } else {
            null
        },
        cardLimit = if (safeJson.has("cardLimit") && !safeJson.isNull("cardLimit")) {
            safeJson.optDoubleSafe("cardLimit")
        } else {
            null
        },
        availableToSpend = if (safeJson.has("availableToSpend") && !safeJson.isNull("availableToSpend")) {
            safeJson.optDoubleSafe("availableToSpend")
        } else {
            null
        }
    )
}
