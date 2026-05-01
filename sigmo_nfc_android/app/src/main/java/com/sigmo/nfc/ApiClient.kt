package com.sigmo.nfc

import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets

class ApiClient(private val sessionStore: SessionStore, private val deviceIdentityStore: DeviceIdentityStore) {
    fun login(email: String, senha: String): MobileAuthResponse {
        val response = request(
            path = "/mobile/login",
            method = "POST",
            body = JSONObject()
                .put("email", email.trim())
                .put("senha", senha),
            authenticated = false
        )
        return parseMobileAuthResponse(response)
    }

    fun getMe(): User {
        val response = request("/mobile/me")
        return parseUser(response.optJSONObject("user") ?: JSONObject())
    }

    fun claimCard(cardId: String, claimToken: String): User {
        val response = request(
            path = "/mobile/cards/claim",
            method = "POST",
            body = JSONObject()
                .put("cardId", cardId)
                .put("claimToken", claimToken)
        )
        return parseUser(response.optJSONObject("user") ?: JSONObject())
    }

    fun getTapCharge(chargeId: String): TapCharge {
        val response = request("/mobile/tap-charges/$chargeId")
        return parseTapCharge(response) ?: throw IllegalStateException("Cobranca invalida")
    }

    fun armTapCharge(chargeId: String, ttlSeconds: Int = 30): Pair<TapCharge, NfcSession> {
        val response = request(
            path = "/mobile/tap-charges/$chargeId/arm",
            method = "POST",
            body = JSONObject().put("ttlSeconds", ttlSeconds)
        )

        val charge = parseTapCharge(response.optJSONObject("charge"))
            ?: throw IllegalStateException("Cobranca invalida")
        val session = response.optJSONObject("session")?.let { parseNfcSession(it) }
            ?: throw IllegalStateException("Sessao invalida")
        return charge to session
    }

    fun createReceiveSession(ttlSeconds: Int = 30): NfcSession {
        val response = request(
            path = "/nfc/receive-session",
            method = "POST",
            body = JSONObject().put("ttlSeconds", ttlSeconds)
        )
        return parseNfcSession(response)
    }

    fun getReceiveSession(sessionId: String): NfcSession {
        val response = request("/nfc/receive-session/$sessionId")
        return parseNfcSession(response)
    }

    fun cancelReceiveSession(sessionId: String): NfcSession {
        val response = request(
            path = "/nfc/receive-session/$sessionId/cancel",
            method = "POST",
            body = JSONObject()
        )
        return parseNfcSession(response)
    }

    fun resolveSession(payload: String): NfcSession {
        val response = request(
            path = "/nfc/session/resolve",
            method = "POST",
            body = JSONObject().put("payload", payload)
        )
        return parseNfcSession(response)
    }

    fun payNfc(payload: String, authMethod: String = "device_auth"): NfcPaymentResponse {
        val response = request(
            path = "/nfc/pay",
            method = "POST",
            body = JSONObject()
                .put("payload", payload)
                .put("authMethod", authMethod)
        )
        return parseNfcPaymentResponse(response)
    }

    private fun request(
        path: String,
        method: String = "GET",
        body: JSONObject? = null,
        authenticated: Boolean = true
    ): JSONObject {
        val connection = (URL(buildUrl(path)).openConnection() as HttpURLConnection).apply {
            requestMethod = method
            connectTimeout = 15000
            readTimeout = 20000
            doInput = true
            setRequestProperty("Accept", "application/json")
            setRequestProperty("Content-Type", "application/json; charset=utf-8")
            setRequestProperty("X-Sigmo-Device-Id", deviceIdentityStore.getDeviceId())
        }

        if (authenticated) {
            val token = sessionStore.getToken()
            if (token.isBlank()) {
                throw ApiException(
                    ApiError(
                        statusCode = 401,
                        code = "AUTH_REQUIRED",
                        error = "Sua sessao expirou",
                        statusConta = "",
                        contaBanida = false,
                        contaBanidaEm = null,
                        motivoBanimento = "",
                        saldo = null,
                        valorRecebidoViaPix = null,
                        valorMinimoDesbloqueioPix = null,
                        cardLimit = null,
                        availableToSpend = null
                    )
                )
            }
            connection.setRequestProperty("Authorization", "Bearer $token")
        }

        if (body != null) {
            connection.doOutput = true
            connection.outputStream.use { stream ->
                stream.write(body.toString().toByteArray(StandardCharsets.UTF_8))
            }
        }

        return try {
            val statusCode = connection.responseCode
            val responseBody = readResponseBody(connection, statusCode in 200..299)
            val json = responseBody.toJsonObjectOrNull()
            if (statusCode !in 200..299) {
                throw ApiException(parseApiError(statusCode, json))
            }
            json ?: JSONObject()
        } finally {
            connection.disconnect()
        }
    }

    private fun readResponseBody(connection: HttpURLConnection, success: Boolean): String {
        val stream = if (success) connection.inputStream else connection.errorStream
        if (stream == null) return ""
        return BufferedReader(InputStreamReader(stream, StandardCharsets.UTF_8)).use { reader ->
            reader.readText().trim()
        }
    }

    private fun String.toJsonObjectOrNull(): JSONObject? {
        if (isBlank()) return null
        return runCatching { JSONObject(this) }.getOrNull()
    }

    private fun buildUrl(path: String): String {
        return BuildConfig.API_BASE_URL.trimEnd('/') + path
    }
}
