package com.sigmo.nfc

import android.content.Context

class SessionStore(context: Context) {
    private val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun hasSession(): Boolean = getToken().isNotBlank()

    fun getToken(): String = prefs.getString(KEY_TOKEN, "").orEmpty()

    fun getUser(): User? {
        val raw = prefs.getString(KEY_USER_JSON, "").orEmpty()
        if (raw.isBlank()) return null
        return runCatching { parseUser(org.json.JSONObject(raw)) }.getOrNull()
    }

    fun saveAuth(auth: MobileAuthResponse) {
        saveAuth(auth.token, auth.user)
    }

    fun saveAuth(token: String, user: User) {
        prefs.edit()
            .putString(KEY_TOKEN, token)
            .putString(KEY_USER_JSON, userToJson(user).toString())
            .apply()
    }

    fun updateUser(user: User) {
        prefs.edit()
            .putString(KEY_USER_JSON, userToJson(user).toString())
            .apply()
    }

    fun mergeBanState(apiError: ApiError): User? {
        val current = getUser() ?: return null
        val updated = current.copy(
            saldo = apiError.saldo ?: current.saldo,
            statusConta = apiError.statusConta.ifBlank { "banida" },
            contaBanida = true,
            contaBanidaEm = apiError.contaBanidaEm ?: current.contaBanidaEm,
            motivoBanimento = apiError.motivoBanimento.ifBlank { current.motivoBanimento }
        )
        updateUser(updated)
        return updated
    }

    fun clear() {
        prefs.edit().clear().apply()
    }

    companion object {
        private const val PREFS_NAME = "sigmo_mobile_session"
        private const val KEY_TOKEN = "token"
        private const val KEY_USER_JSON = "user_json"
    }
}
