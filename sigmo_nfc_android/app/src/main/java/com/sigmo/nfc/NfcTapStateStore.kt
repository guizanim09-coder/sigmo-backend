package com.sigmo.nfc

import android.content.Context

object NfcTapStateStore {
    private const val PREFS_NAME = "sigmo_nfc_tap_state"
    private const val KEY_SESSION_ID = "session_id"
    private const val KEY_PAYLOAD = "payload"
    private const val KEY_EXPIRES_AT = "expires_at"

    private fun prefs(context: Context) =
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun saveActiveSession(context: Context, session: NfcSession) {
        prefs(context).edit()
            .putString(KEY_SESSION_ID, session.id)
            .putString(KEY_PAYLOAD, session.payload)
            .putString(KEY_EXPIRES_AT, session.expiresAt.orEmpty())
            .apply()
    }

    fun getActiveSessionId(context: Context): String {
        return prefs(context).getString(KEY_SESSION_ID, "").orEmpty()
    }

    fun getActivePayload(context: Context): String {
        return prefs(context).getString(KEY_PAYLOAD, "").orEmpty()
    }

    fun getExpiresAt(context: Context): String? {
        return prefs(context).getString(KEY_EXPIRES_AT, "").orEmpty().ifBlank { null }
    }

    fun clear(context: Context) {
        prefs(context).edit().clear().apply()
    }
}
