package com.sigmo.nfc

import android.content.Context
import java.util.UUID

class DeviceIdentityStore(context: Context) {
    private val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun getDeviceId(): String {
        val existing = prefs.getString(KEY_DEVICE_ID, "").orEmpty().trim()
        if (existing.isNotBlank()) return existing

        val generated = "sigmo-" + UUID.randomUUID().toString()
        prefs.edit().putString(KEY_DEVICE_ID, generated).apply()
        return generated
    }

    companion object {
        private const val PREFS_NAME = "sigmo_mobile_device"
        private const val KEY_DEVICE_ID = "device_id"
    }
}
