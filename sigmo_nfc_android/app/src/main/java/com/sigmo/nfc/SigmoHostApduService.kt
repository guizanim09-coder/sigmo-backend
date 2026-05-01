package com.sigmo.nfc

import android.nfc.cardemulation.HostApduService
import android.os.Bundle

class SigmoHostApduService : HostApduService() {
    override fun processCommandApdu(commandApdu: ByteArray?, extras: Bundle?): ByteArray {
        if (commandApdu == null) {
            return NfcProtocol.unknownCommandResponse()
        }

        if (!NfcProtocol.isSelectAidApdu(commandApdu)) {
            return NfcProtocol.unknownCommandResponse()
        }

        val payload = NfcTapStateStore.getActivePayload(applicationContext)
        if (payload.isBlank()) {
            return NfcProtocol.fileNotFoundResponse()
        }

        return NfcProtocol.buildSuccessResponse(payload)
    }

    override fun onDeactivated(reason: Int) = Unit
}
