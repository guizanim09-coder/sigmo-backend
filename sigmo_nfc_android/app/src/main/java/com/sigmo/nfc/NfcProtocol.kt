package com.sigmo.nfc

import java.nio.charset.StandardCharsets

object NfcProtocol {
    private const val AID = "F0395349474D4F01"

    private val STATUS_OK = byteArrayOf(0x90.toByte(), 0x00.toByte())
    private val STATUS_FILE_NOT_FOUND = byteArrayOf(0x6A.toByte(), 0x82.toByte())
    private val STATUS_UNKNOWN_COMMAND = byteArrayOf(0x6D.toByte(), 0x00.toByte())

    private val selectAidCommand: ByteArray by lazy {
        val aidBytes = hexToBytes(AID)
        val command = ByteArray(6 + aidBytes.size)
        command[0] = 0x00
        command[1] = 0xA4.toByte()
        command[2] = 0x04
        command[3] = 0x00
        command[4] = aidBytes.size.toByte()
        aidBytes.copyInto(command, 5)
        command[command.lastIndex] = 0x00
        command
    }

    fun buildSelectAidCommand(): ByteArray = selectAidCommand.copyOf()

    fun isSelectAidApdu(commandApdu: ByteArray): Boolean {
        return commandApdu.contentEquals(selectAidCommand)
    }

    fun buildSuccessResponse(payload: String): ByteArray {
        val body = payload.toByteArray(StandardCharsets.UTF_8)
        return body + STATUS_OK
    }

    fun fileNotFoundResponse(): ByteArray = STATUS_FILE_NOT_FOUND.copyOf()

    fun unknownCommandResponse(): ByteArray = STATUS_UNKNOWN_COMMAND.copyOf()

    fun parsePayloadFromResponse(response: ByteArray): String {
        if (response.size < 2) {
            throw IllegalStateException("Resposta NFC invalida")
        }

        val statusWord = response.copyOfRange(response.size - 2, response.size)
        val payload = response.copyOfRange(0, response.size - 2)

        return when {
            statusWord.contentEquals(STATUS_OK) -> payload.toString(StandardCharsets.UTF_8)
            statusWord.contentEquals(STATUS_FILE_NOT_FOUND) -> {
                throw IllegalStateException("Nenhuma sessao de recebimento ativa")
            }
            else -> throw IllegalStateException("Aproximacao NFC nao reconhecida")
        }
    }

    private fun hexToBytes(value: String): ByteArray {
        val clean = value.trim().replace(" ", "")
        require(clean.length % 2 == 0) { "Hex invalido" }
        return ByteArray(clean.length / 2) { index ->
            clean.substring(index * 2, index * 2 + 2).toInt(16).toByte()
        }
    }
}
