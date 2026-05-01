package com.sigmo.nfc

import android.view.View
import java.text.NumberFormat
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale

private val ptBrLocale = Locale("pt", "BR")
private val moneyFormatter: NumberFormat = NumberFormat.getCurrencyInstance(ptBrLocale)
private val outputDateFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm", ptBrLocale)
private val postgresDateFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)
private val postgresDateFormatterWithMillis: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS", Locale.US)

fun formatCurrency(value: Double): String = moneyFormatter.format(value)

fun firstName(name: String): String {
    return name.trim().split(Regex("\\s+")).firstOrNull().orEmpty().ifBlank { "voce" }
}

fun formatDateTime(value: String?): String {
    if (value.isNullOrBlank()) return "-"

    val parsed = runCatching { OffsetDateTime.parse(value) }.getOrNull()
        ?: runCatching { Instant.parse(value).atZone(ZoneId.systemDefault()).toOffsetDateTime() }.getOrNull()
        ?: runCatching {
            LocalDateTime.parse(value, postgresDateFormatter).atZone(ZoneId.systemDefault()).toOffsetDateTime()
        }.getOrNull()
        ?: runCatching {
            LocalDateTime.parse(value, postgresDateFormatterWithMillis)
                .atZone(ZoneId.systemDefault())
                .toOffsetDateTime()
        }.getOrNull()
        ?: runCatching { LocalDateTime.parse(value).atZone(ZoneId.systemDefault()).toOffsetDateTime() }.getOrNull()

    return parsed?.format(outputDateFormatter) ?: value
}

fun parseMoneyInput(value: String): Double? {
    val clean = value
        .replace("R$", "", ignoreCase = true)
        .replace(" ", "")
        .trim()
    if (clean.isBlank()) return null

    val normalized = when {
        clean.contains(",") && clean.contains(".") -> clean.replace(".", "").replace(",", ".")
        clean.contains(",") -> clean.replace(",", ".")
        else -> clean
    }

    return normalized.toDoubleOrNull()
}

fun humanizeSessionStatus(status: String): String {
    return when (status.lowercase(Locale.ROOT)) {
        "pending" -> "Pronta para receber"
        "consumed" -> "Pagamento confirmado"
        "cancelled" -> "Sessao encerrada"
        "expired" -> "Sessao expirada"
        else -> status.ifBlank { "-" }
    }
}

fun View.show() {
    visibility = View.VISIBLE
}

fun View.hide() {
    visibility = View.GONE
}
