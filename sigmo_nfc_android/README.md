# Sigmo NFC Android

Aplicativo Android nativo para pagamentos Sigmo para Sigmo por aproximacao NFC.

## O que este MVP faz

- login seguro via `POST /mobile/login`
- refresh de sessao via `GET /mobile/me`
- modo `Receber por aproximacao` com HCE
- modo `Pagar por aproximacao` com Reader Mode + IsoDep
- criacao de sessao curta no backend
- resolucao do recebedor ao encostar
- confirmacao manual do valor antes de pagar
- liquidacao online usando a mesma logica de transferencia Sigmo para Sigmo

## Requisitos

- Android Studio recente
- dispositivo Android com NFC
- para receber por aproximacao: suporte a HCE
- backend com as rotas NFC e mobile deste repositorio

## Antes de rodar

1. Garanta que o backend com as rotas novas esteja publicado.
2. Ajuste a URL em `app/build.gradle.kts` no campo `API_BASE_URL` se necessario.
3. Abra a pasta `sigmo_nfc_android` no Android Studio.
4. Sincronize o projeto Gradle.
5. Rode em dois aparelhos Android com NFC.

## Fluxo esperado

1. Usuario A faz login e abre `Receber por aproximacao`.
2. Usuario B faz login e abre `Pagar por aproximacao`.
3. B encosta no celular de A.
4. O app de B detecta a sessao de A.
5. B informa o valor e confirma.
6. O backend liquida a transferencia Sigmo para Sigmo.
7. A tela de A atualiza a sessao como recebida.

## Estrutura principal

- `app/src/main/java/com/sigmo/nfc/ApiClient.kt`: cliente HTTP das rotas mobile/NFC
- `app/src/main/java/com/sigmo/nfc/SigmoHostApduService.kt`: emulacao de cartao no recebedor
- `app/src/main/java/com/sigmo/nfc/ReceiveNfcActivity.kt`: gera a sessao e publica o payload no HCE
- `app/src/main/java/com/sigmo/nfc/PayNfcActivity.kt`: le a sessao por `IsoDep`, resolve no backend e envia o pagamento
- `app/src/main/res/xml/apduservice.xml`: registro do AID NFC `F0395349474D4F01`

## Observacoes

- Este ambiente local nao possui Java/Gradle instalados, entao eu nao consegui compilar o app aqui.
- O projeto foi scaffoldado para abrir no Android Studio e sincronizar normalmente.
- O app usa apenas as rotas mobile/NFC novas; ele nao depende da autenticacao fraca do frontend web.
