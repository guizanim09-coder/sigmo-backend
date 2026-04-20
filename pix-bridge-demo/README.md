# Pix Bridge Demo

Este exemplo mostra a estrutura segura que voce descreveu:

- `exemplomascara.com` tem a interface que o usuario enxerga;
- `dentpeg.com/checkout/sigmo` e a pagina de checkout;
- `https://api.dentpeg.com/api/v1/deposits` e a API oficial para criar a cobranca PIX;
- a chave PIX volta para o frontend da mascara sem carregar a pagina do outro dominio.

## Arquivos

- `mask-server.js`
  Servidor que simula o `exemplomascara.com`.
- `link-server.js`
  Servidor que simula o upstream `dentpeg.com/checkout/sigmo`.
- `public/index.html`
  Estrutura HTML da interface inspirada no frontend da Sigmo.
- `public/styles.css`
  Estilos organizados da tela.
- `public/app.js`
  Comportamento do modal, geracao da chave e copia do PIX.
- `dentpeg-checkout.js`
  Automacao Playwright do checkout `dentpeg.com/checkout/sigmo`.

## Fluxo

1. O usuario abre `exemplomascara.com/sigmo`.
2. Ele digita o valor e clica em `Gerar chave PIX`.
3. O navegador envia `POST /api/pix` para `exemplomascara.com`.
4. O backend do `exemplomascara.com` faz uma chamada server-to-server para o upstream configurado.
5. O upstream cria o pedido e devolve a chave PIX.
6. O `exemplomascara.com` devolve essa resposta ao navegador.
7. A tela mostra a chave PIX no layout da mascara.

## Como rodar

Em um terminal:

```bash
npm install
npm run start:link
```

Em outro terminal:

```bash
npm run start:mask
```

Abra:

- `http://localhost:4100` para ver o lado upstream simulado;
- `http://localhost:3100/sigmo` para ver o lado `exemplomascara.com`.

## Variaveis

- `LINK_PORT=4100`
- `MASK_PORT=3100`
- `UPSTREAM_API_BASE=http://localhost:4100`
- `FRONTEND_ORIGIN=http://localhost:5500`
- `DENTPEG_API_BASE=https://api.dentpeg.com/api/v1`
- `DENTPEG_API_KEY=sua_api_key_dentpeg`
- `DENTPEG_CHECKOUT_URL=https://dentpeg.com/checkout/sigmo`
- `DENTPEG_HEADLESS=true`
- `DENTPEG_TIMEOUT_MS=45000`

## O ponto principal

O usuario usa a interface do `exemplomascara.com`, mas a cobranca e criada por uma API separada no upstream configurado. Nao e necessario carregar a pagina do upstream dentro da pagina da mascara para isso funcionar.

## Ajustes visuais

Se voce quiser editar o visual, trabalhe nestes arquivos:

- `public/index.html` para a estrutura da tela;
- `public/styles.css` para layout, cores e responsividade;
- `public/app.js` para o fluxo de abrir modal, gerar chave e copiar.

## Frontend no Netlify + backend no Railway

Para usar o frontend separado do backend:

- suba o backend `mask-server.js` no Railway com `npm start`;
- se quiser usar API oficial, configure `DENTPEG_API_KEY`;
- se nao quiser usar API oficial, deixe a chave vazia e o backend vai tentar usar o checkout `dentpeg.com/checkout/sigmo` via Playwright;
- use `UPSTREAM_API_BASE` apenas se estiver testando com um simulador local;
- configure `FRONTEND_ORIGIN` com o dominio do Netlify, por exemplo `https://seu-site.netlify.app`;
- no frontend estatico, ajuste `public/config.js` para apontar `API_BASE_URL` para a URL publica do Railway.

Exemplo:

```js
window.APP_CONFIG = {
  API_BASE_URL: "https://seu-backend.up.railway.app"
};
```
