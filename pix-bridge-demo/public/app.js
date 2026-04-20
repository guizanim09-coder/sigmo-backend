const modal = document.getElementById("modalSaldo");
const amountInput = document.getElementById("valorAdicionarSaldo");
const previewLiquido = document.getElementById("previewLiquido");
const etapaAcoes = document.getElementById("etapaAcoes");
const pedidoEntradaResumo = document.getElementById("pedidoEntradaResumo");
const toast = document.getElementById("toast");
const generatePixButton = document.getElementById("generatePixButton");
const copyPixButton = document.getElementById("copyPixButton");
const newPixButton = document.getElementById("newPixButton");
const API_BASE_URL = String(window.APP_CONFIG?.API_BASE_URL || "").trim();

let currentPixKey = "";

function formatCurrency(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL"
  }).format(Number(value || 0));
}

function openModal() {
  modal.classList.add("open");
  amountInput.focus();
}

function closeModal() {
  modal.classList.remove("open");
}

function showToast(message) {
  toast.textContent = message;
  toast.classList.add("show");
  window.clearTimeout(showToast.timer);
  showToast.timer = window.setTimeout(() => {
    toast.classList.remove("show");
  }, 2200);
}

function updatePreview() {
  const amount = Number(amountInput.value || 0);
  previewLiquido.textContent = `Sera processado: ${formatCurrency(amount)}`;
}

function resetGeneratedState(clearAmount = false) {
  currentPixKey = "";
  etapaAcoes.hidden = true;
  pedidoEntradaResumo.innerHTML = "";

  if (clearAmount) {
    amountInput.value = "";
    updatePreview();
  }
}

async function generatePix() {
  const amount = Number(amountInput.value || 0);

  if (!amount || amount <= 0) {
    showToast("Digite um valor valido.");
    amountInput.focus();
    return;
  }

  generatePixButton.disabled = true;
  generatePixButton.textContent = "Gerando chave PIX...";
  resetGeneratedState(false);

  try {
    const response = await fetch(`${API_BASE_URL}/api/pix`, {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify({ amount })
    });

    const data = await response.json();

    if (!response.ok || !data.ok) {
      throw new Error(data.error || "Nao foi possivel gerar a chave PIX.");
    }

    currentPixKey = String(data.pixKey || "");
    etapaAcoes.hidden = false;

    pedidoEntradaResumo.innerHTML = [
      `<p><span>Valor</span><strong>${formatCurrency(data.amount)}</strong></p>`,
      `<p><span>Order ID</span><strong>${data.orderId}</strong></p>`,
      `<div class="pix-key-box">`,
      `<strong>Chave PIX Copia e Cola</strong>`,
      `<div class="pix-key-text">${currentPixKey}</div>`,
      `</div>`
    ].join("");

    showToast("Chave PIX gerada com sucesso.");
  } catch (error) {
    showToast(error.message);
  } finally {
    generatePixButton.disabled = false;
    generatePixButton.textContent = "Gerar chave PIX";
  }
}

async function copyPixKey() {
  if (!currentPixKey) {
    showToast("Nenhuma chave PIX disponivel para copiar.");
    return;
  }

  try {
    await navigator.clipboard.writeText(currentPixKey);
    showToast("Chave PIX copiada.");
  } catch (_error) {
    showToast("Nao foi possivel copiar automaticamente.");
  }
}

document.getElementById("openModalButton").addEventListener("click", openModal);
document.getElementById("closeModalButton").addEventListener("click", closeModal);
generatePixButton.addEventListener("click", generatePix);
copyPixButton.addEventListener("click", copyPixKey);
newPixButton.addEventListener("click", () => {
  resetGeneratedState(true);
  amountInput.focus();
});
amountInput.addEventListener("input", updatePreview);

modal.addEventListener("click", (event) => {
  if (event.target === modal) {
    closeModal();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeModal();
  }
});

updatePreview();
