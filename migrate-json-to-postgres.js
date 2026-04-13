const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");

const DB_FILE = path.join(__dirname, "database.json");
const DATABASE_URL =
  process.env.DATABASE_PUBLIC_URL ||
  process.env.DATABASE_URL ||
  "";

if (!DATABASE_URL) {
  console.error("DATABASE_URL não configurada.");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

function readJSON() {
  if (!fs.existsSync(DB_FILE)) {
    return { usuarios: [], depositos: [] };
  }

  return JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
}

async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      id TEXT PRIMARY KEY,
      nome TEXT,
      email TEXT UNIQUE,
      senha TEXT,
      saldo NUMERIC DEFAULT 0,
      criado_em TIMESTAMP,
      nome_atualizado_em TIMESTAMP,
      saldo_atualizado_em TIMESTAMP,
      senha_atualizada_em TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS depositos (
      id TEXT PRIMARY KEY,
      user_id TEXT,
      valor NUMERIC,
      chave_pix TEXT,
      tipo_chave TEXT,
      tipo_transacao TEXT,
      status TEXT,
      comprovante_url TEXT,
      descricao TEXT,
      criado_em TIMESTAMP,
      aprovado_em TIMESTAMP,
      recusado_em TIMESTAMP,
      comprovante_enviado_em TIMESTAMP
    );
  `);
}

async function migrateUsers(users) {
  let inserted = 0;
  let updated = 0;

  for (const u of users) {
    const existing = await pool.query(
      "SELECT id FROM usuarios WHERE id = $1 OR email = $2 LIMIT 1",
      [u.id, String(u.email || "").toLowerCase()]
    );

    await pool.query(
      `
      INSERT INTO usuarios (
        id, nome, email, senha, saldo, criado_em,
        nome_atualizado_em, saldo_atualizado_em, senha_atualizada_em
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT (id) DO UPDATE SET
        nome = EXCLUDED.nome,
        email = EXCLUDED.email,
        senha = EXCLUDED.senha,
        saldo = EXCLUDED.saldo,
        criado_em = COALESCE(usuarios.criado_em, EXCLUDED.criado_em),
        nome_atualizado_em = EXCLUDED.nome_atualizado_em,
        saldo_atualizado_em = EXCLUDED.saldo_atualizado_em,
        senha_atualizada_em = EXCLUDED.senha_atualizada_em
      `,
      [
        u.id,
        u.nome || "",
        String(u.email || "").trim().toLowerCase(),
        u.senha || "",
        Number(u.saldo || 0),
        u.criadoEm || new Date().toISOString(),
        u.nomeAtualizadoEm || null,
        u.saldoAtualizadoEm || null,
        u.senhaAtualizadaEm || null
      ]
    );

    if (existing.rows.length) updated++;
    else inserted++;
  }

  return { inserted, updated };
}

async function migrateDeposits(deps) {
  let inserted = 0;
  let updated = 0;

  for (const d of deps) {
    const existing = await pool.query(
      "SELECT id FROM depositos WHERE id = $1 LIMIT 1",
      [d.id]
    );

    await pool.query(
      `
      INSERT INTO depositos (
        id, user_id, valor, chave_pix, tipo_chave, tipo_transacao, status,
        comprovante_url, descricao, criado_em, aprovado_em, recusado_em, comprovante_enviado_em
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
      ON CONFLICT (id) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        valor = EXCLUDED.valor,
        chave_pix = EXCLUDED.chave_pix,
        tipo_chave = EXCLUDED.tipo_chave,
        tipo_transacao = EXCLUDED.tipo_transacao,
        status = EXCLUDED.status,
        comprovante_url = EXCLUDED.comprovante_url,
        descricao = EXCLUDED.descricao,
        criado_em = COALESCE(depositos.criado_em, EXCLUDED.criado_em),
        aprovado_em = EXCLUDED.aprovado_em,
        recusado_em = EXCLUDED.recusado_em,
        comprovante_enviado_em = EXCLUDED.comprovante_enviado_em
      `,
      [
        d.id,
        d.userId,
        Number(d.valor || 0),
        d.chavePix || "",
        d.tipoChave || "",
        d.tipoTransacao || "entrada",
        d.status || "pendente",
        d.comprovanteUrl || "",
        d.descricao || "",
        d.criadoEm || new Date().toISOString(),
        d.aprovadoEm || null,
        d.recusadoEm || null,
        d.comprovanteEnviadoEm || null
      ]
    );

    if (existing.rows.length) updated++;
    else inserted++;
  }

  return { inserted, updated };
}

async function main() {
  const json = readJSON();

  console.log("Iniciando migração...");
  console.log(`Usuários no JSON: ${json.usuarios.length}`);
  console.log(`Depósitos no JSON: ${json.depositos.length}`);

  await ensureTables();

  const userStats = await migrateUsers(json.usuarios || []);
  const depStats = await migrateDeposits(json.depositos || []);

  console.log("Migração concluída.");
  console.log("Usuários:", userStats);
  console.log("Depósitos:", depStats);

  await pool.end();
}

main().catch(async (err) => {
  console.error("Erro na migração:", err);
  try {
    await pool.end();
  } catch {}
  process.exit(1);
});