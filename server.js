require('dotenv').config();

const express = require('express');
const sql = require('mssql');

const app = express();

const config = {
  port: parseInt(process.env.PORT || '3000', 10),
  pollIntervalMinutes: parseInt(process.env.POLL_INTERVAL_MINUTES || '30', 10),
  sql: {
    user: process.env.SQL_USER,
    password: process.env.SQL_PASSWORD,
    server: process.env.SQL_HOST,
    port: parseInt(process.env.SQL_PORT || '1433', 10),
    database: process.env.SQL_DB || 'zapresponder',
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
  },
  api: {
    baseUrl: process.env.ZAP_API_BASE_URL || 'https://api.zapresponder.com.br',
    token: process.env.ZAP_API_TOKEN,
    skip: parseInt(process.env.ZAP_API_SKIP || '0', 10),
    limit: parseInt(process.env.ZAP_API_LIMIT || '1000', 10),
  },
};

const status = {
  running: false,
  lastRunStartedAt: null,
  lastRunFinishedAt: null,
  lastSuccessAt: null,
  lastError: null,
  lastErrorAt: null,
  campaignsTotal: 0,
  campaignsProcessed: 0,
  logsInserted: 0,
  logInsertErrors: 0,
  currentCampaignId: null,
  progressPercent: 0,
  progressBar: '',
  lastLogCreatedAt: null,
};

let poolPromise = null;

function nowIso() {
  return new Date().toISOString();
}

function clampStr(value, maxLen) {
  if (value === null || value === undefined) return null;
  const str = String(value);
  return str.length > maxLen ? str.slice(0, maxLen) : str;
}

function toSqlDateTime(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (isNaN(d.getTime())) return null;
  d.setHours(d.getHours() - 3);
  return d;
}

function buildProgressBar(percent, width) {
  const size = width || 20;
  const filled = Math.round((percent / 100) * size);
  const empty = Math.max(size - filled, 0);
  return `[${'#'.repeat(filled)}${'-'.repeat(empty)}]`;
}

async function getPool() {
  if (!poolPromise) {
    poolPromise = sql.connect(config.sql);
  }
  return poolPromise;
}

async function fetchCampaigns(pool) {
  const query = 'SELECT * FROM campanhas ORDER BY created_at DESC;';
  const result = await pool.request().query(query);
  return result.recordset || [];
}

async function fetchLogsForCampaign(campaignId) {
  const url = new URL(`/campaigns/${campaignId}/logs`, config.api.baseUrl);
  url.searchParams.set('skip', String(config.api.skip));
  url.searchParams.set('limit', String(config.api.limit));

  const res = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${config.api.token}`,
      accept: 'application/json',
    },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`API ${res.status} ${res.statusText} ${body}`.trim());
  }

  return res.json();
}

function mapLogs(root) {
  const logs = Array.isArray(root?.logs) ? root.logs : [];
  return logs.map((l) => ({
    status: l?.status ?? null,
    destino: l?.destino ?? null,
    _id: l?.campanha ?? null,
    departamento: l?.departamento ?? null,
    template_name: l?.rawContact?.blocks?.[0]?.template?.name ?? null,
    contact_name: l?.rawContact?.contact?.name ?? null,
    createdAt: toSqlDateTime(l?.createdAt),
  }));
}

async function insertLog(pool, item) {
  const query = `
INSERT INTO dbo.logs_whatsapp (
  _id,
  status,
  destino,
  departamento,
  template_name,
  contact_name,
  created_at
)
VALUES (
  @id,
  @status,
  @destino,
  @departamento,
  @template_name,
  @contact_name,
  @created_at
);`;

  const request = pool.request();
  request.input('id', sql.VarChar(255), clampStr(item._id, 255));
  request.input('status', sql.VarChar(30), clampStr(item.status, 30));
  request.input('destino', sql.VarChar(30), clampStr(item.destino, 30));
  request.input('departamento', sql.VarChar(255), clampStr(item.departamento, 255));
  request.input('template_name', sql.VarChar(100), clampStr(item.template_name, 100));
  request.input('contact_name', sql.VarChar(255), clampStr(item.contact_name, 255));
  request.input('created_at', sql.DateTime2, item.createdAt);

  await request.query(query);
}

async function insertLogs(pool, mappedLogs) {
  let inserted = 0;
  for (const item of mappedLogs) {
    try {
      await insertLog(pool, item);
      inserted += 1;
      if (item.createdAt) {
        const iso = item.createdAt.toISOString();
        if (!status.lastLogCreatedAt || new Date(iso) > new Date(status.lastLogCreatedAt)) {
          status.lastLogCreatedAt = iso;
        }
      }
    } catch (err) {
      status.logInsertErrors += 1;
      status.lastError = `Insert failed: ${err.message}`;
      status.lastErrorAt = nowIso();
      console.error(status.lastError);
    }
  }
  return inserted;
}

async function runOnce() {
  if (status.running) {
    status.lastError = 'Skipped: previous run still in progress';
    status.lastErrorAt = nowIso();
    console.warn(status.lastError);
    return;
  }

  status.running = true;
  status.lastRunStartedAt = nowIso();
  status.lastRunFinishedAt = null;
  status.currentCampaignId = null;
  status.campaignsProcessed = 0;
  status.campaignsTotal = 0;
  status.logsInserted = 0;
  status.logInsertErrors = 0;
  status.progressPercent = 0;
  status.progressBar = buildProgressBar(0);
  status.lastError = null;
  status.lastErrorAt = null;

  let runHadError = false;

  try {
    if (!config.api.token) {
      throw new Error('Missing ZAP_API_TOKEN');
    }
    const pool = await getPool();
    const campaigns = await fetchCampaigns(pool);
    status.campaignsTotal = campaigns.length;

    for (let i = 0; i < campaigns.length; i += 1) {
      const campaign = campaigns[i];
      const campaignId = campaign?.id;
      status.currentCampaignId = campaignId || null;

      try {
        if (!campaignId) {
          throw new Error('Campaign missing id');
        }
        const root = await fetchLogsForCampaign(campaignId);
        const mapped = mapLogs(root);
        const inserted = await insertLogs(pool, mapped);
        status.logsInserted += inserted;
      } catch (err) {
        runHadError = true;
        status.lastError = `Campaign ${campaignId} failed: ${err.message}`;
        status.lastErrorAt = nowIso();
        console.error(status.lastError);
      }

      status.campaignsProcessed = i + 1;
      status.progressPercent = status.campaignsTotal
        ? Math.round((status.campaignsProcessed / status.campaignsTotal) * 100)
        : 100;
      status.progressBar = buildProgressBar(status.progressPercent);
      console.log(
        `Progress ${status.progressBar} ${status.campaignsProcessed}/${status.campaignsTotal} campaigns`
      );
    }

    status.lastRunFinishedAt = nowIso();
    if (!runHadError) {
      status.lastSuccessAt = status.lastRunFinishedAt;
    }
  } catch (err) {
    status.lastError = err.message;
    status.lastErrorAt = nowIso();
    status.lastRunFinishedAt = nowIso();
    console.error(status.lastError);
  } finally {
    status.running = false;
  }
}

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    running: status.running,
    lastRunStartedAt: status.lastRunStartedAt,
    lastRunFinishedAt: status.lastRunFinishedAt,
  });
});

app.get('/status', (req, res) => {
  res.json(status);
});

app.listen(config.port, () => {
  console.log(`Server listening on port ${config.port}`);
  const delayMs = config.pollIntervalMinutes * 60 * 1000;
  const runLoop = async () => {
    await runOnce();
    setTimeout(runLoop, delayMs);
  };
  runLoop();
});

process.on('SIGINT', async () => {
  try {
    const pool = await poolPromise;
    if (pool) {
      await pool.close();
    }
  } finally {
    process.exit(0);
  }
});
