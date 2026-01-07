#!/usr/bin/env node
/**
 * Minimal Postgres MCP server (read/write) for regional_db.
 *
 * Usage:
 *   # In duisburg-web-application/backend
 *   npm install pg @modelcontextprotocol/server express
 *   node ../mcp/postgres-server.js
 *
 * Config via env (reads backend/.env):
 *   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
 *   PORT (MCP HTTP port, default 4545)
 *
 * Exposes public schema with read/write:
 *   - Query: POST /query  { sql, params? }  (returns rows)
 *   - Exec:  POST /exec   { sql, params? }  (returns rowCount)
 *
 * WARNING: This is intentionally minimal; it does not sandbox SQL.
 * Use only in trusted environments. For tighter control, add allowlists
 * or reject DDL/unsafe statements.
 */

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
require('dotenv').config({ path: './.env' });

const app = express();
const PORT = process.env.MCP_PORT || process.env.PORT || 4545;

app.use(cors());
app.use(express.json());

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

pool.on('error', (err) => {
  console.error('PG pool error:', err);
});

const disallowPatterns = [/;\s*drop\s/i, /;\s*truncate\s/i];

function isSqlSafe(sql) {
  return !disallowPatterns.some((re) => re.test(sql));
}

app.post('/query', async (req, res) => {
  const { sql, params = [] } = req.body || {};
  if (!sql) {
    return res.status(400).json({ error: 'sql is required' });
  }
  if (!isSqlSafe(sql)) {
    return res.status(400).json({ error: 'SQL rejected by safety filter' });
  }
  try {
    const result = await pool.query(sql, params);
    return res.json({ rows: result.rows });
  } catch (err) {
    console.error('Query error:', err);
    return res.status(500).json({ error: err.message });
  }
});

app.post('/exec', async (req, res) => {
  const { sql, params = [] } = req.body || {};
  if (!sql) {
    return res.status(400).json({ error: 'sql is required' });
  }
  if (!isSqlSafe(sql)) {
    return res.status(400).json({ error: 'SQL rejected by safety filter' });
  }
  try {
    const result = await pool.query(sql, params);
    return res.json({ rowCount: result.rowCount });
  } catch (err) {
    console.error('Exec error:', err);
    return res.status(500).json({ error: err.message });
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.listen(PORT, () => {
  console.log(`Postgres MCP server running on http://localhost:${PORT}`);
  console.log(`Using DB ${process.env.DB_NAME} on ${process.env.DB_HOST}:${process.env.DB_PORT}`);
});
