# Postgres MCP Server (regional_db)

Minimal HTTP bridge to expose the public schema of `regional_db` for Claude Code/Perplexity MCP.

## Prereqs
- Node 18+.
- DB creds in `backend/.env` (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD).
- Backend dependencies already include `pg` and `express`.

## Run the server
From `duisburg-web-application/backend`:
```bash
# ensure deps are installed
npm install

# start MCP server (reads backend/.env); default port 4545 unless MCP_PORT/PORT set
MCP_PORT=4545 npm run mcp
# or: MCP_PORT=4545 node mcp/postgres-server.js
```

Server defaults:
- Port: 4545 (override with PORT env)
- Routes:
  - POST `/query` { sql, params? } -> rows
  - POST `/exec` { sql, params? } -> rowCount
  - GET `/health`
- Schema: public (no schema filtering applied)
- Mode: read/write (no DDL filter beyond a simple DROP/TRUNCATE regex)

## Claude/Perplexity MCP config example
Point your MCP client to run the server command and pass env vars:
```json
{
  "mcpServers": {
    "regional_db": {
      "command": "node",
      "args": ["mcp/postgres-server.js"],
      "env": {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
      "DB_NAME": "regional_db",
      "DB_USER": "fadzie",
      "DB_PASSWORD": "",
      "MCP_PORT": "4545"
    }
  }
}
}
```

## Notes / Safety
- This is intentionally minimal: it permits arbitrary SQL unless blocked by the simple safety regex. Use only in trusted environments. If you want stricter controls, we can:
  - Make it read-only.
  - Add table/column allowlists.
  - Reject DDL entirely.
