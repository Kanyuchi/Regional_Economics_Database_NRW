# Duisburg Economic Dashboard

A comprehensive dashboard for visualizing regional economic data for Duisburg and neighboring cities in the NRW region.

## Features

- **Overview Tab**: Key information about Duisburg and comparison cities
- **Demographics Tab**: Population and demographic indicators comparison
- **Labor Market Tab**: Employment, unemployment, and labor market metrics
- **Trends Tab**: Historical time series data with D3.js line charts
- **Interactive Visualizations**: Bar charts and line charts with tooltips
- **City Comparison**: Compare Duisburg with Düsseldorf, Essen, Oberhausen, Mülheim an der Ruhr
- **Chat Assistant**: Docked chat drawer with minimize/reset; slides the dashboard left to keep tabs visible; supports table responses and chart-aware answers.

## Technology Stack

### Backend
- Node.js
- Express
- PostgreSQL (pg driver)
- CORS enabled

### Frontend
- React 19
- Vite (build tool)
- D3.js (data visualization)
- Axios (API calls)

## Database

The dashboard connects to the `regional_db` PostgreSQL database containing:
- Demographics data
- Labor market indicators
- Business economy metrics
- Public finance data
- Time series from 1995-2024

## Setup Instructions

### Prerequisites
- Node.js (v16 or higher)
- PostgreSQL 17 (already installed and running)
- regional_db database (already configured)

### Backend Setup

1. Navigate to the backend directory:
   ```bash
   cd /Users/fadzie/Desktop/duisburg-dashboard/backend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Verify the `.env` file settings:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=regional_db
   DB_USER=fadzie
   DB_PASSWORD=
   PORT=3001
   OPENAI_API_KEY= # optional, required for LLM chatbot
   OPENAI_MODEL=gpt-4o-mini
   ```

4. Start the backend server:
   ```bash
   npm start
   ```

   Or for development with auto-reload:
   ```bash
   npm run dev
   ```

   The API will be available at `http://localhost:3001`

### Frontend Setup

1. Navigate to the frontend directory:
   ```bash
   cd /Users/fadzie/Desktop/duisburg-dashboard/frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   npm run dev
   ```

   The dashboard will be available at `http://localhost:5173`

### Frontend Build for Production

1. Set the API base (optional; defaults to current origin):
   ```bash
   export VITE_API_BASE=https://your-backend.example.com
   ```
2. Build:
   ```bash
   npm run build
   ```
   Deploy `frontend/dist` to your static host (Vercel/Netlify/Cloudflare Pages) or serve via Nginx/your backend. Ensure CORS if frontend/backends are on different origins.

### Docker Deploy (Backend)
1. Build:
   ```bash
   cd duisburg-web-application/backend
   docker build -t duisburg-backend .
   ```
2. Run (envs required):
   ```bash
   docker run -p 3001:3001 \
     -e DB_HOST=... -e DB_PORT=5432 -e DB_NAME=regional_db \
     -e DB_USER=... -e DB_PASSWORD=... -e PORT=3001 \
     duisburg-backend
   ```

### Nginx + Static Frontend + API Proxy
- Build frontend (`npm run build` in `frontend`) and place `dist/` in `/usr/share/nginx/html`.
- Use `deploy/nginx.conf` as a template to proxy `/api` to the backend and serve the SPA with fallback to `index.html`.
- Adjust `backend_api` upstream to your backend host:port.

### Docker Compose (Backend + Frontend)
- Prereq: Docker/Compose installed.
- From `duisburg-web-application/`:
  ```bash
  docker-compose up --build
  ```
  - Backend available at `http://localhost:3001`
  - Frontend at `http://localhost:8080` (proxies `/api` to backend)
- Frontend build arg in compose defaults `VITE_API_BASE=http://backend:3001`.
- Backend envs pulled from `backend/.env`; ensure they’re set before running compose.

### Hosting Suggestions
- Backend: Render/Railway/Fly/DigitalOcean App Platform with envs for DB and PORT.
- Frontend: Vercel/Netlify/Cloudflare Pages (static) or Nginx serving `dist/` with `/api` proxy.

## Running the Dashboard

1. **Start Backend** (Terminal 1):
   ```bash
   cd /Users/fadzie/Desktop/duisburg-dashboard/backend
   npm start
   ```

2. **Start Frontend** (Terminal 2):
   ```bash
   cd /Users/fadzie/Desktop/duisburg-dashboard/frontend
   npm run dev
   ```

3. Open your browser to `http://localhost:5173`

## AI Chatbot (Optional)

The chatbot now mixes deterministic data answers with LLM phrasing:

1. Set `OPENAI_API_KEY` (and optionally `OPENAI_MODEL`) in `backend/.env`.
2. Restart the backend (`npm start` or `npm run dev`).
3. The frontend chat widget uses:
   - Deterministic, DB-backed responses for business registrations/deregistrations, unemployment counts, and general indicator time series (e.g., “doctors in Duisburg 2019-2024”), returned with tables.
   - Chart-aware context: current tab/indicator/year/chart type/view mode/selected city and recent chart data are sent to the backend so the bot can describe what you’re viewing.
   - LLM fallback only when no deterministic path matches; the model is instructed not to invent numbers.

## API Endpoints

- `GET /api/health` - Health check
- `GET /api/cities` - Get all comparison cities
- `GET /api/duisburg` - Get Duisburg information
- `GET /api/demographics/:year` - Demographics data for a specific year
- `GET /api/labor-market/:year` - Labor market data for a specific year
- `GET /api/business-economy/:year` - Business economy data for a specific year
- `GET /api/public-finance/:year` - Public finance data for a specific year
- `GET /api/timeseries/:indicatorCode` - Time series data for an indicator
- `GET /api/indicators` - Get all available indicators
- `GET /api/years` - Get all available years
- `POST /api/chat` - Chat assistant (deterministic responses + LLM fallback)

## MCP (Postgres) setup

If you want the database accessible to Claude Code/Perplexity via MCP, use the custom server under `backend/mcp/postgres-server.js` (see `mcp/postgres-mcp.md`). It reads `backend/.env`, exposes the public schema, and supports read/write by default. Set `MCP_PORT` (or `PORT`) to avoid conflicts with the API port (e.g., `MCP_PORT=4545 npm run mcp`).

## Dashboard Features

### Overview Tab
- Duisburg key information (region code, type, area)
- List of comparison cities with Ruhr area designation

### Demographics Tab
- Bar charts comparing demographic indicators across cities
- Filterable by year
- Duisburg highlighted in blue

### Labor Market Tab
- Employment and unemployment metrics
- Cross-city comparisons
- Year selection

### Trends Tab
- Line charts showing historical trends
- Multiple cities on same chart
- Indicator selection dropdown
- Time range: 2010-2024

## Data Caching

The dashboard queries data on-demand. Since regional economic data doesn't change frequently, you can:
- Implement caching in the backend for better performance
- Add a data refresh button
- Schedule periodic data updates

## Future Enhancements

- Add more visualization types (pie charts, heat maps)
- Implement data export (CSV, PDF)
- Add filters for specific economic sectors
- Include commuter data visualization
- Add infrastructure and healthcare metrics
- Implement user preferences/saved views
- Add comparison with state (NRW) averages

## Troubleshooting

### Backend won't start
- Ensure PostgreSQL is running: `brew services list`
- Check database connection in `.env`
- Verify port 3001 is not in use

### Frontend shows error
- Ensure backend is running on port 3001
- Check browser console for errors
- Verify API calls in Network tab

### No data showing
- Check that regional_db has data
- Verify year selection matches available data
- Check browser console for API errors

## License

Private project for regional economic analysis.
