# Duisburg Economic Dashboard

A comprehensive dashboard for visualizing regional economic data for Duisburg and neighboring cities in the NRW region.

## Features

- **Overview Tab**: Key information about Duisburg and comparison cities
- **Demographics Tab**: Population and demographic indicators comparison
- **Labor Market Tab**: Employment, unemployment, and labor market metrics
- **Trends Tab**: Historical time series data with D3.js line charts
- **Interactive Visualizations**: Bar charts and line charts with tooltips
- **City Comparison**: Compare Duisburg with Düsseldorf, Essen, Oberhausen, Mülheim an der Ruhr

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
