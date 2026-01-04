const express = require('express');
const cors = require('cors');
const pool = require('./db');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', message: 'Duisburg Dashboard API is running' });
});

// Get all cities in NRW (focusing on Duisburg and neighbors)
app.get('/api/cities', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        geo_id,
        region_code,
        region_name,
        region_type,
        ruhr_area,
        lower_rhine_region,
        population_2023,
        area_sqkm,
        latitude,
        longitude
      FROM dim_geography
      WHERE region_type = 'urban_district'
        AND region_name IN ('Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr', 'Krefeld', 'Bochum', 'Dortmund')
      ORDER BY region_name
    `);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching cities:', err);
    res.status(500).json({ error: 'Failed to fetch cities' });
  }
});

// Get Duisburg specific data
app.get('/api/duisburg', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        geo_id,
        region_code,
        region_name,
        region_type,
        ruhr_area,
        population_2023,
        area_sqkm,
        latitude,
        longitude
      FROM dim_geography
      WHERE region_name = 'Duisburg'
    `);
    res.json(result.rows[0]);
  } catch (err) {
    console.error('Error fetching Duisburg data:', err);
    res.status(500).json({ error: 'Failed to fetch Duisburg data' });
  }
});

// Get demographics data for comparison
app.get('/api/demographics/:year?', async (req, res) => {
  try {
    const year = req.params.year || 2023;
    const result = await pool.query(`
      SELECT
        g.region_name,
        g.region_code,
        i.indicator_name,
        i.indicator_code,
        i.unit_of_measure,
        t.year,
        t.quarter,
        SUM(fd.value) as value,
        STRING_AGG(DISTINCT fd.notes, '; ') as notes
      FROM fact_demographics fd
      JOIN dim_geography g ON fd.geo_id = g.geo_id
      JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
      JOIN dim_time t ON fd.time_id = t.time_id
      WHERE g.region_type = 'urban_district'
        AND g.region_name IN ('Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr')
        AND t.year = $1
        AND i.is_active = true
        AND (i.indicator_category IN ('Demographics', 'demographics', 'Health', 'Infrastructure'))
        AND (fd.age_group = 'total' OR fd.age_group IS NULL)
      GROUP BY g.region_name, g.region_code, i.indicator_name, i.indicator_code, i.unit_of_measure, t.year, t.quarter
      ORDER BY g.region_name, i.indicator_name
    `, [year]);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching demographics:', err);
    res.status(500).json({ error: 'Failed to fetch demographics data' });
  }
});

// Get labor market data for comparison
app.get('/api/labor-market/:year?', async (req, res) => {
  try {
    const year = req.params.year || 2023;
    const result = await pool.query(`
      SELECT
        g.region_name,
        g.region_code,
        i.indicator_name,
        i.indicator_code,
        i.unit_of_measure,
        t.year,
        t.quarter,
        SUM(fd.value) as value,
        STRING_AGG(DISTINCT fd.notes, '; ') as notes
      FROM fact_demographics fd
      JOIN dim_geography g ON fd.geo_id = g.geo_id
      JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
      JOIN dim_time t ON fd.time_id = t.time_id
      WHERE g.region_type = 'urban_district'
        AND g.region_name IN ('Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr')
        AND t.year = $1
        AND i.is_active = true
        AND (i.indicator_category = 'Labor Market' OR i.indicator_category = 'labor_market')
        AND (fd.age_group = 'total' OR fd.age_group IS NULL)
      GROUP BY g.region_name, g.region_code, i.indicator_name, i.indicator_code, i.unit_of_measure, t.year, t.quarter
      ORDER BY g.region_name, i.indicator_name
    `, [year]);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching labor market data:', err);
    res.status(500).json({ error: 'Failed to fetch labor market data' });
  }
});

// Get business economy data for comparison
app.get('/api/business-economy/:year?', async (req, res) => {
  try {
    const year = req.params.year || 2023;
    const result = await pool.query(`
      SELECT
        g.region_name,
        g.region_code,
        i.indicator_name,
        i.indicator_code,
        i.unit_of_measure,
        t.year,
        t.quarter,
        SUM(fd.value) as value,
        STRING_AGG(DISTINCT fd.notes, '; ') as notes
      FROM fact_demographics fd
      JOIN dim_geography g ON fd.geo_id = g.geo_id
      JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
      JOIN dim_time t ON fd.time_id = t.time_id
      WHERE g.region_type = 'urban_district'
        AND g.region_name IN ('Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr')
        AND t.year = $1
        AND i.is_active = true
        AND (i.indicator_category IN ('business_economy', 'GDP/GVA', 'Employee Compensation'))
        AND (fd.age_group = 'total' OR fd.age_group IS NULL)
      GROUP BY g.region_name, g.region_code, i.indicator_name, i.indicator_code, i.unit_of_measure, t.year, t.quarter
      ORDER BY g.region_name, i.indicator_name
    `, [year]);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching business economy data:', err);
    res.status(500).json({ error: 'Failed to fetch business economy data' });
  }
});

// Get public finance data for comparison
app.get('/api/public-finance/:year?', async (req, res) => {
  try {
    const year = req.params.year || 2023;
    const result = await pool.query(`
      SELECT
        g.region_name,
        g.region_code,
        i.indicator_name,
        i.indicator_code,
        i.unit_of_measure,
        t.year,
        t.quarter,
        SUM(fd.value) as value,
        STRING_AGG(DISTINCT fd.notes, '; ') as notes
      FROM fact_demographics fd
      JOIN dim_geography g ON fd.geo_id = g.geo_id
      JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
      JOIN dim_time t ON fd.time_id = t.time_id
      WHERE g.region_type = 'urban_district'
        AND g.region_name IN ('Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr')
        AND t.year = $1
        AND i.is_active = true
        AND (i.indicator_category IN ('Public Finance', 'public_finance'))
        AND (fd.age_group = 'total' OR fd.age_group IS NULL)
      GROUP BY g.region_name, g.region_code, i.indicator_name, i.indicator_code, i.unit_of_measure, t.year, t.quarter
      ORDER BY g.region_name, i.indicator_name
    `, [year]);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching public finance data:', err);
    res.status(500).json({ error: 'Failed to fetch public finance data' });
  }
});

// Get time series data for a specific indicator and cities
app.get('/api/timeseries/:indicatorCode', async (req, res) => {
  try {
    const { indicatorCode } = req.params;
    const { startYear, endYear, cities } = req.query;

    const cityList = cities ? cities.split(',') : ['Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr'];
    const start = startYear || 2010;
    const end = endYear || 2024;

    const result = await pool.query(`
      SELECT
        g.region_name,
        g.region_code,
        i.indicator_name,
        i.indicator_code,
        i.unit_of_measure,
        t.year,
        t.quarter,
        SUM(fd.value) as value
      FROM fact_demographics fd
      JOIN dim_geography g ON fd.geo_id = g.geo_id
      JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
      JOIN dim_time t ON fd.time_id = t.time_id
      WHERE g.region_type = 'urban_district'
        AND g.region_name = ANY($1)
        AND i.indicator_code = $2
        AND t.year BETWEEN $3 AND $4
        AND (fd.age_group = 'total' OR fd.age_group IS NULL)
      GROUP BY g.region_name, g.region_code, i.indicator_name, i.indicator_code, i.unit_of_measure, t.year, t.quarter
      ORDER BY g.region_name, t.year, t.quarter
    `, [cityList, indicatorCode, start, end]);

    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching time series data:', err);
    res.status(500).json({ error: 'Failed to fetch time series data' });
  }
});

// Get available indicators
app.get('/api/indicators', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        indicator_id,
        indicator_code,
        indicator_name,
        indicator_category,
        indicator_subcategory,
        unit_of_measure,
        description,
        update_frequency
      FROM dim_indicator
      WHERE is_active = true
      ORDER BY indicator_category, indicator_name
    `);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching indicators:', err);
    res.status(500).json({ error: 'Failed to fetch indicators' });
  }
});

// Get available years (only years with actual data)
app.get('/api/years', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT t.year
      FROM dim_time t
      JOIN fact_demographics f ON t.time_id = f.time_id
      ORDER BY t.year DESC
    `);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching years:', err);
    res.status(500).json({ error: 'Failed to fetch years' });
  }
});

app.listen(PORT, () => {
  console.log(`\n🚀 Duisburg Dashboard API running on http://localhost:${PORT}`);
  console.log(`📊 Database: ${process.env.DB_NAME}`);
  console.log(`\nAvailable endpoints:`);
  console.log(`  GET /api/health - Health check`);
  console.log(`  GET /api/cities - Get all cities`);
  console.log(`  GET /api/duisburg - Get Duisburg info`);
  console.log(`  GET /api/demographics/:year - Demographics data`);
  console.log(`  GET /api/labor-market/:year - Labor market data`);
  console.log(`  GET /api/business-economy/:year - Business economy data`);
  console.log(`  GET /api/public-finance/:year - Public finance data`);
  console.log(`  GET /api/timeseries/:indicatorCode - Time series data`);
  console.log(`  GET /api/indicators - Available indicators`);
  console.log(`  GET /api/years - Available years\n`);
});
