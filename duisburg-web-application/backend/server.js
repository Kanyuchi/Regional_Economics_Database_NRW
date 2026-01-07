const express = require('express');
const cors = require('cors');
const pool = require('./db');
const OpenAI = require('openai');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 5000;
const HOST = process.env.HOST || '0.0.0.0';
const CITY_LIST = ['Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr'];
const openaiClient = process.env.OPENAI_API_KEY
  ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
  : null;
const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';
const RATE_KEYWORDS = ['rate', 'quote', 'prozent', 'percent'];
const INDICATOR_SYNONYMS = {
  doctors: ['arzt', 'ärzte', 'arztzahl', 'doctors', 'physicians', 'mediziner', 'arztpraxen'],
  gdp: ['gdp', 'bip', 'bruttoinlandsprodukt'],
  unemployment: ['arbeitslose', 'arbeitslosen', 'arbeitslosenquote', 'unemployment'],
  employment: ['beschäftigte', 'employment'],
};
const INDICATOR_ALIAS = {
  doctors: 'full_time_physicians_hospitals',
  physicians: 'full_time_physicians_hospitals',
  arzt: 'full_time_physicians_hospitals',
  ärzte: 'full_time_physicians_hospitals',
  gdp: 'GDP_MARKET_PRICE',
  bip: 'GDP_MARKET_PRICE',
};

// Hard whitelist for safe indicator access (add here as needed)
const ALLOWED_INDICATORS = new Set([
  'GDP_MARKET_PRICE',
  'GDP_PER_EMPLOYED',
  'full_time_physicians_hospitals',
]);

// Fetch latest population per city to ground AI answers
async function getLatestPopulationData() {
  const result = await pool.query(
    `
    SELECT region_name, year, value
    FROM (
      SELECT
        g.region_name,
        t.year,
        fd.value,
        ROW_NUMBER() OVER (
          PARTITION BY g.region_name
          ORDER BY t.year DESC, t.quarter DESC
        ) AS rn
      FROM fact_demographics fd
      JOIN dim_geography g ON fd.geo_id = g.geo_id
      JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
      JOIN dim_time t ON fd.time_id = t.time_id
      WHERE i.indicator_name ILIKE '%Bevölkerung%'
        AND g.region_type = 'urban_district'
        AND g.region_name = ANY($1)
        AND (fd.age_group = 'total' OR fd.age_group IS NULL)
    ) ranked
    WHERE rn = 1
    ORDER BY value::numeric DESC NULLS LAST;
  `,
    [CITY_LIST]
  );

  return result.rows.map((row) => ({
    city: row.region_name,
    year: parseInt(row.year, 10),
    value: parseFloat(row.value),
  }));
}

function buildPopulationContext(popData) {
  if (!popData || popData.length === 0) {
    return 'No population data available for the requested cities.';
  }

  const top = popData
    .map((row) => `${row.city}: ${row.value.toLocaleString('de-DE')} (year ${row.year})`)
    .join('\n');

  return `Latest population (absolute counts) for NRW cities of interest:\n${top}\nAlways use these numbers for rankings; do not guess.`;
}

// Fetch business registrations/deregistrations per city by year (total rows only)
async function getBusinessTrends() {
  const result = await pool.query(
    `
    SELECT
      g.region_name,
      t.year,
      i.indicator_code,
      i.indicator_name,
      SUM(fd.value) AS value
    FROM fact_demographics fd
    JOIN dim_geography g ON fd.geo_id = g.geo_id
    JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE g.region_type = 'urban_district'
      AND g.region_name = ANY($1)
      AND i.indicator_code IN ('business_registrations', 'business_deregistrations')
      AND (fd.notes ILIKE '%Total%' OR fd.notes IS NULL)
    GROUP BY g.region_name, t.year, i.indicator_code, i.indicator_name
    ORDER BY t.year DESC, g.region_name
    `,
    [CITY_LIST]
  );

  return result.rows.map((row) => ({
    city: row.region_name,
    year: parseInt(row.year, 10),
    indicatorCode: row.indicator_code,
    indicatorName: row.indicator_name,
    value: parseFloat(row.value),
  }));
}

function buildBusinessContext(bizData) {
  if (!bizData || bizData.length === 0) {
    return 'No business registration/deregistration data available.';
  }

  const latestYear = Math.max(...bizData.map((d) => d.year));
  const latest = bizData.filter((d) => d.year === latestYear);

  const summary = latest
    .map(
      (row) =>
        `${row.city} - ${row.indicatorName}: ${row.value.toLocaleString('de-DE')} (${latestYear})`
    )
    .join('\n');

  return `Business activity totals (latest year ${latestYear}, totals only):\n${summary}\nUse these values for quick rankings; do not guess or fabricate numbers.`;
}

function summarizeBusinessTrends(bizData) {
  if (!bizData || bizData.length === 0) {
    return null;
  }

  const years = [...new Set(bizData.map((d) => d.year))].sort((a, b) => b - a);
  const recentYears = years.slice(0, 5);

  let text = `Business registrations/deregistrations (totals, last ${recentYears.length} years):\n\n`;
  recentYears.forEach((year) => {
    const rows = bizData.filter((d) => d.year === year);
    text += `**${year}:**\n`;
    rows.forEach((row) => {
      text += `- ${row.city}: ${row.indicatorName} ${row.value.toLocaleString('de-DE')}\n`;
    });
    text += '\n';
  });

  text += 'For category breakdowns or charts, open the "Trends" tab and pick the business registration/deregistration indicators.';
  return text.trim();
}

// Fetch unemployment trends (counts) for city comparison
async function getUnemploymentTrends() {
  const result = await pool.query(
    `
    SELECT
      g.region_name,
      t.year,
      i.indicator_name,
      SUM(fd.value) AS value
    FROM fact_demographics fd
    JOIN dim_geography g ON fd.geo_id = g.geo_id
    JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE i.indicator_name ILIKE '%Arbeitslosenquote%'
      AND g.region_type = 'urban_district'
      AND g.region_name = ANY($1)
      AND (fd.age_group = 'total' OR fd.age_group IS NULL)
    GROUP BY g.region_name, t.year, i.indicator_name
    ORDER BY t.year DESC, g.region_name
    `,
    [CITY_LIST]
  );

  return result.rows.map((row) => ({
    city: row.region_name,
    year: parseInt(row.year, 10),
    indicatorName: row.indicator_name,
    value: parseFloat(row.value),
  }));
}

function summarizeUnemployment(unempData) {
  if (!unempData || unempData.length === 0) return null;

  const years = [...new Set(unempData.map((d) => d.year))].sort((a, b) => b - a);
  const recentYears = years.slice(0, 5);
  let text = `Unemployment counts (recent ${recentYears.length} years):\n\n`;
  recentYears.forEach((year) => {
    text += `**${year}:**\n`;
    unempData
      .filter((d) => d.year === year)
      .forEach((row) => {
        text += `- ${row.city}: ${Math.round(row.value).toLocaleString('de-DE')}\n`;
      });
    text += '\n';
  });
  text += 'These are counts, not rates. For charts, see the Trends tab.';
  return text.trim();
}

// Utility to extract years from message
function extractYears(text) {
  const years = (text.match(/\b(19|20)\d{2}\b/g) || []).map((y) => parseInt(y, 10));
  return [...new Set(years)].sort((a, b) => a - b);
}

// Utility to extract city names from message based on known list
function extractCities(text) {
  const lower = text.toLowerCase();
  return CITY_LIST.filter((city) => lower.includes(city.toLowerCase()));
}

// Extract a rough indicator search term by removing cities/years/common words
function extractIndicatorTerm(text) {
  const lower = text.toLowerCase();
  const cleaned = lower
    .replace(/\b(19|20)\d{2}\b/g, ' ')
    .replace(/\b(in|for|of|show|what|whats|give|display|trend|trends|time series|series|data|numbers|value|values|count|counts|rate|rates|and|or|to|from|please|me)\b/gi, ' ');
  let result = cleaned;
  CITY_LIST.forEach((city) => {
    result = result.replace(new RegExp(city, 'gi'), ' ');
  });
  return result.replace(/\s+/g, ' ').trim();
}

function buildIndicatorTerms(term, message) {
  const terms = new Set();
  if (term) terms.add(term.toLowerCase());
  const lowerMsg = (message || '').toLowerCase();
  Object.entries(INDICATOR_SYNONYMS).forEach(([, syns]) => {
    syns.forEach((syn) => {
      if (lowerMsg.includes(syn)) terms.add(syn);
    });
  });
  // Include alias keys explicitly
  Object.keys(INDICATOR_ALIAS).forEach((alias) => {
    if (lowerMsg.includes(alias)) terms.add(alias);
  });
  return Array.from(terms);
}

async function findIndicatorByTerm(termOrTerms) {
  const termsArray = Array.isArray(termOrTerms)
    ? termOrTerms.filter((t) => t && t.length >= 3)
    : [termOrTerms].filter((t) => t && t.length >= 3);
  if (termsArray.length === 0) return null;

  // Alias check first
  for (const t of termsArray) {
    const aliasCode = INDICATOR_ALIAS[t.toLowerCase()];
    if (aliasCode) {
      const aliasRow = await pool.query(
        `SELECT indicator_code, indicator_name, indicator_category, indicator_subcategory, unit_of_measure
         FROM dim_indicator
         WHERE indicator_code = $1 AND is_active = true
         LIMIT 1`,
        [aliasCode]
      );
      if (aliasRow.rows.length > 0) {
        return aliasRow.rows[0];
      }
    }
  }

  const patterns = termsArray.map((t) => `%${t.toLowerCase()}%`);
  const codes = termsArray.map((t) => t.toLowerCase());

  const result = await pool.query(
    `
    SELECT indicator_code, indicator_name, indicator_category, indicator_subcategory, unit_of_measure
    FROM dim_indicator
    WHERE is_active = true
      AND (
        LOWER(indicator_name) ILIKE ANY($1)
        OR LOWER(indicator_code) = ANY($2)
        OR LOWER(indicator_subcategory) ILIKE ANY($1)
      )
    LIMIT 25;
  `,
    [patterns, codes]
  );

  if (result.rows.length === 0) return null;

  const isRateQuery = termsArray.some((t) => RATE_KEYWORDS.some((kw) => t.toLowerCase().includes(kw)));
  const scoreCandidate = (row) => {
    let score = 0;
    termsArray.forEach((t) => {
      if (row.indicator_code.toLowerCase() === t.toLowerCase()) score += 5;
      if (row.indicator_name.toLowerCase().includes(t.toLowerCase())) score += 3;
      if ((row.indicator_subcategory || '').toLowerCase().includes(t.toLowerCase())) score += 2;
    });
    const isRateIndicator = RATE_KEYWORDS.some((kw) => row.indicator_name.toLowerCase().includes(kw));
    if (isRateQuery && isRateIndicator) score += 2;
    if (!isRateQuery && isRateIndicator) score -= 1;
    // Prefer allowed indicators
    if (ALLOWED_INDICATORS.has(row.indicator_code)) score += 3;
    return score;
  };

  const sorted = result.rows
    .map((row) => ({ row, score: scoreCandidate(row) }))
    .sort((a, b) => b.score - a.score);

  return sorted[0].row;
}

async function getIndicatorTimeSeries(indicatorCode, cities, startYear, endYear) {
  const cityList = cities && cities.length ? cities : CITY_LIST;
  const start = startYear || 2000;
  const end = endYear || 2024;
  const result = await pool.query(
    `
    SELECT
      g.region_name,
      t.year,
      i.indicator_name,
      i.unit_of_measure,
      SUM(fd.value) AS value
    FROM fact_demographics fd
    JOIN dim_geography g ON fd.geo_id = g.geo_id
    JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE g.region_type = 'urban_district'
      AND g.region_name = ANY($1)
      AND i.indicator_code = $2
      AND t.year BETWEEN $3 AND $4
      AND (fd.age_group = 'total' OR fd.age_group IS NULL)
    GROUP BY g.region_name, t.year, i.indicator_name, i.unit_of_measure
    ORDER BY t.year DESC, g.region_name
    `,
    [cityList, indicatorCode, start, end]
  );
  return result.rows.map((row) => ({
    city: row.region_name,
    year: parseInt(row.year, 10),
    indicatorName: row.indicator_name,
    unit: row.unit_of_measure,
    value: parseFloat(row.value),
  }));
}

function summarizeIndicatorSeries(series, indicatorName, unit) {
  if (!series || series.length === 0) return null;
  const years = [...new Set(series.map((r) => r.year))].sort((a, b) => b - a);
  const recentYears = years.slice(0, 5);
  let text = `${indicatorName || 'Indicator'} (recent ${recentYears.length} years`;
  text += unit ? `, unit: ${unit}` : '';
  text += '):\n\n';
  recentYears.forEach((year) => {
    text += `**${year}:**\n`;
    series
      .filter((r) => r.year === year)
      .forEach((r) => {
        text += `- ${r.city}: ${Math.round(r.value).toLocaleString('de-DE')}\n`;
      });
    text += '\n';
  });
  return text.trim();
}

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
        AND (fd.notes LIKE '%Total%' OR fd.notes LIKE '%total%' OR fd.notes IS NULL)
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

// Get indicator metadata including year ranges
app.get('/api/indicator-metadata', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        i.indicator_code,
        i.indicator_name,
        i.indicator_category,
        MIN(t.year) as min_year,
        MAX(t.year) as max_year,
        COUNT(DISTINCT t.year) as year_count
      FROM fact_demographics f
      JOIN dim_geography g ON f.geo_id = g.geo_id
      JOIN dim_indicator i ON f.indicator_id = i.indicator_id
      JOIN dim_time t ON f.time_id = t.time_id
      WHERE g.region_type = 'urban_district'
        AND g.region_name IN ('Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr')
        AND i.is_active = true
        AND (f.age_group = 'total' OR f.age_group IS NULL)
      GROUP BY i.indicator_code, i.indicator_name, i.indicator_category
      ORDER BY i.indicator_category, i.indicator_name
    `);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching indicator metadata:', err);
    res.status(500).json({ error: 'Failed to fetch indicator metadata' });
  }
});

// Get available years for a specific indicator
app.get('/api/indicator-years/:indicatorCode', async (req, res) => {
  try {
    const { indicatorCode } = req.params;
    const result = await pool.query(`
      SELECT DISTINCT t.year
      FROM fact_demographics f
      JOIN dim_geography g ON f.geo_id = g.geo_id
      JOIN dim_indicator i ON f.indicator_id = i.indicator_id
      JOIN dim_time t ON f.time_id = t.time_id
      WHERE g.region_type = 'urban_district'
        AND g.region_name IN ('Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr')
        AND i.indicator_code = $1
        AND i.is_active = true
        AND (f.age_group = 'total' OR f.age_group IS NULL)
      ORDER BY t.year DESC
    `, [indicatorCode]);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching indicator years:', err);
    res.status(500).json({ error: 'Failed to fetch indicator years' });
  }
});

// Get category breakdown for indicators with subcategories (e.g., business registrations/deregistrations)
app.get('/api/timeseries/:indicatorCode/categories', async (req, res) => {
  try {
    const { indicatorCode } = req.params;
    const { startYear, endYear, city } = req.query;

    const selectedCity = city || 'Duisburg';
    const start = startYear || 2010;
    const end = endYear || 2024;

    // Query to get category breakdowns (excluding "Total" records)
    const result = await pool.query(`
      SELECT
        g.region_name,
        i.indicator_name,
        i.indicator_code,
        i.unit_of_measure,
        t.year,
        fd.notes as category,
        fd.value
      FROM fact_demographics fd
      JOIN dim_geography g ON fd.geo_id = g.geo_id
      JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
      JOIN dim_time t ON fd.time_id = t.time_id
      WHERE g.region_type = 'urban_district'
        AND g.region_name = $1
        AND i.indicator_code = $2
        AND t.year BETWEEN $3 AND $4
        AND fd.notes IS NOT NULL
        AND fd.notes NOT LIKE '%Total%'
        AND fd.notes NOT LIKE '%total%'
      ORDER BY t.year, fd.notes
    `, [selectedCity, indicatorCode, start, end]);

    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching category breakdown:', err);
    res.status(500).json({ error: 'Failed to fetch category breakdown' });
  }
});

// AI Chatbot endpoint
app.post('/api/chat', async (req, res) => {
  try {
    const { message, history, uiContext, chartContext } = req.body;

    if (!message) {
      return res.status(400).json({ error: 'Message is required' });
    }

    const systemContext = `You are an AI assistant for the Duisburg Economic Dashboard. Help users understand economic data for Duisburg and neighboring NRW cities. Be concise, cite specific figures only when provided in context, and never invent numbers. If data is not available, say so briefly and point to the relevant dashboard tab (Demographics, Labor Market, Trends). Always prefer the provided data context over assumptions. If asked for a table and you have structured data, return a concise Markdown table.`;

    const recentHistory = Array.isArray(history)
      ? history.slice(-8).map((entry) => ({
          role: entry.role === 'assistant' ? 'assistant' : 'user',
          content: entry.content,
        }))
      : [];

    const contextMessages = [];
    if (uiContext && typeof uiContext === 'object') {
      const {
        activeTab,
        selectedYear,
        selectedIndicator,
        selectedIndicatorName,
        chartType,
        viewMode,
        selectedCity,
      } = uiContext;
      const parts = [];
      if (activeTab) parts.push(`Active tab: ${activeTab}`);
      if (selectedYear) parts.push(`Year: ${selectedYear}`);
      if (selectedIndicator) {
        parts.push(
          `Indicator: ${selectedIndicator} ${selectedIndicatorName ? `(${selectedIndicatorName})` : ''
          }`
        );
      }
      if (chartType) parts.push(`Chart type: ${chartType}`);
      if (viewMode) parts.push(`View mode: ${viewMode}`);
      if (selectedCity) parts.push(`Selected city: ${selectedCity}`);
      if (parts.length > 0) {
        contextMessages.push({
          role: 'system',
          content: `Current dashboard context: ${parts.join(' | ')}.`,
        });
      }
    }
    if (chartContext && chartContext.rows && chartContext.rows.length > 0) {
      const { indicatorCode, indicatorName, unit, mode, rows } = chartContext;
      const limitedRows = rows.slice(0, 40);
      const rowSummary = limitedRows
        .map((r) => {
          const label = r.city || r.category || r.series || 'value';
          return `${r.year}: ${label} = ${r.value}`;
        })
        .join('; ');
      const chartMsg = `Current chart data (${mode || 'unknown'}): indicator ${indicatorCode || ''} ${indicatorName ? `(${indicatorName})` : ''} ${unit ? `unit=${unit}` : ''}. Recent points: ${rowSummary}`;
      contextMessages.push({ role: 'system', content: chartMsg });
    }

    const lowerMessage = message.toLowerCase();
    const isPopulationQuery =
      lowerMessage.includes('population') ||
      lowerMessage.includes('einwohner') ||
      lowerMessage.includes('bevölkerung') ||
      lowerMessage.includes('bevölkerung') ||
      lowerMessage.includes('rank') ||
      lowerMessage.includes('highest');
    const isUnemploymentQuery =
      lowerMessage.includes('unemployment') ||
      lowerMessage.includes('arbeitslos') ||
      lowerMessage.includes('arbeitslosenquote');
    const isBusinessQuery =
      lowerMessage.includes('business') ||
      lowerMessage.includes('gewerbe') ||
      lowerMessage.includes('registration') ||
      lowerMessage.includes('registrations') ||
      lowerMessage.includes('deregistration') ||
      lowerMessage.includes('deregistrations') ||
      lowerMessage.includes('anmeldung') ||
      lowerMessage.includes('abmeldung');

    // Prepare data context for LLM grounding
    let populationContext = null;
    let businessContext = null;
    let businessSummary = null;
    let tablePayload = null;
    let unemploymentSummary = null;
    let indicatorSummary = null;
    if (isPopulationQuery) {
      try {
        const popData = await getLatestPopulationData();
        populationContext = buildPopulationContext(popData);
      } catch (popErr) {
        console.error('Population data fetch error:', popErr);
      }
    }
    if (isBusinessQuery) {
      try {
        const bizData = await getBusinessTrends();
        businessContext = buildBusinessContext(bizData);
        businessSummary = summarizeBusinessTrends(bizData);
        if (bizData && bizData.length > 0) {
          const columns = ['Year', 'City', 'Indicator', 'Value'];
          const rows = bizData
            .sort((a, b) => b.year - a.year || a.city.localeCompare(b.city))
            .slice(0, 50) // keep it concise
            .map((row) => [
              row.year,
              row.city,
              row.indicatorName,
              row.value,
            ]);
          tablePayload = { columns, rows, title: 'Business registrations/deregistrations (totals)' };
        }
      } catch (bizErr) {
        console.error('Business data fetch error:', bizErr);
      }
    }
    if (isUnemploymentQuery) {
      try {
        const unempData = await getUnemploymentTrends();
        unemploymentSummary = summarizeUnemployment(unempData);
        if (unempData && unempData.length > 0) {
          const columns = ['Year', 'City', 'Indicator', 'Value'];
          const rows = unempData
            .sort((a, b) => b.year - a.year || a.city.localeCompare(b.city))
            .slice(0, 40)
            .map((row) => [row.year, row.city, row.indicatorName, Math.round(row.value)]);
          tablePayload = tablePayload || { columns, rows, title: 'Unemployment (counts)' };
        }
      } catch (unempErr) {
        console.error('Unemployment data fetch error:', unempErr);
      }
    }

    // General indicator time-series handler (deterministic)
    const tryIndicatorSeries = async () => {
      const years = extractYears(message);
      const citiesInMsg = extractCities(message);
      const indicatorTerm = extractIndicatorTerm(message);
      const indicatorTerms = buildIndicatorTerms(indicatorTerm, message);

      // Require an indicator term
      if (indicatorTerms.length === 0) return null;

      const indicator = await findIndicatorByTerm(indicatorTerms);
      if (!indicator) return null;

      const startYear = years[0] || 2000;
      const endYear = years[years.length - 1] || 2024;
      const series = await getIndicatorTimeSeries(indicator.indicator_code, citiesInMsg, startYear, endYear);
      if (!series || series.length === 0) return null;

      const summary = summarizeIndicatorSeries(series, indicator.indicator_name, indicator.unit_of_measure);

      const columns = ['Year', 'City', 'Indicator', 'Value'];
      const rows = series
        .sort((a, b) => b.year - a.year || a.city.localeCompare(b.city))
        .slice(0, 60)
        .map((row) => [row.year, row.city, indicator.indicator_name, Math.round(row.value)]);

      return { summary, table: { columns, rows, title: `${indicator.indicator_name} (${indicator.indicator_code})` } };
    };

    let indicatorResult = null;
    if (!isBusinessQuery && !isUnemploymentQuery && !isPopulationQuery) {
      try {
        indicatorResult = await tryIndicatorSeries();
      } catch (indErr) {
        console.error('Indicator time series error:', indErr);
      }
    }

    const generateRuleBasedResponse = async () => {
      let response = '';

      if (lowerMessage.includes('unemployment') || lowerMessage.includes('arbeitslos')) {
        const unemploymentData = await pool.query(`
          SELECT
            g.region_name,
            t.year,
            fd.value
          FROM fact_demographics fd
          JOIN dim_geography g ON fd.geo_id = g.geo_id
          JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
          JOIN dim_time t ON fd.time_id = t.time_id
          WHERE i.indicator_name LIKE '%Arbeitslosenquote%'
            AND g.region_type = 'urban_district'
            AND t.year >= 2020
            AND (fd.age_group = 'total' OR fd.age_group IS NULL)
          ORDER BY t.year DESC, g.region_name
          LIMIT 25
        `);

        if (unemploymentData.rows.length > 0) {
          const duisburgLatest = unemploymentData.rows.find(r => r.region_name === 'Duisburg');
          const latestYear = duisburgLatest?.year || unemploymentData.rows[0].year;

          response = `**${latestYear} Unemployed Persons (Annual Average):**\n\n`;

          const yearData = unemploymentData.rows.filter(r => r.year === latestYear);
          yearData.forEach(row => {
            const count = parseInt(row.value);
            response += `- ${row.region_name}: ${count.toLocaleString('de-DE')} persons\n`;
          });

          response += `\n💡 These are counts of unemployed individuals (not percentages).\n`;
          response += `📊 Try the "Trends" tab and select "Arbeitslose und Arbeitslosenquoten" to see the time series.`;
        }
      } else if (lowerMessage.includes('population') || lowerMessage.includes('einwohner') || lowerMessage.includes('bevölkerung')) {
        const popData = await pool.query(`
          SELECT
            g.region_name,
            t.year,
            fd.value
          FROM fact_demographics fd
          JOIN dim_geography g ON fd.geo_id = g.geo_id
          JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
          JOIN dim_time t ON fd.time_id = t.time_id
          WHERE i.indicator_name LIKE '%Bevölkerung%'
            AND g.region_type = 'urban_district'
            AND t.year >= 2020
            AND (fd.age_group = 'total' OR fd.age_group IS NULL)
          ORDER BY t.year DESC, g.region_name
          LIMIT 25
        `);

        if (popData.rows.length > 0) {
          const latestYear = popData.rows[0].year;
          response = `**${latestYear} Population Data:**\n\n`;

          const yearData = popData.rows.filter(r => r.year === latestYear);
          yearData.forEach(row => {
            const population = parseInt(row.value);
            response += `- ${row.region_name}: ${population.toLocaleString('de-DE')} residents\n`;
          });

          response += `\n📊 Check the "Demographics" tab for detailed breakdowns.`;
        }
      } else if (isBusinessQuery) {
        const bizData = await getBusinessTrends();
        if (bizData.length > 0) {
          const latestYear = Math.max(...bizData.map((d) => d.year));
          const latest = bizData.filter((d) => d.year === latestYear);

          response = `**Business activity totals — ${latestYear}:**\n\n`;
          latest.forEach((row) => {
            response += `- ${row.city}: ${row.indicatorName} ${row.value.toLocaleString('de-DE')}\n`;
          });
          response += `\n📊 For trends over time, open the "Trends" tab and select the business registrations/deregistrations indicators.`;
        } else {
          response = 'I could not find business registration/deregistration data right now.';
        }
      } else if (lowerMessage.includes('gdp') || lowerMessage.includes('bip') || lowerMessage.includes('wirtschaft')) {
        response = `I can help you explore GDP and economic data:\n\n`;
        response += `- GDP (Bruttoinlandsprodukt) available 1991-2023\n`;
        response += `- Business registrations/deregistrations\n`;
        response += `- Employment and labor market data\n`;
        response += `- Public finance indicators\n\n`;
        response += `To view GDP trends: go to "Trends" → select "Bruttoinlandsprodukt" → pick a chart type (Line, Area, Bar, Table).\n`;
        response += `Want a specific comparison or year range?`;
      } else if (lowerMessage.includes('compare') || lowerMessage.includes('vergleich')) {
        response = `I can help you compare cities:\n\n`;
        response += `- "Demographics" tab: population and age distribution by city for a year\n`;
        response += `- "Labor Market" tab: unemployment and employment figures\n`;
        response += `- "Trends" tab: any indicator over time for Duisburg, Düsseldorf, Essen, Oberhausen, Mülheim\n\n`;
        response += `Tell me what to compare, e.g., unemployment Duisburg vs Essen or business registrations across all cities.`;
      } else if (lowerMessage.includes('trend') || lowerMessage.includes('develop') || lowerMessage.includes('entwicklung')) {
        response = `Trend analysis highlights:\n\n`;
        response += `- Long series: GDP (1991-2023), population (1975-2024), employment\n`;
        response += `- Recent: health/care (2017-2023), wage stats (2020-2024)\n\n`;
        response += `Use "Trends" → choose indicator (year ranges shown) → choose City Comparison or Category Breakdown → pick chart type.\n`;
        response += `Which indicator should we explore?`;
      } else if (lowerMessage.includes('help') || lowerMessage.includes('hilfe') || lowerMessage.includes('what can')) {
        response = `👋 I can help with:\n`;
        response += `- Data queries: unemployment, population, GDP, business activity\n`;
        response += `- City comparisons across metrics\n`;
        response += `- Explaining trends and historical context\n\n`;
        response += `Navigation tips: point you to the right tab, suggest visualizations, explain chart choices.\n`;
        response += `Try: "Unemployment trend in Duisburg", "Compare GDP across all cities", or "Show business registration trends".`;
      } else {
        response = `I understand you're asking about "${message}".\n\n`;
        response += `I can help with Demographics, Labor Market, Business Activity, GDP, and Public Finance. `;
        response += `Ask for a specific indicator or city comparison, e.g., "What's the unemployment rate in Duisburg?" or "Compare business registrations across cities".`;
      }

      return response;
    };

    let responseText = null;

    // For business queries, prefer deterministic DB-backed summary to avoid hallucinated numbers
    if (isBusinessQuery && businessSummary) {
      responseText = businessSummary;
    }
    if (isUnemploymentQuery && unemploymentSummary) {
      responseText = unemploymentSummary;
    }
    if (indicatorResult?.summary) {
      responseText = indicatorResult.summary;
      if (indicatorResult.table) {
        tablePayload = tablePayload || indicatorResult.table;
      }
    }

    if (!responseText && openaiClient) {
      try {
        const llmMessages = [
          { role: 'system', content: systemContext },
          ...(populationContext ? [{ role: 'system', content: populationContext }] : []),
          ...(businessContext ? [{ role: 'system', content: businessContext }] : []),
          ...contextMessages,
          ...recentHistory,
          { role: 'user', content: message },
        ];

        const completion = await openaiClient.chat.completions.create({
          model: OPENAI_MODEL,
          messages: llmMessages,
          temperature: 0.4,
          max_tokens: 500,
        });

        responseText = completion.choices?.[0]?.message?.content?.trim();
      } catch (llmError) {
        console.error('LLM chat error:', llmError);
      }
    }

    // Fallback to rule-based responses if LLM is unavailable or fails
    if (!responseText) {
      responseText = await generateRuleBasedResponse();
    }

    res.json({ response: responseText, table: tablePayload });
  } catch (err) {
    console.error('Chat error:', err);
    res.status(500).json({
      error: 'Failed to process chat message',
      response: 'I apologize, but I encountered an error processing your request. Please try again or rephrase your question.'
    });
  }
});

app.listen(PORT, HOST, () => {
  console.log(`\n🚀 Duisburg Dashboard API running on http://${HOST}:${PORT}`);
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
  console.log(`  GET /api/timeseries/:indicatorCode/categories - Category breakdown`);
  console.log(`  GET /api/indicators - Available indicators`);
  console.log(`  GET /api/years - Available years`);
  console.log(`  POST /api/chat - AI chatbot endpoint\n`);
});
