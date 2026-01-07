import { useState, useEffect } from 'react';
import apiService from './services/api';
import BarChart from './components/BarChart';
import LineChart from './components/LineChart';
import AreaChart from './components/AreaChart';
import DataTable from './components/DataTable';
import Chatbot from './components/Chatbot';
import './App.css';

function App() {
  const [duisburgInfo, setDuisburgInfo] = useState(null);
  const [cities, setCities] = useState([]);
  const [selectedYear, setSelectedYear] = useState(2023);
  const [availableYears, setAvailableYears] = useState([]);
  const [indicators, setIndicators] = useState([]);
  const [indicatorMetadata, setIndicatorMetadata] = useState({});
  const [selectedIndicator, setSelectedIndicator] = useState(null);
  const [demographicsData, setDemographicsData] = useState([]);
  const [laborMarketData, setLaborMarketData] = useState([]);
  const [timeSeriesData, setTimeSeriesData] = useState([]);
  const [categoryData, setCategoryData] = useState([]);
  const [viewMode, setViewMode] = useState('cities'); // 'cities' or 'categories'
  const [selectedCity, setSelectedCity] = useState('Duisburg');
  const [chartType, setChartType] = useState('line'); // 'line', 'area', 'bar', 'table'
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState('overview');

  useEffect(() => {
    loadInitialData();
  }, []);

  useEffect(() => {
    if (selectedYear) {
      loadYearData(selectedYear);
    }
  }, [selectedYear]);

  useEffect(() => {
    if (selectedIndicator) {
      if (viewMode === 'cities') {
        loadTimeSeriesData(selectedIndicator);
      } else if (viewMode === 'categories' && hasCategories(selectedIndicator)) {
        loadCategoryData(selectedIndicator, selectedCity);
      }
    }
  }, [selectedIndicator, viewMode, selectedCity]);

  const loadInitialData = async () => {
    try {
      setLoading(true);
      const [duisburgRes, citiesRes, yearsRes, indicatorsRes, metadataRes] = await Promise.all([
        apiService.getDuisburg(),
        apiService.getCities(),
        apiService.getYears(),
        apiService.getIndicators(),
        apiService.getIndicatorMetadata(),
      ]);

      setDuisburgInfo(duisburgRes.data);
      setCities(citiesRes.data);
      setAvailableYears(yearsRes.data);
      setIndicators(indicatorsRes.data);

      // Create a map of indicator_code -> metadata (min_year, max_year, year_count)
      const metadataMap = {};
      metadataRes.data.forEach((item) => {
        metadataMap[item.indicator_code] = {
          min_year: parseInt(item.min_year),
          max_year: parseInt(item.max_year),
          year_count: parseInt(item.year_count),
        };
      });
      setIndicatorMetadata(metadataMap);

      // Set default indicator for labor market (unemployment rate if available)
      const unemploymentIndicator = indicatorsRes.data.find((ind) =>
        ind.indicator_name.toLowerCase().includes('arbeitslosenquote')
      );
      if (unemploymentIndicator) {
        setSelectedIndicator(unemploymentIndicator.indicator_code);
        // Auto-select a valid year for this indicator
        const metadata = metadataMap[unemploymentIndicator.indicator_code];
        if (metadata && selectedYear < metadata.min_year || selectedYear > metadata.max_year) {
          setSelectedYear(metadata.max_year);
        }
      }

      setLoading(false);
    } catch (err) {
      console.error('Error loading initial data:', err);
      setError('Failed to load dashboard data. Make sure the backend is running on port 3001.');
      setLoading(false);
    }
  };

  const loadYearData = async (year) => {
    try {
      const [demoRes, laborRes] = await Promise.all([
        apiService.getDemographics(year),
        apiService.getLaborMarket(year),
      ]);

      setDemographicsData(demoRes.data);
      setLaborMarketData(laborRes.data);
    } catch (err) {
      console.error('Error loading year data:', err);
    }
  };

  const loadTimeSeriesData = async (indicatorCode) => {
    try {
      const res = await apiService.getTimeSeries(indicatorCode, {
        startYear: 2010,
        endYear: 2024,
      });
      setTimeSeriesData(res.data);
    } catch (err) {
      console.error('Error loading time series data:', err);
    }
  };

  const loadCategoryData = async (indicatorCode, city) => {
    try {
      const res = await apiService.getCategoryBreakdown(indicatorCode, {
        city: city,
        startYear: 2010,
        endYear: 2024,
      });
      setCategoryData(res.data);
    } catch (err) {
      console.error('Error loading category data:', err);
    }
  };

  // Check if indicator has category breakdowns
  const hasCategories = (indicatorCode) => {
    return indicatorCode === 'business_deregistrations' ||
           indicatorCode === 'business_registrations';
  };

  const transformDataForBarChart = (data, indicatorName) => {
    const filtered = data.filter((d) => d.indicator_name === indicatorName);
    return filtered.map((d) => ({
      city: d.region_name,
      value: parseFloat(d.value),
    }));
  };

  const transformDataForLineChart = (data) => {
    return data.map((d) => ({
      city: d.region_name,
      year: parseInt(d.year),
      value: parseFloat(d.value),
    }));
  };

  const transformCategoryDataForLineChart = (data) => {
    return data.map((d) => ({
      city: d.category, // Use category as "city" for color grouping
      year: parseInt(d.year),
      value: parseFloat(d.value),
    }));
  };

  const formatUnit = (unit) => {
    if (!unit) return 'Value';
    return unit.replace(/_/g, ' ');
  };

  const selectedIndicatorName = indicators.find(
    (ind) => ind.indicator_code === selectedIndicator
  )?.indicator_name || null;

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner"></div>
        <p>Loading Duisburg Economic Dashboard...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="error">
        <h2>Error</h2>
        <p>{error}</p>
        <p>Please ensure the backend server is running on port 3001</p>
      </div>
    );
  }

  // Build chart context for chatbot
  let chartContext = null;
  if (activeTab === 'trends') {
    if (viewMode === 'cities' && timeSeriesData.length > 0) {
      const rows = transformDataForLineChart(timeSeriesData);
      const years = [...new Set(rows.map((r) => r.year))].sort((a, b) => a - b);
      const recentYears = years.slice(-6);
      const filteredRows = rows.filter((r) => recentYears.includes(r.year));
      chartContext = {
        type: 'trends',
        mode: 'cities',
        indicatorCode: selectedIndicator,
        indicatorName: selectedIndicatorName,
        unit: timeSeriesData[0]?.unit_of_measure || null,
        rows: filteredRows,
      };
    } else if (viewMode === 'categories' && categoryData.length > 0) {
      const rows = transformCategoryDataForLineChart(categoryData);
      const years = [...new Set(rows.map((r) => r.year))].sort((a, b) => a - b);
      const recentYears = years.slice(-6);
      const filteredRows = rows.filter((r) => recentYears.includes(r.year));
      chartContext = {
        type: 'trends',
        mode: 'categories',
        indicatorCode: selectedIndicator,
        indicatorName: selectedIndicatorName,
        unit: categoryData[0]?.unit_of_measure || null,
        rows: filteredRows,
        city: selectedCity,
      };
    }
  }

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>Duisburg Economic Dashboard</h1>
        <p className="subtitle">Regional Economic Indicators - NRW Region</p>
      </header>

      <div className="tabs">
        <button
          className={activeTab === 'overview' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('overview')}
        >
          Overview
        </button>
        <button
          className={activeTab === 'demographics' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('demographics')}
        >
          Demographics
        </button>
        <button
          className={activeTab === 'labor' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('labor')}
        >
          Labor Market
        </button>
        <button
          className={activeTab === 'trends' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('trends')}
        >
          Trends
        </button>
      </div>

      <div className="controls">
        <div className="control-group">
          <label htmlFor="year-select">Year:</label>
          <select
            id="year-select"
            value={selectedYear}
            onChange={(e) => setSelectedYear(parseInt(e.target.value))}
          >
            {availableYears
              .filter((y) => {
                // If on Trends tab and indicator is selected, filter by indicator's year range
                if (activeTab === 'trends' && selectedIndicator && indicatorMetadata[selectedIndicator]) {
                  const meta = indicatorMetadata[selectedIndicator];
                  return y.year >= meta.min_year && y.year <= meta.max_year;
                }
                return true;
              })
              .map((y) => (
                <option key={y.year} value={y.year}>
                  {y.year}
                </option>
              ))}
          </select>
        </div>

        {activeTab === 'trends' && (
          <div className="control-group">
            <label htmlFor="indicator-select">Indicator:</label>
            <select
              id="indicator-select"
              value={selectedIndicator || ''}
              onChange={(e) => {
                const newIndicator = e.target.value;
                setSelectedIndicator(newIndicator);
                // Auto-select a valid year for the new indicator
                if (newIndicator && indicatorMetadata[newIndicator]) {
                  const meta = indicatorMetadata[newIndicator];
                  if (selectedYear < meta.min_year || selectedYear > meta.max_year) {
                    setSelectedYear(meta.max_year);
                  }
                }
                // Reset to city view when changing indicators
                setViewMode('cities');
              }}
            >
              {indicators.map((ind) => {
                const meta = indicatorMetadata[ind.indicator_code];
                const yearRange = meta ? ` (${meta.min_year}-${meta.max_year})` : '';
                return (
                  <option key={ind.indicator_code} value={ind.indicator_code}>
                    {ind.indicator_name}{yearRange}
                  </option>
                );
              })}
            </select>
          </div>
        )}

        {activeTab === 'trends' && (
          <div className="control-group">
            <label htmlFor="chart-type-select">Chart Type:</label>
            <select
              id="chart-type-select"
              value={chartType}
              onChange={(e) => setChartType(e.target.value)}
            >
              <option value="line">📈 Line Chart</option>
              <option value="area">📊 Area Chart</option>
              <option value="bar">📊 Bar Chart</option>
              <option value="table">📋 Table View</option>
            </select>
          </div>
        )}

        {activeTab === 'trends' && selectedIndicator && hasCategories(selectedIndicator) && (
          <>
            <div className="control-group">
              <label>View:</label>
              <div className="button-group">
                <button
                  className={viewMode === 'cities' ? 'toggle-btn active' : 'toggle-btn'}
                  onClick={() => setViewMode('cities')}
                >
                  City Comparison
                </button>
                <button
                  className={viewMode === 'categories' ? 'toggle-btn active' : 'toggle-btn'}
                  onClick={() => setViewMode('categories')}
                >
                  Category Breakdown
                </button>
              </div>
            </div>

            {viewMode === 'categories' && (
              <div className="control-group">
                <label htmlFor="city-select">City:</label>
                <select
                  id="city-select"
                  value={selectedCity}
                  onChange={(e) => setSelectedCity(e.target.value)}
                >
                  {cities.map((city) => (
                    <option key={city.geo_id} value={city.region_name}>
                      {city.region_name}
                    </option>
                  ))}
                </select>
              </div>
            )}
          </>
        )}
      </div>

      {activeTab === 'overview' && duisburgInfo && (
        <div className="overview-section">
          <div className="info-card">
            <h2>Duisburg at a Glance</h2>
            <div className="info-grid">
              <div className="info-item">
                <span className="label">Region Code:</span>
                <span className="value">{duisburgInfo.region_code}</span>
              </div>
              <div className="info-item">
                <span className="label">Type:</span>
                <span className="value">{duisburgInfo.region_type}</span>
              </div>
              <div className="info-item">
                <span className="label">Ruhr Area:</span>
                <span className="value">{duisburgInfo.ruhr_area ? 'Yes' : 'No'}</span>
              </div>
              <div className="info-item">
                <span className="label">Area:</span>
                <span className="value">
                  {duisburgInfo.area_sqkm
                    ? `${duisburgInfo.area_sqkm} km²`
                    : 'N/A'}
                </span>
              </div>
            </div>
          </div>

          <div className="cities-card">
            <h2>Comparison Cities</h2>
            <div className="cities-list">
              {cities.map((city) => (
                <div key={city.geo_id} className="city-item">
                  <span className="city-name">{city.region_name}</span>
                  <span className="city-badge">
                    {city.ruhr_area ? 'Ruhr' : 'Other'}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {activeTab === 'demographics' && demographicsData.length > 0 && (
      <div className="charts-section">
        <h2>Demographics Comparison ({selectedYear})</h2>
        <div className="charts-grid">
          {[...new Set(demographicsData.map((d) => d.indicator_name))].map(
            (indicatorName) => {
                const chartData = transformDataForBarChart(
                  demographicsData,
                  indicatorName
                );
                if (chartData.length === 0) return null;

                const unit = demographicsData.find(
                  (d) => d.indicator_name === indicatorName
                )?.unit_of_measure;

                return (
                <div key={indicatorName} className="chart-container">
                  <BarChart
                    data={chartData}
                    title={indicatorName}
                    xLabel="City"
                    yLabel={formatUnit(unit)}
                    highlightCity="Duisburg"
                  />
                </div>
              );
            }
            )}
          </div>
        </div>
      )}

      {activeTab === 'labor' && laborMarketData.length > 0 && (
        <div className="charts-section">
          <h2>Labor Market Comparison ({selectedYear})</h2>
          <div className="charts-grid">
            {[...new Set(laborMarketData.map((d) => d.indicator_name))].map(
              (indicatorName) => {
                const chartData = transformDataForBarChart(
                  laborMarketData,
                  indicatorName
                );
                if (chartData.length === 0) return null;

                const unit = laborMarketData.find(
                  (d) => d.indicator_name === indicatorName
                )?.unit_of_measure;

                return (
                <div key={indicatorName} className="chart-container">
                  <BarChart
                    data={chartData}
                    title={indicatorName}
                    xLabel="City"
                    yLabel={formatUnit(unit)}
                    highlightCity="Duisburg"
                  />
                </div>
              );
            }
            )}
          </div>
        </div>
      )}

      {activeTab === 'trends' && viewMode === 'cities' && timeSeriesData.length > 0 && (
        <div className="charts-section">
          <h2>Historical Trends - City Comparison</h2>
          <div className="chart-container">
            {chartType === 'line' && (
              <LineChart
                data={transformDataForLineChart(timeSeriesData)}
                title={timeSeriesData[0]?.indicator_name || 'Time Series'}
                xLabel="Year"
                yLabel={formatUnit(timeSeriesData[0]?.unit_of_measure)}
                highlightCity="Duisburg"
              />
            )}
            {chartType === 'area' && (
              <AreaChart
                data={transformDataForLineChart(timeSeriesData)}
                title={timeSeriesData[0]?.indicator_name || 'Time Series'}
                xLabel="Year"
                yLabel={formatUnit(timeSeriesData[0]?.unit_of_measure)}
                highlightCity="Duisburg"
              />
            )}
            {chartType === 'bar' && (
              <BarChart
                data={transformDataForBarChart(
                  timeSeriesData.filter(d => d.year === selectedYear),
                  timeSeriesData[0]?.indicator_name
                )}
                title={`${timeSeriesData[0]?.indicator_name || 'Indicator'} (${selectedYear})`}
                xLabel="City"
                yLabel={formatUnit(timeSeriesData[0]?.unit_of_measure)}
                highlightCity="Duisburg"
              />
            )}
            {chartType === 'table' && (
              <DataTable
                data={transformDataForLineChart(timeSeriesData)}
                title={timeSeriesData[0]?.indicator_name || 'Time Series'}
                highlightCity="Duisburg"
              />
            )}
          </div>
        </div>
      )}

      {activeTab === 'trends' && viewMode === 'categories' && categoryData.length > 0 && (
        <div className="charts-section">
          <h2>Historical Trends - Category Breakdown for {selectedCity}</h2>
          <div className="chart-container">
            {chartType === 'line' && (
              <LineChart
                data={transformCategoryDataForLineChart(categoryData)}
                title={categoryData[0]?.indicator_name || 'Category Breakdown'}
                xLabel="Year"
                yLabel={formatUnit(categoryData[0]?.unit_of_measure)}
                highlightCity={null}
              />
            )}
            {chartType === 'area' && (
              <AreaChart
                data={transformCategoryDataForLineChart(categoryData)}
                title={categoryData[0]?.indicator_name || 'Category Breakdown'}
                xLabel="Year"
                yLabel={formatUnit(categoryData[0]?.unit_of_measure)}
                highlightCity={null}
              />
            )}
            {chartType === 'bar' && (
              <BarChart
                data={categoryData
                  .filter(d => d.year === selectedYear)
                  .map(d => ({ city: d.category, value: parseFloat(d.value) }))
                }
                title={`${categoryData[0]?.indicator_name || 'Category Breakdown'} (${selectedYear})`}
                xLabel="Category"
                yLabel={formatUnit(categoryData[0]?.unit_of_measure)}
                highlightCity={null}
              />
            )}
            {chartType === 'table' && (
              <DataTable
                data={transformCategoryDataForLineChart(categoryData)}
                title={categoryData[0]?.indicator_name || 'Category Breakdown'}
                highlightCity={null}
              />
            )}
          </div>
        </div>
      )}

      {activeTab === 'demographics' && demographicsData.length === 0 && (
        <div className="no-data">
          <p>No demographics data available for {selectedYear}</p>
        </div>
      )}

      {activeTab === 'labor' && laborMarketData.length === 0 && (
        <div className="no-data">
          <p>No labor market data available for {selectedYear}</p>
        </div>
      )}

      {activeTab === 'trends' && timeSeriesData.length === 0 && selectedIndicator && (
        <div className="no-data">
          <p>No time series data available for the selected indicator</p>
          {indicatorMetadata[selectedIndicator] && (
            <p className="data-hint">
              This indicator has data for years {indicatorMetadata[selectedIndicator].min_year} - {indicatorMetadata[selectedIndicator].max_year}
            </p>
          )}
        </div>
      )}

      {/* AI Chatbot */}
      <Chatbot
        uiContext={{
          activeTab,
          selectedYear,
          selectedIndicator,
          selectedIndicatorName:
            indicators.find((ind) => ind.indicator_code === selectedIndicator)
              ?.indicator_name || null,
          chartType,
          viewMode,
          selectedCity,
        }}
      />
    </div>
  );
}

export default App;
