import { useState, useEffect } from 'react';
import apiService from './services/api';
import BarChart from './components/BarChart';
import LineChart from './components/LineChart';
import './App.css';

function App() {
  const [duisburgInfo, setDuisburgInfo] = useState(null);
  const [cities, setCities] = useState([]);
  const [selectedYear, setSelectedYear] = useState(2023);
  const [availableYears, setAvailableYears] = useState([]);
  const [indicators, setIndicators] = useState([]);
  const [selectedIndicator, setSelectedIndicator] = useState(null);
  const [demographicsData, setDemographicsData] = useState([]);
  const [laborMarketData, setLaborMarketData] = useState([]);
  const [timeSeriesData, setTimeSeriesData] = useState([]);
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
      loadTimeSeriesData(selectedIndicator);
    }
  }, [selectedIndicator]);

  const loadInitialData = async () => {
    try {
      setLoading(true);
      const [duisburgRes, citiesRes, yearsRes, indicatorsRes] = await Promise.all([
        apiService.getDuisburg(),
        apiService.getCities(),
        apiService.getYears(),
        apiService.getIndicators(),
      ]);

      setDuisburgInfo(duisburgRes.data);
      setCities(citiesRes.data);
      setAvailableYears(yearsRes.data);
      setIndicators(indicatorsRes.data);

      // Set default indicator for labor market (unemployment rate if available)
      const unemploymentIndicator = indicatorsRes.data.find((ind) =>
        ind.indicator_name.toLowerCase().includes('arbeitslosenquote')
      );
      if (unemploymentIndicator) {
        setSelectedIndicator(unemploymentIndicator.indicator_code);
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
            {availableYears.map((y) => (
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
              onChange={(e) => setSelectedIndicator(e.target.value)}
            >
              {indicators.map((ind) => (
                <option key={ind.indicator_code} value={ind.indicator_code}>
                  {ind.indicator_name}
                </option>
              ))}
            </select>
          </div>
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
                      yLabel={unit || 'Value'}
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
                      yLabel={unit || 'Value'}
                      highlightCity="Duisburg"
                    />
                  </div>
                );
              }
            )}
          </div>
        </div>
      )}

      {activeTab === 'trends' && timeSeriesData.length > 0 && (
        <div className="charts-section">
          <h2>Historical Trends</h2>
          <div className="chart-container">
            <LineChart
              data={transformDataForLineChart(timeSeriesData)}
              title={
                timeSeriesData[0]?.indicator_name || 'Time Series'
              }
              xLabel="Year"
              yLabel={timeSeriesData[0]?.unit_of_measure || 'Value'}
              highlightCity="Duisburg"
            />
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

      {activeTab === 'trends' && timeSeriesData.length === 0 && (
        <div className="no-data">
          <p>No time series data available for the selected indicator</p>
        </div>
      )}
    </div>
  );
}

export default App;
