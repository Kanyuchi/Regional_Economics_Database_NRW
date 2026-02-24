import { useEffect, useMemo, useState } from 'react';
import apiService from './services/api';
import BarChart from './components/BarChart';
import LineChart from './components/LineChart';
import AreaChart from './components/AreaChart';
import HorizontalBarChart from './components/HorizontalBarChart';
import ScatterChart from './components/ScatterChart';
import DataTable from './components/DataTable';
import Chatbot from './components/Chatbot';
import { CITY_COLOR_MAP } from './constants/cityColors';
import './App.css';

const OVERVIEW_KPI_CONFIG = [
  { code: 'pop_total', label: 'Population', rankLabel: 'largest population' },
  { code: 'unemployment_persons', label: 'Unemployed Persons', rankLabel: 'highest unemployed persons' },
  { code: 'GDP_MARKET_PRICE', label: 'GDP (Market Prices)', rankLabel: 'highest GDP' },
  { code: 'ba_median_wage', label: 'Median Wage', rankLabel: 'highest median wage' },
];
const PER_CAPITA_BASE = 1000;
const SAVED_VIEWS_STORAGE_KEY = 'duisburg_dashboard_saved_views_v1';
const VALID_TABS = new Set(['overview', 'demographics', 'labor', 'business', 'finance', 'trends']);
const VALID_VIEW_MODES = new Set(['cities', 'categories']);
const VALID_CHART_TYPES = new Set(['line', 'area', 'bar', 'horizontal', 'scatter', 'table']);

function parseInitialUrlState() {
  if (typeof window === 'undefined') return {};
  const params = new URLSearchParams(window.location.search);
  const yearParam = Number.parseInt(params.get('year') || '', 10);
  const tabParam = params.get('tab');
  const modeParam = params.get('mode');
  const chartParam = params.get('chart');
  const citiesParam = params
    .get('cities')
    ?.split(',')
    .map((city) => city.trim())
    .filter(Boolean);

  return {
    selectedYear: Number.isFinite(yearParam) ? yearParam : undefined,
    activeTab: tabParam && VALID_TABS.has(tabParam) ? tabParam : undefined,
    viewMode: modeParam && VALID_VIEW_MODES.has(modeParam) ? modeParam : undefined,
    chartType: chartParam && VALID_CHART_TYPES.has(chartParam) ? chartParam : undefined,
    selectedIndicator: params.get('indicator') || undefined,
    selectedCity: params.get('city') || undefined,
    indicatorSearch: params.get('search') || '',
    selectedCities: citiesParam?.length ? citiesParam : undefined,
    normalizePerCapita:
      params.get('perCapita') === '1' || params.get('perCapita') === 'true',
    scatterMetric: params.get('scatterMetric') || undefined,
  };
}

function MiniSparkline({ data, color = '#2563eb' }) {
  const points = Array.isArray(data) ? data : [];
  if (points.length < 2) {
    return <div className="sparkline-empty">Not enough trend points</div>;
  }

  const width = 160;
  const height = 48;
  const min = Math.min(...points.map((point) => point.value));
  const max = Math.max(...points.map((point) => point.value));
  const range = max - min || 1;
  const stepX = width / (points.length - 1);

  const polylinePoints = points
    .map((point, index) => {
      const x = index * stepX;
      const y = height - ((point.value - min) / range) * (height - 6) - 3;
      return `${x},${y}`;
    })
    .join(' ');

  return (
    <svg className="mini-sparkline" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
      <polyline points={polylinePoints} fill="none" stroke={color} strokeWidth="2.4" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

function App() {
  const [initialUrlState] = useState(() => parseInitialUrlState());
  const [duisburgInfo, setDuisburgInfo] = useState(null);
  const [cities, setCities] = useState([]);
  const [selectedYear, setSelectedYear] = useState(initialUrlState.selectedYear ?? 2023);
  const [availableYears, setAvailableYears] = useState([]);
  const [indicators, setIndicators] = useState([]);
  const [indicatorMetadata, setIndicatorMetadata] = useState({});
  const [selectedIndicator, setSelectedIndicator] = useState(initialUrlState.selectedIndicator ?? null);
  const [demographicsData, setDemographicsData] = useState([]);
  const [laborMarketData, setLaborMarketData] = useState([]);
  const [businessEconomyData, setBusinessEconomyData] = useState([]);
  const [publicFinanceData, setPublicFinanceData] = useState([]);
  const [timeSeriesData, setTimeSeriesData] = useState([]);
  const [categoryData, setCategoryData] = useState([]);
  const [scatterMetricSeries, setScatterMetricSeries] = useState([]);
  const [overviewSeries, setOverviewSeries] = useState({});
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [viewMode, setViewMode] = useState(initialUrlState.viewMode ?? 'cities'); // 'cities' or 'categories'
  const [selectedCity, setSelectedCity] = useState(initialUrlState.selectedCity ?? 'Duisburg');
  const [selectedCities, setSelectedCities] = useState(initialUrlState.selectedCities ?? []);
  const [indicatorSearch, setIndicatorSearch] = useState(initialUrlState.indicatorSearch ?? '');
  const [chartType, setChartType] = useState(initialUrlState.chartType ?? 'line'); // 'line', 'area', 'bar', 'horizontal', 'scatter', 'table'
  const [scatterMetric, setScatterMetric] = useState(initialUrlState.scatterMetric ?? 'pop_total');
  const [normalizePerCapita, setNormalizePerCapita] = useState(initialUrlState.normalizePerCapita ?? false);
  const [savedViews, setSavedViews] = useState([]);
  const [savedViewSelection, setSavedViewSelection] = useState('');
  const [viewName, setViewName] = useState('');
  const [shareMessage, setShareMessage] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState(initialUrlState.activeTab ?? 'overview');

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
  }, [selectedIndicator, viewMode, selectedCity, indicatorMetadata]);

  useEffect(() => {
    if (Object.keys(indicatorMetadata).length > 0) {
      loadOverviewKpiData();
    }
  }, [indicatorMetadata]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      const raw = window.localStorage.getItem(SAVED_VIEWS_STORAGE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        setSavedViews(parsed);
      }
    } catch (err) {
      console.error('Error loading saved views:', err);
    }
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      window.localStorage.setItem(SAVED_VIEWS_STORAGE_KEY, JSON.stringify(savedViews));
    } catch (err) {
      console.error('Error persisting saved views:', err);
    }
  }, [savedViews]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const params = new URLSearchParams();
    params.set('tab', activeTab);
    params.set('year', String(selectedYear));
    params.set('mode', viewMode);
    params.set('chart', chartType);
    if (selectedIndicator) params.set('indicator', selectedIndicator);
    if (selectedCity) params.set('city', selectedCity);
    if (indicatorSearch.trim()) params.set('search', indicatorSearch.trim());
    if (selectedCities.length > 0) params.set('cities', selectedCities.join(','));
    if (normalizePerCapita) params.set('perCapita', '1');
    if (scatterMetric) params.set('scatterMetric', scatterMetric);
    window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`);
  }, [
    activeTab,
    selectedYear,
    viewMode,
    chartType,
    selectedIndicator,
    selectedCity,
    indicatorSearch,
    selectedCities,
    normalizePerCapita,
    scatterMetric,
  ]);

  useEffect(() => {
    if (viewMode === 'categories' && chartType === 'scatter') {
      setChartType('line');
    }
  }, [viewMode, chartType]);

  useEffect(() => {
    if (!scatterMetric && indicators.length > 0) {
      setScatterMetric(indicators[0].indicator_code);
    }
  }, [scatterMetric, indicators]);

  useEffect(() => {
    if (
      activeTab === 'trends' &&
      viewMode === 'cities' &&
      chartType === 'scatter' &&
      scatterMetric &&
      Object.keys(indicatorMetadata).length > 0
    ) {
      loadScatterMetricData(scatterMetric);
    }
  }, [activeTab, viewMode, chartType, scatterMetric, indicatorMetadata]);

  useEffect(() => {
    if (!shareMessage) return undefined;
    const timer = setTimeout(() => setShareMessage(''), 2400);
    return () => clearTimeout(timer);
  }, [shareMessage]);

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
      const cityNames = citiesRes.data.map((city) => city.region_name);
      setSelectedCities((prev) => {
        const validSelection = prev.filter((city) => cityNames.includes(city));
        return validSelection.length > 0 ? validSelection : cityNames;
      });
      setSelectedCity((prev) => (cityNames.includes(prev) ? prev : 'Duisburg'));
      setAvailableYears(yearsRes.data);
      const visibleIndicators = indicatorsRes.data.filter(
        (ind) => ind.indicator_code !== 'unemployment_rate'
      );
      setIndicators(visibleIndicators);

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

      // Set default indicator for labor market (prefer % rate, fallback to unemployed persons)
      const unemploymentIndicator =
        visibleIndicators.find((ind) => ind.indicator_code === 'unemployment_rate_percent') ||
        visibleIndicators.find((ind) => ind.indicator_name.toLowerCase().includes('arbeitslosenquote')) ||
        visibleIndicators.find((ind) => ind.indicator_code === 'unemployment_persons') ||
        visibleIndicators.find((ind) => ind.indicator_code === 'unemployment_rate');
      const isCurrentIndicatorValid =
        selectedIndicator &&
        visibleIndicators.some((ind) => ind.indicator_code === selectedIndicator);
      const indicatorToUse =
        (isCurrentIndicatorValid && selectedIndicator) ||
        unemploymentIndicator?.indicator_code ||
        visibleIndicators[0]?.indicator_code;
      if (indicatorToUse) {
        setSelectedIndicator(indicatorToUse);
        // Auto-select a valid year for the active indicator
        const metadata = metadataMap[indicatorToUse];
        if (metadata && (selectedYear < metadata.min_year || selectedYear > metadata.max_year)) {
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
      const [demoRes, laborRes, businessRes, financeRes] = await Promise.allSettled([
        apiService.getDemographics(year),
        apiService.getLaborMarket(year),
        apiService.getBusinessEconomy(year),
        apiService.getPublicFinance(year),
      ]);

      setDemographicsData(demoRes.status === 'fulfilled' ? demoRes.value.data : []);
      setLaborMarketData(laborRes.status === 'fulfilled' ? laborRes.value.data : []);
      setBusinessEconomyData(businessRes.status === 'fulfilled' ? businessRes.value.data : []);
      setPublicFinanceData(financeRes.status === 'fulfilled' ? financeRes.value.data : []);

      if (demoRes.status === 'rejected') {
        console.error('Error loading demographics data:', demoRes.reason);
      }
      if (laborRes.status === 'rejected') {
        console.error('Error loading labor market data:', laborRes.reason);
      }
      if (businessRes.status === 'rejected') {
        console.error('Error loading business economy data:', businessRes.reason);
      }
      if (financeRes.status === 'rejected') {
        console.error('Error loading public finance data:', financeRes.reason);
      }
    } catch (err) {
      console.error('Error loading year data:', err);
    }
  };

  const loadTimeSeriesData = async (indicatorCode) => {
    try {
      const metadata = indicatorMetadata[indicatorCode];
      const res = await apiService.getTimeSeries(indicatorCode, {
        startYear: metadata?.min_year ?? 2000,
        endYear: metadata?.max_year ?? 2024,
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

  const loadScatterMetricData = async (indicatorCode) => {
    try {
      if (!indicatorCode) return;
      const metadata = indicatorMetadata[indicatorCode];
      if (!metadata) {
        setScatterMetricSeries([]);
        return;
      }
      const res = await apiService.getTimeSeries(indicatorCode, {
        startYear: metadata.min_year,
        endYear: metadata.max_year,
      });
      setScatterMetricSeries(res.data);
    } catch (err) {
      console.error('Error loading scatter metric data:', err);
      setScatterMetricSeries([]);
    }
  };

  const loadOverviewKpiData = async () => {
    try {
      setOverviewLoading(true);

      const responses = await Promise.all(
        OVERVIEW_KPI_CONFIG.map(async (kpi) => {
          const metadata = indicatorMetadata[kpi.code];
          if (!metadata) return [kpi.code, []];

          const res = await apiService.getTimeSeries(kpi.code, {
            startYear: metadata.min_year,
            endYear: metadata.max_year,
          });

          const normalized = res.data
            .map((row) => ({
              city: row.region_name,
              year: Number.parseInt(row.year, 10),
              value: Number.parseFloat(row.value),
            }))
            .filter((row) => row.city && Number.isFinite(row.year) && Number.isFinite(row.value));

          return [kpi.code, normalized];
        })
      );

      setOverviewSeries(Object.fromEntries(responses));
    } catch (err) {
      console.error('Error loading overview KPI data:', err);
    } finally {
      setOverviewLoading(false);
    }
  };

  // Check if indicator has category breakdowns
  const hasCategories = (indicatorCode) => {
    return indicatorCode === 'business_deregistrations' ||
           indicatorCode === 'business_registrations';
  };

  const transformDataForBarChart = (data, indicatorName) => {
    const filtered = indicatorName
      ? data.filter((d) => d.indicator_name === indicatorName)
      : data;
    const grouped = new Map();

    filtered.forEach((d) => {
      const city = d.region_name || d.city;
      const value = Number.parseFloat(d.value);
      if (!city || !Number.isFinite(value)) return;
      grouped.set(city, (grouped.get(city) || 0) + value);
    });

    return Array.from(grouped.entries()).map(([city, value]) => ({ city, value }));
  };

  const transformDataForLineChart = (data) => {
    return data.map((d) => ({
      city: d.region_name,
      year: parseInt(d.year),
      value: parseFloat(d.value),
    })).filter((d) => d.city && Number.isFinite(d.year) && Number.isFinite(d.value));
  };

  const transformCategoryDataForLineChart = (data) => {
    return data.map((d) => ({
      city: d.category, // Use category as "city" for color grouping
      year: parseInt(d.year),
      value: parseFloat(d.value),
    })).filter((d) => d.city && Number.isFinite(d.year) && Number.isFinite(d.value));
  };

  const buildLatestPointMap = (dataPoints, targetYear) => {
    const grouped = new Map();
    dataPoints.forEach((point) => {
      if (!grouped.has(point.city)) grouped.set(point.city, []);
      grouped.get(point.city).push(point);
    });

    const result = new Map();
    grouped.forEach((rows, city) => {
      const sorted = [...rows].sort((a, b) => a.year - b.year);
      const exact = sorted.find((row) => row.year === targetYear);
      if (exact) {
        result.set(city, exact);
        return;
      }
      const fallback = sorted.filter((row) => row.year <= targetYear).at(-1) || sorted.at(-1);
      if (fallback) {
        result.set(city, fallback);
      }
    });

    return result;
  };

  const toggleSelectedCity = (cityName) => {
    setSelectedCities((prev) => {
      if (prev.includes(cityName)) {
        if (prev.length === 1) return prev;
        return prev.filter((c) => c !== cityName);
      }
      return [...prev, cityName];
    });
  };

  const buildCurrentViewState = () => ({
    activeTab,
    selectedYear,
    selectedIndicator,
    viewMode,
    selectedCity,
    selectedCities,
    indicatorSearch,
    chartType,
    normalizePerCapita,
    scatterMetric,
  });

  const applyViewState = (state) => {
    if (!state || typeof state !== 'object') return;
    if (state.activeTab && VALID_TABS.has(state.activeTab)) setActiveTab(state.activeTab);
    if (Number.isFinite(Number.parseInt(state.selectedYear, 10))) {
      setSelectedYear(Number.parseInt(state.selectedYear, 10));
    }
    if (state.selectedIndicator) setSelectedIndicator(state.selectedIndicator);
    if (state.viewMode && VALID_VIEW_MODES.has(state.viewMode)) setViewMode(state.viewMode);
    if (state.selectedCity) setSelectedCity(state.selectedCity);
    if (Array.isArray(state.selectedCities) && state.selectedCities.length > 0) {
      setSelectedCities(state.selectedCities);
    }
    if (typeof state.indicatorSearch === 'string') setIndicatorSearch(state.indicatorSearch);
    if (state.chartType && VALID_CHART_TYPES.has(state.chartType)) setChartType(state.chartType);
    if (typeof state.normalizePerCapita === 'boolean') setNormalizePerCapita(state.normalizePerCapita);
    if (state.scatterMetric) setScatterMetric(state.scatterMetric);
  };

  const handleSaveView = () => {
    const now = new Date();
    const trimmed = viewName.trim();
    const name =
      trimmed ||
      `${activeTab} ${selectedYear} ${now.toLocaleDateString('de-DE')} ${now.toLocaleTimeString('de-DE', {
        hour: '2-digit',
        minute: '2-digit',
      })}`;
    const view = {
      id: String(Date.now()),
      name,
      createdAt: now.toISOString(),
      state: buildCurrentViewState(),
    };
    setSavedViews((prev) => [view, ...prev].slice(0, 20));
    setViewName('');
    setSavedViewSelection(view.id);
    setShareMessage(`Saved view "${name}"`);
  };

  const handleLoadSavedView = () => {
    if (!savedViewSelection) return;
    const selected = savedViews.find((view) => view.id === savedViewSelection);
    if (!selected) return;
    applyViewState(selected.state);
    setShareMessage(`Loaded view "${selected.name}"`);
  };

  const handleDeleteSavedView = () => {
    if (!savedViewSelection) return;
    const selected = savedViews.find((view) => view.id === savedViewSelection);
    setSavedViews((prev) => prev.filter((view) => view.id !== savedViewSelection));
    setSavedViewSelection('');
    if (selected) {
      setShareMessage(`Deleted view "${selected.name}"`);
    }
  };

  const handleCopyLink = async () => {
    try {
      if (typeof window === 'undefined') return;
      if (!navigator?.clipboard?.writeText) {
        setShareMessage('Clipboard not available');
        return;
      }
      await navigator.clipboard.writeText(window.location.href);
      setShareMessage('Share link copied');
    } catch (err) {
      console.error('Error copying share link:', err);
      setShareMessage('Could not copy link');
    }
  };

  const handlePrintPdf = () => {
    if (typeof window !== 'undefined') {
      window.print();
    }
  };

  const formatUnit = (unit) => {
    if (!unit) return 'Value';
    return unit.replace(/_/g, ' ');
  };

  const shouldNormalizeIndicator = (indicatorCode, unit) => {
    if (!normalizePerCapita) return false;
    if (!indicatorCode) return false;
    if (indicatorCode === 'pop_total') return false;

    const normalizedCode = indicatorCode.toLowerCase();
    const normalizedUnit = (unit || '').toLowerCase();
    if (normalizedCode.includes('rate')) return false;
    if (normalizedUnit.includes('%') || normalizedUnit.includes('percent')) return false;
    if (normalizedUnit.includes('rate') || normalizedUnit.includes('quote')) return false;
    if (normalizedUnit.includes('index')) return false;
    return true;
  };

  const normalizeByPopulation = (value, cityName, year) => {
    const numeric = Number.parseFloat(value);
    if (!Number.isFinite(numeric)) return null;
    const populationPoint = getCityMetricPoint('pop_total', cityName, year);
    const population = populationPoint?.value;
    if (!Number.isFinite(population) || population <= 0) return null;
    return (numeric / population) * PER_CAPITA_BASE;
  };

  const chartTitleWithScale = (title, normalized) =>
    normalized ? `${title} (per ${PER_CAPITA_BASE.toLocaleString('de-DE')} residents)` : title;

  const chartYLabel = (unit, normalized) =>
    normalized ? `per ${PER_CAPITA_BASE.toLocaleString('de-DE')} residents` : formatUnit(unit);

  const buildCityComparisonBarData = (rows, indicatorName, indicatorCode, unit, year) => {
    const normalized = shouldNormalizeIndicator(indicatorCode, unit);
    const filtered = rows.filter((row) => row.indicator_name === indicatorName);
    const prepared = filtered
      .map((row) => {
        const rawValue = Number.parseFloat(row.value);
        if (!Number.isFinite(rawValue)) return null;
        if (!normalized) return { ...row, value: rawValue };
        const normalizedValue = normalizeByPopulation(rawValue, row.region_name, year);
        if (!Number.isFinite(normalizedValue)) return null;
        return { ...row, value: normalizedValue };
      })
      .filter(Boolean);

    return {
      normalized,
      data: transformDataForBarChart(prepared),
    };
  };

  const selectedIndicatorName = indicators.find(
    (ind) => ind.indicator_code === selectedIndicator
  )?.indicator_name || null;

  const formatKpiValue = (kpiCode, value) => {
    if (!Number.isFinite(value)) return '—';

    if (kpiCode === 'pop_total') {
      return Math.round(value).toLocaleString('de-DE');
    }
    if (kpiCode === 'unemployment_persons') {
      return Math.round(value).toLocaleString('de-DE');
    }
    if (kpiCode === 'unemployment_rate_percent') {
      return `${value.toFixed(1)} %`;
    }
    if (kpiCode === 'GDP_MARKET_PRICE') {
      return `${value.toLocaleString('de-DE', { maximumFractionDigits: 1 })} Mio €`;
    }
    if (kpiCode === 'ba_median_wage') {
      return `${Math.round(value).toLocaleString('de-DE')} €`;
    }
    return value.toLocaleString('de-DE', { maximumFractionDigits: 1 });
  };

  const cityOrder = useMemo(
    () => new Map(cities.map((city, index) => [city.region_name, index])),
    [cities]
  );

  const sortCities = (cityNames) =>
    [...new Set(cityNames)]
      .filter(Boolean)
      .sort(
        (a, b) =>
          (cityOrder.get(a) ?? Number.MAX_SAFE_INTEGER) -
            (cityOrder.get(b) ?? Number.MAX_SAFE_INTEGER) ||
          a.localeCompare(b)
      );

  const getCityMetricPoint = (kpiCode, cityName, year, exactOnly = false) => {
    const series = overviewSeries[kpiCode] || [];
    const citySeries = series
      .filter((row) => row.city === cityName)
      .sort((a, b) => a.year - b.year);

    if (citySeries.length === 0) return null;

    const exact = citySeries.find((row) => row.year === year);
    if (exact || exactOnly) return exact || null;

    return citySeries.filter((row) => row.year <= year).at(-1) || citySeries.at(0);
  };

  const duisburgOverviewKpis = useMemo(() => {
    return OVERVIEW_KPI_CONFIG.map((kpi) => {
      const series = (overviewSeries[kpi.code] || [])
        .filter((row) => row.city === 'Duisburg')
        .sort((a, b) => a.year - b.year);

      const current = getCityMetricPoint(kpi.code, 'Duisburg', selectedYear);
      const previous = current
        ? series.filter((row) => row.year < current.year).at(-1) || null
        : null;

      const change = current && previous ? current.value - previous.value : null;
      const changePct =
        change !== null && previous && previous.value !== 0
          ? (change / previous.value) * 100
          : null;

      return {
        ...kpi,
        series,
        sparkline: series.slice(-10),
        current,
        previous,
        change,
        changePct,
      };
    });
  }, [overviewSeries, selectedYear]);

  const overviewRankings = useMemo(() => {
    return OVERVIEW_KPI_CONFIG.map((kpi) => {
      const series = overviewSeries[kpi.code] || [];
      const availableYears = [...new Set(series.map((row) => row.year))].sort((a, b) => a - b);
      const rankingYear = availableYears.includes(selectedYear)
        ? selectedYear
        : availableYears.filter((year) => year <= selectedYear).at(-1) || availableYears.at(-1);

      if (!rankingYear) {
        return { ...kpi, rankingYear: null, rank: null, total: 0, bestCity: null };
      }

      const ranked = series
        .filter((row) => row.year === rankingYear)
        .sort((a, b) => b.value - a.value);

      const rankIndex = ranked.findIndex((row) => row.city === 'Duisburg');

      return {
        ...kpi,
        rankingYear,
        rank: rankIndex >= 0 ? rankIndex + 1 : null,
        total: ranked.length,
        bestCity: ranked[0]?.city || null,
      };
    });
  }, [overviewSeries, selectedYear]);

  const overviewFreshness = useMemo(() => {
    return OVERVIEW_KPI_CONFIG.map((kpi) => {
      const metadata = indicatorMetadata[kpi.code];
      return {
        ...kpi,
        minYear: metadata?.min_year ?? null,
        maxYear: metadata?.max_year ?? null,
      };
    });
  }, [indicatorMetadata]);

  const overviewLatestYear = useMemo(() => {
    const years = overviewFreshness
      .map((item) => item.maxYear)
      .filter((year) => Number.isFinite(year));
    if (years.length === 0) return null;
    return Math.max(...years);
  }, [overviewFreshness]);

  const overviewComparisonCities = useMemo(() => {
    const cityNames = new Set();
    OVERVIEW_KPI_CONFIG.forEach((kpi) => {
      (overviewSeries[kpi.code] || []).forEach((row) => cityNames.add(row.city));
    });
    return sortCities([...cityNames]);
  }, [overviewSeries, cityOrder]);

  const overviewComparisonRows = useMemo(() => {
    return overviewComparisonCities.map((cityName) => ({
      city: cityName,
      values: Object.fromEntries(
        OVERVIEW_KPI_CONFIG.map((kpi) => [
          kpi.code,
          getCityMetricPoint(kpi.code, cityName, selectedYear, true)?.value ?? null,
        ])
      ),
    }));
  }, [overviewComparisonCities, overviewSeries, selectedYear]);

  const demographicsCities = useMemo(
    () => sortCities(demographicsData.map((d) => d.region_name)),
    [demographicsData, cityOrder]
  );

  const laborCities = useMemo(
    () => sortCities(laborMarketData.map((d) => d.region_name)),
    [laborMarketData, cityOrder]
  );

  const businessCities = useMemo(
    () => sortCities(businessEconomyData.map((d) => d.region_name)),
    [businessEconomyData, cityOrder]
  );

  const publicFinanceCities = useMemo(
    () => sortCities(publicFinanceData.map((d) => d.region_name)),
    [publicFinanceData, cityOrder]
  );

  const trendsCities = useMemo(
    () => sortCities(timeSeriesData.map((d) => d.region_name)),
    [timeSeriesData, cityOrder]
  );

  const cityFilterOptions = useMemo(() => {
    if (activeTab === 'demographics') return demographicsCities;
    if (activeTab === 'labor') return laborCities;
    if (activeTab === 'business') return businessCities;
    if (activeTab === 'finance') return publicFinanceCities;
    if (activeTab === 'trends' && viewMode === 'cities') return trendsCities;
    return sortCities(cities.map((city) => city.region_name));
  }, [activeTab, viewMode, demographicsCities, laborCities, businessCities, publicFinanceCities, trendsCities, cities, cityOrder]);

  useEffect(() => {
    if (cityFilterOptions.length === 0) return;
    setSelectedCities((prev) => {
      const allowed = new Set(cityFilterOptions);
      const next = prev.filter((city) => allowed.has(city));
      if (next.length === 0) return cityFilterOptions;
      if (next.length === prev.length) return prev;
      return next;
    });
  }, [cityFilterOptions]);

  const filteredIndicators = indicators.filter((ind) => {
    const term = indicatorSearch.trim().toLowerCase();
    if (!term) return true;
    return (
      ind.indicator_name.toLowerCase().includes(term) ||
      ind.indicator_code.toLowerCase().includes(term)
    );
  });
  const indicatorNameCounts = filteredIndicators.reduce((acc, ind) => {
    acc[ind.indicator_name] = (acc[ind.indicator_name] || 0) + 1;
    return acc;
  }, {});

  if (
    selectedIndicator &&
    !filteredIndicators.some((ind) => ind.indicator_code === selectedIndicator)
  ) {
    const active = indicators.find((ind) => ind.indicator_code === selectedIndicator);
    if (active) filteredIndicators.unshift(active);
  }

  const visibleDemographicsData = demographicsData.filter((d) =>
    selectedCities.includes(d.region_name)
  );
  const visibleLaborMarketData = laborMarketData.filter((d) =>
    selectedCities.includes(d.region_name)
  );
  const visibleBusinessEconomyData = businessEconomyData.filter((d) =>
    selectedCities.includes(d.region_name)
  );
  const visiblePublicFinanceData = publicFinanceData.filter((d) =>
    selectedCities.includes(d.region_name)
  );
  const publicFinanceUsesFallbackYear = visiblePublicFinanceData.some(
    (row) => Number.parseInt(row.year, 10) < selectedYear
  );
  const visibleTimeSeriesData = timeSeriesData.filter((d) =>
    selectedCities.includes(d.region_name)
  );
  const shouldNormalizeTrends = shouldNormalizeIndicator(
    selectedIndicator,
    visibleTimeSeriesData[0]?.unit_of_measure
  );
  const trendLineData = transformDataForLineChart(visibleTimeSeriesData)
    .map((point) => {
      if (!shouldNormalizeTrends) return point;
      const normalizedValue = normalizeByPopulation(point.value, point.city, point.year);
      if (!Number.isFinite(normalizedValue)) return null;
      return { ...point, value: normalizedValue };
    })
    .filter(Boolean);
  const trendYears = [...new Set(trendLineData.map((d) => d.year))].sort((a, b) => a - b);
  const trendBarYear = trendYears.includes(selectedYear)
    ? selectedYear
    : trendYears[trendYears.length - 1];
  const trendBarData = trendLineData
    .filter((d) => d.year === trendBarYear)
    .map((d) => ({ city: d.city, value: d.value }));

  const scatterMetricName =
    indicators.find((ind) => ind.indicator_code === scatterMetric)?.indicator_name || scatterMetric;
  const shouldNormalizeScatterX = shouldNormalizeIndicator(
    scatterMetric,
    scatterMetricSeries[0]?.unit_of_measure
  );
  const scatterMetricLineData = transformDataForLineChart(scatterMetricSeries)
    .map((point) => {
      if (!shouldNormalizeScatterX) return point;
      const normalizedValue = normalizeByPopulation(point.value, point.city, point.year);
      if (!Number.isFinite(normalizedValue)) return null;
      return { ...point, value: normalizedValue };
    })
    .filter(Boolean);

  const scatterXAxisMap = buildLatestPointMap(scatterMetricLineData, selectedYear);
  const scatterYAxisMap = buildLatestPointMap(trendLineData, selectedYear);
  const scatterData = selectedCities
    .map((city) => {
      const xPoint = scatterXAxisMap.get(city);
      const yPoint = scatterYAxisMap.get(city);
      if (!xPoint || !yPoint) return null;
      return {
        city,
        x: xPoint.value,
        y: yPoint.value,
        xYear: xPoint.year,
        yYear: yPoint.year,
      };
    })
    .filter(Boolean);
  const scatterUsesFallbackYear = scatterData.some(
    (point) => point.xYear < selectedYear || point.yYear < selectedYear
  );
  const scatterXLabel = `${scatterMetricName} (${chartYLabel(
    scatterMetricSeries[0]?.unit_of_measure,
    shouldNormalizeScatterX
  )})`;
  const scatterYLabel = `${visibleTimeSeriesData[0]?.indicator_name || 'Indicator'} (${chartYLabel(
    visibleTimeSeriesData[0]?.unit_of_measure,
    shouldNormalizeTrends
  )})`;

  const shouldNormalizeCategories = shouldNormalizeIndicator(
    selectedIndicator,
    categoryData[0]?.unit_of_measure
  );
  const categoryLineData = transformCategoryDataForLineChart(categoryData)
    .map((point) => {
      if (!shouldNormalizeCategories) return point;
      const normalizedValue = normalizeByPopulation(point.value, selectedCity, point.year);
      if (!Number.isFinite(normalizedValue)) return null;
      return { ...point, value: normalizedValue };
    })
    .filter(Boolean);
  const categoryYears = [...new Set(categoryLineData.map((d) => d.year))].sort((a, b) => a - b);
  const categoryBarYear = categoryYears.includes(selectedYear)
    ? selectedYear
    : categoryYears[categoryYears.length - 1];
  const categoryBarData = categoryLineData
    .filter((d) => d.year === categoryBarYear)
    .map((d) => ({ city: d.city, value: d.value }));

  const duisburgAreaSqKm =
    duisburgInfo?.area_sqkm ??
    cities.find((city) => city.region_name === 'Duisburg')?.area_sqkm ??
    null;

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
    if (viewMode === 'cities' && trendLineData.length > 0) {
      const rows = trendLineData;
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
    } else if (viewMode === 'categories' && categoryLineData.length > 0) {
      const rows = categoryLineData;
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
          className={activeTab === 'business' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('business')}
        >
          Business & GDP
        </button>
        <button
          className={activeTab === 'finance' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('finance')}
        >
          Public Finance
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
            <label htmlFor="indicator-search">Search:</label>
            <input
              id="indicator-search"
              type="text"
              placeholder="Find indicator name or code..."
              value={indicatorSearch}
              onChange={(e) => setIndicatorSearch(e.target.value)}
            />
          </div>
        )}

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
              {filteredIndicators.map((ind) => {
                const meta = indicatorMetadata[ind.indicator_code];
                const yearRange = meta ? ` (${meta.min_year}-${meta.max_year})` : '';
                const duplicateName = indicatorNameCounts[ind.indicator_name] > 1;
                const label = duplicateName
                  ? `${ind.indicator_name} [${ind.indicator_code}]`
                  : ind.indicator_name;
                return (
                  <option key={ind.indicator_code} value={ind.indicator_code}>
                    {label}{yearRange}
                  </option>
                );
              })}
              {filteredIndicators.length === 0 && (
                <option value="" disabled>
                  No indicators match search
                </option>
              )}
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
              <option value="horizontal">📉 Horizontal Bars</option>
              {viewMode === 'cities' && <option value="scatter">🔵 Scatter Plot</option>}
              <option value="table">📋 Table View</option>
            </select>
          </div>
        )}

        {activeTab === 'trends' && viewMode === 'cities' && chartType === 'scatter' && (
          <div className="control-group">
            <label htmlFor="scatter-metric-select">X Metric:</label>
            <select
              id="scatter-metric-select"
              value={scatterMetric}
              onChange={(e) => setScatterMetric(e.target.value)}
            >
              {filteredIndicators.map((ind) => (
                <option key={ind.indicator_code} value={ind.indicator_code}>
                  {ind.indicator_name}
                </option>
              ))}
            </select>
          </div>
        )}

        {activeTab !== 'overview' && (
          <div className="control-group">
            <label>Scale:</label>
            <div className="button-group">
              <button
                type="button"
                className={!normalizePerCapita ? 'toggle-btn active' : 'toggle-btn'}
                onClick={() => setNormalizePerCapita(false)}
              >
                Absolute
              </button>
              <button
                type="button"
                className={normalizePerCapita ? 'toggle-btn active' : 'toggle-btn'}
                onClick={() => setNormalizePerCapita(true)}
              >
                Per 1,000 Residents
              </button>
            </div>
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

        {(activeTab === 'demographics' ||
          activeTab === 'labor' ||
          activeTab === 'business' ||
          activeTab === 'finance' ||
          (activeTab === 'trends' && viewMode === 'cities')) && (
          <div className="control-group city-filter-group">
            <label>Cities:</label>
            <div className="city-filter-list">
              {cityFilterOptions.map((cityName) => {
                const checked = selectedCities.includes(cityName);
                return (
                  <label key={cityName} className={`city-filter-chip ${checked ? 'active' : ''}`}>
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleSelectedCity(cityName)}
                    />
                    <span>{cityName}</span>
                  </label>
                );
              })}
            </div>
          </div>
        )}

        {activeTab === 'trends' && (
          <div className="control-group view-actions-group">
            <label htmlFor="saved-view-select">Views:</label>
            <input
              id="save-view-name"
              type="text"
              placeholder="Optional name"
              value={viewName}
              onChange={(e) => setViewName(e.target.value)}
            />
            <button type="button" className="toggle-btn" onClick={handleSaveView}>
              Save View
            </button>
            <select
              id="saved-view-select"
              value={savedViewSelection}
              onChange={(e) => setSavedViewSelection(e.target.value)}
            >
              <option value="">Saved views ({savedViews.length})</option>
              {savedViews.map((view) => (
                <option key={view.id} value={view.id}>
                  {view.name}
                </option>
              ))}
            </select>
            <button
              type="button"
              className="toggle-btn"
              disabled={!savedViewSelection}
              onClick={handleLoadSavedView}
            >
              Load
            </button>
            <button
              type="button"
              className="toggle-btn"
              disabled={!savedViewSelection}
              onClick={handleDeleteSavedView}
            >
              Delete
            </button>
            <button type="button" className="toggle-btn" onClick={handleCopyLink}>
              Copy Link
            </button>
            <button type="button" className="toggle-btn" onClick={handlePrintPdf}>
              Print/PDF
            </button>
            {shareMessage ? <span className="view-action-status">{shareMessage}</span> : null}
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
              {duisburgAreaSqKm ? (
                <div className="info-item">
                  <span className="label">Area:</span>
                  <span className="value">{duisburgAreaSqKm} km²</span>
                </div>
              ) : null}
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

          <div className="overview-block kpi-block">
            <h2>Executive KPIs</h2>
            {overviewLoading ? (
              <p className="overview-muted">Loading KPI trends...</p>
            ) : (
              <div className="kpi-grid">
                {duisburgOverviewKpis.map((kpi) => (
                  <div key={kpi.code} className="kpi-card">
                    <div className="kpi-head">
                      <h3>{kpi.label}</h3>
                      <span className="kpi-year">{kpi.current?.year || '—'}</span>
                    </div>
                    <div className="kpi-value">{formatKpiValue(kpi.code, kpi.current?.value)}</div>
                    <div className="kpi-change">
                      {kpi.change !== null && Number.isFinite(kpi.changePct) ? (
                        <span className={kpi.change >= 0 ? 'kpi-up' : 'kpi-down'}>
                          {kpi.change >= 0 ? '▲' : '▼'} {Math.abs(kpi.changePct).toFixed(1)}% YoY
                        </span>
                      ) : (
                        <span className="overview-muted">No prior year for YoY</span>
                      )}
                    </div>
                    <MiniSparkline data={kpi.sparkline} />
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="overview-block ranking-block">
            <h2>Duisburg Rankings</h2>
            <div className="ranking-list">
              {overviewRankings.map((rankItem) => (
                <div key={rankItem.code} className="ranking-chip">
                  {rankItem.rank ? (
                    <>
                      <strong>#{rankItem.rank}</strong> of {rankItem.total} in {rankItem.rankingYear} for {rankItem.rankLabel}
                    </>
                  ) : (
                    <span className="overview-muted">No ranking data for {rankItem.label}</span>
                  )}
                </div>
              ))}
            </div>
          </div>

          <div className="overview-block freshness-block">
            <h2>Data Freshness</h2>
            <p className="overview-muted">
              Most recent update across core KPIs: {overviewLatestYear || 'Unknown'}
            </p>
            <div className="freshness-list">
              {overviewFreshness.map((item) => (
                <div key={item.code} className="freshness-item">
                  <span>{item.label}</span>
                  <span>
                    {item.minYear && item.maxYear
                      ? `${item.minYear} - ${item.maxYear}`
                      : 'No metadata'}
                  </span>
                </div>
              ))}
            </div>
          </div>

          <div className="overview-block comparison-block">
            <h2>Quick Comparison ({selectedYear})</h2>
            <div className="overview-table-wrap">
              <table className="overview-table">
                <thead>
                  <tr>
                    <th>City</th>
                    {OVERVIEW_KPI_CONFIG.map((kpi) => (
                      <th key={kpi.code}>{kpi.label}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {overviewComparisonRows.map((row) => (
                    <tr key={row.city} className={row.city === 'Duisburg' ? 'duisburg-row' : ''}>
                      <td>{row.city}</td>
                      {OVERVIEW_KPI_CONFIG.map((kpi) => (
                        <td key={kpi.code}>{formatKpiValue(kpi.code, row.values[kpi.code])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="overview-muted">Only exact values from {selectedYear} are shown in this table.</p>
          </div>
        </div>
      )}

      {activeTab === 'demographics' && visibleDemographicsData.length > 0 && (
      <div className="charts-section">
        <h2>Demographics Comparison ({selectedYear})</h2>
        <div className="charts-grid">
          {[...new Set(visibleDemographicsData.map((d) => d.indicator_name))].map(
            (indicatorName) => {
                const indicatorRow = visibleDemographicsData.find(
                  (d) => d.indicator_name === indicatorName
                );
                const unit = indicatorRow?.unit_of_measure;
                const indicatorCode = indicatorRow?.indicator_code;
                const { data: chartData, normalized } = buildCityComparisonBarData(
                  visibleDemographicsData,
                  indicatorName,
                  indicatorCode,
                  unit,
                  selectedYear
                );
                if (chartData.length === 0) return null;

                return (
                <div key={indicatorName} className="chart-container">
                  <BarChart
                    data={chartData}
                    title={chartTitleWithScale(indicatorName, normalized)}
                    xLabel="City"
                    yLabel={chartYLabel(unit, normalized)}
                    highlightCity="Duisburg"
                    colorMap={CITY_COLOR_MAP}
                  />
                </div>
              );
            }
            )}
          </div>
        </div>
      )}

      {activeTab === 'labor' && visibleLaborMarketData.length > 0 && (
        <div className="charts-section">
          <h2>Labor Market Comparison ({selectedYear})</h2>
          <div className="charts-grid">
            {[...new Set(visibleLaborMarketData.map((d) => d.indicator_name))].map(
              (indicatorName) => {
                const indicatorRow = visibleLaborMarketData.find(
                  (d) => d.indicator_name === indicatorName
                );
                const unit = indicatorRow?.unit_of_measure;
                const indicatorCode = indicatorRow?.indicator_code;
                const { data: chartData, normalized } = buildCityComparisonBarData(
                  visibleLaborMarketData,
                  indicatorName,
                  indicatorCode,
                  unit,
                  selectedYear
                );
                if (chartData.length === 0) return null;

                return (
                <div key={indicatorName} className="chart-container">
                  <BarChart
                    data={chartData}
                    title={chartTitleWithScale(indicatorName, normalized)}
                    xLabel="City"
                    yLabel={chartYLabel(unit, normalized)}
                    highlightCity="Duisburg"
                    colorMap={CITY_COLOR_MAP}
                  />
                </div>
              );
            }
            )}
          </div>
        </div>
      )}

      {activeTab === 'business' && visibleBusinessEconomyData.length > 0 && (
        <div className="charts-section">
          <h2>Business & GDP Comparison ({selectedYear})</h2>
          <div className="charts-grid">
            {[...new Set(visibleBusinessEconomyData.map((d) => d.indicator_name))].map(
              (indicatorName) => {
                const indicatorRow = visibleBusinessEconomyData.find(
                  (d) => d.indicator_name === indicatorName
                );
                const unit = indicatorRow?.unit_of_measure;
                const indicatorCode = indicatorRow?.indicator_code;
                const { data: chartData, normalized } = buildCityComparisonBarData(
                  visibleBusinessEconomyData,
                  indicatorName,
                  indicatorCode,
                  unit,
                  selectedYear
                );
                if (chartData.length === 0) return null;

                return (
                  <div key={indicatorName} className="chart-container">
                    <BarChart
                      data={chartData}
                      title={chartTitleWithScale(indicatorName, normalized)}
                      xLabel="City"
                      yLabel={chartYLabel(unit, normalized)}
                      highlightCity="Duisburg"
                      colorMap={CITY_COLOR_MAP}
                    />
                  </div>
                );
              }
            )}
          </div>
        </div>
      )}

      {activeTab === 'finance' && visiblePublicFinanceData.length > 0 && (
        <div className="charts-section">
          <h2>Public Finance Comparison ({selectedYear})</h2>
          {publicFinanceUsesFallbackYear && (
            <p className="data-hint">
              Some indicators are shown at their latest available year up to {selectedYear}.
            </p>
          )}
          <div className="charts-grid">
            {[...new Set(visiblePublicFinanceData.map((d) => d.indicator_name))].map(
              (indicatorName) => {
                const indicatorRow = visiblePublicFinanceData.find(
                  (d) => d.indicator_name === indicatorName
                );
                const unit = indicatorRow?.unit_of_measure;
                const indicatorCode = indicatorRow?.indicator_code;
                const { data: chartData, normalized } = buildCityComparisonBarData(
                  visiblePublicFinanceData,
                  indicatorName,
                  indicatorCode,
                  unit,
                  selectedYear
                );
                if (chartData.length === 0) return null;

                return (
                  <div key={indicatorName} className="chart-container">
                    <BarChart
                      data={chartData}
                      title={chartTitleWithScale(indicatorName, normalized)}
                      xLabel="City"
                      yLabel={chartYLabel(unit, normalized)}
                      highlightCity="Duisburg"
                      colorMap={CITY_COLOR_MAP}
                    />
                  </div>
                );
              }
            )}
          </div>
        </div>
      )}

      {activeTab === 'trends' && viewMode === 'cities' && trendLineData.length > 0 && (
        <div className="charts-section">
          <h2>Historical Trends - City Comparison</h2>
          {(chartType === 'bar' || chartType === 'horizontal') && trendBarYear !== selectedYear && (
            <p className="data-hint">
              No data for {selectedYear}. Showing latest available year: {trendBarYear}.
            </p>
          )}
          {chartType === 'scatter' && scatterUsesFallbackYear && (
            <p className="data-hint">
              Some points use the latest available year up to {selectedYear} for one axis.
            </p>
          )}
          <div className="chart-container">
            {chartType === 'line' && (
              <LineChart
                data={trendLineData}
                title={chartTitleWithScale(visibleTimeSeriesData[0]?.indicator_name || 'Time Series', shouldNormalizeTrends)}
                xLabel="Year"
                yLabel={chartYLabel(visibleTimeSeriesData[0]?.unit_of_measure, shouldNormalizeTrends)}
                highlightCity="Duisburg"
                colorMap={CITY_COLOR_MAP}
              />
            )}
            {chartType === 'area' && (
              <AreaChart
                data={trendLineData}
                title={chartTitleWithScale(visibleTimeSeriesData[0]?.indicator_name || 'Time Series', shouldNormalizeTrends)}
                xLabel="Year"
                yLabel={chartYLabel(visibleTimeSeriesData[0]?.unit_of_measure, shouldNormalizeTrends)}
                highlightCity="Duisburg"
                colorMap={CITY_COLOR_MAP}
              />
            )}
            {chartType === 'bar' && (
              <BarChart
                data={trendBarData}
                title={chartTitleWithScale(`${visibleTimeSeriesData[0]?.indicator_name || 'Indicator'} (${trendBarYear})`, shouldNormalizeTrends)}
                xLabel="City"
                yLabel={chartYLabel(visibleTimeSeriesData[0]?.unit_of_measure, shouldNormalizeTrends)}
                highlightCity="Duisburg"
                colorMap={CITY_COLOR_MAP}
              />
            )}
            {chartType === 'horizontal' && (
              <HorizontalBarChart
                data={trendBarData}
                title={chartTitleWithScale(`${visibleTimeSeriesData[0]?.indicator_name || 'Indicator'} (${trendBarYear})`, shouldNormalizeTrends)}
                xLabel={chartYLabel(visibleTimeSeriesData[0]?.unit_of_measure, shouldNormalizeTrends)}
                yLabel="City"
                highlightCity="Duisburg"
                colorMap={CITY_COLOR_MAP}
              />
            )}
            {chartType === 'scatter' && scatterData.length > 0 && (
              <ScatterChart
                data={scatterData}
                title={`${visibleTimeSeriesData[0]?.indicator_name || 'Indicator'} vs ${scatterMetricName} (${selectedYear})`}
                xLabel={scatterXLabel}
                yLabel={scatterYLabel}
                highlightCity="Duisburg"
                colorMap={CITY_COLOR_MAP}
              />
            )}
            {chartType === 'scatter' && scatterData.length === 0 && (
              <div className="no-data">
                <p>No matching city values for this scatter comparison.</p>
              </div>
            )}
            {chartType === 'table' && (
              <DataTable
                data={trendLineData}
                title={chartTitleWithScale(visibleTimeSeriesData[0]?.indicator_name || 'Time Series', shouldNormalizeTrends)}
                highlightCity="Duisburg"
                maxFractionDigits={shouldNormalizeTrends ? 2 : 0}
              />
            )}
          </div>
        </div>
      )}

      {activeTab === 'trends' && viewMode === 'categories' && categoryLineData.length > 0 && (
        <div className="charts-section">
          <h2>Historical Trends - Category Breakdown for {selectedCity}</h2>
          {(chartType === 'bar' || chartType === 'horizontal') && categoryBarYear !== selectedYear && (
            <p className="data-hint">
              No data for {selectedYear}. Showing latest available year: {categoryBarYear}.
            </p>
          )}
          <div className="chart-container">
            {chartType === 'line' && (
              <LineChart
                data={categoryLineData}
                title={chartTitleWithScale(categoryData[0]?.indicator_name || 'Category Breakdown', shouldNormalizeCategories)}
                xLabel="Year"
                yLabel={chartYLabel(categoryData[0]?.unit_of_measure, shouldNormalizeCategories)}
                highlightCity={null}
              />
            )}
            {chartType === 'area' && (
              <AreaChart
                data={categoryLineData}
                title={chartTitleWithScale(categoryData[0]?.indicator_name || 'Category Breakdown', shouldNormalizeCategories)}
                xLabel="Year"
                yLabel={chartYLabel(categoryData[0]?.unit_of_measure, shouldNormalizeCategories)}
                highlightCity={null}
              />
            )}
            {chartType === 'bar' && (
              <BarChart
                data={categoryBarData}
                title={chartTitleWithScale(`${categoryData[0]?.indicator_name || 'Category Breakdown'} (${categoryBarYear})`, shouldNormalizeCategories)}
                xLabel="Category"
                yLabel={chartYLabel(categoryData[0]?.unit_of_measure, shouldNormalizeCategories)}
                highlightCity={null}
              />
            )}
            {chartType === 'horizontal' && (
              <HorizontalBarChart
                data={categoryBarData}
                title={chartTitleWithScale(`${categoryData[0]?.indicator_name || 'Category Breakdown'} (${categoryBarYear})`, shouldNormalizeCategories)}
                xLabel={chartYLabel(categoryData[0]?.unit_of_measure, shouldNormalizeCategories)}
                yLabel="Category"
                highlightCity={null}
              />
            )}
            {chartType === 'table' && (
              <DataTable
                data={categoryLineData}
                title={chartTitleWithScale(categoryData[0]?.indicator_name || 'Category Breakdown', shouldNormalizeCategories)}
                highlightCity={null}
                maxFractionDigits={shouldNormalizeCategories ? 2 : 0}
              />
            )}
          </div>
        </div>
      )}

      {activeTab === 'demographics' && visibleDemographicsData.length === 0 && (
        <div className="no-data">
          <p>No demographics data available for {selectedYear} with current city filter.</p>
        </div>
      )}

      {activeTab === 'labor' && visibleLaborMarketData.length === 0 && (
        <div className="no-data">
          <p>No labor market data available for {selectedYear} with current city filter.</p>
        </div>
      )}

      {activeTab === 'business' && visibleBusinessEconomyData.length === 0 && (
        <div className="no-data">
          <p>No business economy data available for {selectedYear} with current city filter.</p>
        </div>
      )}

      {activeTab === 'finance' && visiblePublicFinanceData.length === 0 && (
        <div className="no-data">
          <p>No public finance data available for {selectedYear} with current city filter.</p>
        </div>
      )}

      {activeTab === 'trends' && trendLineData.length === 0 && selectedIndicator && (
        <div className="no-data">
          <p>No time series data available for the selected indicator and city filter.</p>
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
          scatterMetric,
          normalizePerCapita,
          viewMode,
          selectedCity,
        }}
      />
    </div>
  );
}

export default App;
