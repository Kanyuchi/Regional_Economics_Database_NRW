import { useEffect, useMemo, useState } from 'react';
import apiService from './services/api';
import BarChart from './components/BarChart';
import LineChart from './components/LineChart';
import AreaChart from './components/AreaChart';
import HorizontalBarChart from './components/HorizontalBarChart';
import ScatterChart from './components/ScatterChart';
import DataTable from './components/DataTable';
import Chatbot from './components/Chatbot';
import IctDotChart from './components/IctDotChart';
import IctTrendChart from './components/IctTrendChart';
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
const VALID_TABS = new Set(['overview', 'demographics', 'labor', 'business', 'ict', 'finance', 'trends']);
const VALID_VIEW_MODES = new Set(['cities', 'categories']);
const VALID_CHART_TYPES = new Set(['line', 'area', 'bar', 'horizontal', 'scatter', 'table']);
const FALLBACK_GROUP_TITLES = {
  demographics: 'Demographics',
  labor: 'Labor Market',
  business: 'Business & GDP',
  ict: 'ICT / Digitization',
  finance: 'Public Finance',
};

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
    overviewMetricCode: params.get('overviewMetric') || undefined,
    headToHeadMetricCode: params.get('headMetric') || undefined,
    headToHeadLeftCity: params.get('headLeft') || undefined,
    headToHeadRightCity: params.get('headRight') || undefined,
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
  const [ictData, setIctData] = useState([]);
  const [ictAllYearsData, setIctAllYearsData] = useState({});
  const [ictAllYearsLoading, setIctAllYearsLoading] = useState(false);
  const [ictView, setIctView] = useState('snapshot'); // 'snapshot' | 'trends'
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
  const [sectionOpenState, setSectionOpenState] = useState({});
  const [overviewMetricCode, setOverviewMetricCode] = useState(initialUrlState.overviewMetricCode ?? 'pop_total');
  const [headToHeadLeftCity, setHeadToHeadLeftCity] = useState(initialUrlState.headToHeadLeftCity ?? 'Duisburg');
  const [headToHeadRightCity, setHeadToHeadRightCity] = useState(initialUrlState.headToHeadRightCity ?? 'Essen');
  const [headToHeadMetricCode, setHeadToHeadMetricCode] = useState(initialUrlState.headToHeadMetricCode ?? 'GDP_MARKET_PRICE');
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
    if (overviewMetricCode) params.set('overviewMetric', overviewMetricCode);
    if (headToHeadMetricCode) params.set('headMetric', headToHeadMetricCode);
    if (headToHeadLeftCity) params.set('headLeft', headToHeadLeftCity);
    if (headToHeadRightCity) params.set('headRight', headToHeadRightCity);
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
    overviewMetricCode,
    headToHeadMetricCode,
    headToHeadLeftCity,
    headToHeadRightCity,
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

  useEffect(() => {
    if (activeTab === 'ict') {
      loadIctAllYears();
    }
  }, [activeTab]);

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

      // Create a map of indicator_code -> metadata (year range + best available scope)
      const metadataMap = {};
      metadataRes.data.forEach((item) => {
        metadataMap[item.indicator_code] = {
          min_year: parseInt(item.min_year),
          max_year: parseInt(item.max_year),
          year_count: parseInt(item.year_count),
          data_scope: item.data_scope || 'city',
          primary_source_table: item.primary_source_table || 'fact_demographics',
        };
      });
      setIndicatorMetadata(metadataMap);

      const visibleIndicators = indicatorsRes.data.filter((ind) => {
        if (ind.indicator_code === 'unemployment_rate') return false; // legacy alias
        const metadata = metadataMap[ind.indicator_code];
        // Trends view is city-comparison focused, so only expose indicators with city-level coverage.
        return Boolean(metadata && metadata.data_scope === 'city');
      });
      setIndicators(visibleIndicators);

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
      const [demoRes, laborRes, businessRes, ictRes, financeRes] = await Promise.allSettled([
        apiService.getDemographics(year),
        apiService.getLaborMarket(year),
        apiService.getBusinessEconomy(year),
        apiService.getIct(year),
        apiService.getPublicFinance(year),
      ]);

      setDemographicsData(demoRes.status === 'fulfilled' ? demoRes.value.data : []);
      setLaborMarketData(laborRes.status === 'fulfilled' ? laborRes.value.data : []);
      setBusinessEconomyData(businessRes.status === 'fulfilled' ? businessRes.value.data : []);
      setIctData(ictRes.status === 'fulfilled' ? ictRes.value.data : []);
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
      if (ictRes.status === 'rejected') {
        console.error('Error loading ICT data:', ictRes.reason);
      }
      if (financeRes.status === 'rejected') {
        console.error('Error loading public finance data:', financeRes.reason);
      }
    } catch (err) {
      console.error('Error loading year data:', err);
    }
  };

  const ICT_YEARS = [2020, 2021, 2022, 2023, 2024];

  const loadIctAllYears = async () => {
    if (ictAllYearsLoading || Object.keys(ictAllYearsData).length > 0) return;
    setIctAllYearsLoading(true);
    try {
      const results = await Promise.allSettled(
        ICT_YEARS.map((yr) => apiService.getIct(yr))
      );
      const combined = {};
      results.forEach((res, i) => {
        if (res.status === 'fulfilled' && res.value.data?.length > 0) {
          combined[ICT_YEARS[i]] = res.value.data;
        }
      });
      setIctAllYearsData(combined);
    } catch (err) {
      console.error('Error loading multi-year ICT data:', err);
    } finally {
      setIctAllYearsLoading(false);
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

  const toTitleCase = (value) =>
    String(value || '')
      .replace(/[_-]+/g, ' ')
      .trim()
      .replace(/\s+/g, ' ')
      .replace(/\b\w/g, (char) => char.toUpperCase());

  const inferFallbackGroup = (tabKey, indicatorName = '') => {
    const name = indicatorName.toLowerCase();
    if (tabKey === 'demographics') {
      if (name.includes('bevölk') || name.includes('population')) return 'Population';
      if (name.includes('pflege') || name.includes('care')) return 'Care Services';
      if (name.includes('kranken') || name.includes('arzt') || name.includes('hospital') || name.includes('health')) return 'Healthcare';
      if (name.includes('straße') || name.includes('autobahn') || name.includes('road')) return 'Infrastructure';
    }
    if (tabKey === 'labor') {
      if (name.includes('arbeitslos') || name.includes('unemployment')) return 'Unemployment';
      if (name.includes('pendler') || name.includes('commuter')) return 'Commuters';
      if (name.includes('entgelt') || name.includes('wage')) return 'Wages';
      if (name.includes('beschäft')) return 'Employment';
    }
    if (tabKey === 'business') {
      if (name.includes('gdp') || name.includes('bip') || name.includes('value added') || name.includes('bruttowert')) return 'GDP & GVA';
      if (name.includes('gründ') || name.includes('insolvenz') || name.includes('business')) return 'Business Dynamics';
      if (name.includes('compensation') || name.includes('entgelt')) return 'Compensation';
    }
    if (tabKey === 'ict') {
      if (name.includes('internet') || name.includes('website')) return 'Connectivity';
      if (name.includes('cloud') || name.includes('big data') || name.includes('ai') || name.includes('robot')) return 'Digital Adoption';
      if (name.includes('e-commerce') || name.includes('online')) return 'Digital Commerce';
    }
    if (tabKey === 'finance') {
      if (name.includes('steuer') || name.includes('tax') || name.includes('einkommens')) return 'Tax & Income';
      if (name.includes('municipal') || name.includes('finanz')) return 'Municipal Finances';
    }
    return FALLBACK_GROUP_TITLES[tabKey] || 'Indicators';
  };

  const getRowGroup = (row, tabKey) => {
    const explicit = row.indicator_subcategory || row.indicator_category;
    if (explicit && String(explicit).trim()) {
      return toTitleCase(explicit);
    }
    return inferFallbackGroup(tabKey, row.indicator_name);
  };

  const getGroupedIndicators = (rows, tabKey) => {
    const map = new Map();
    rows.forEach((row) => {
      const group = getRowGroup(row, tabKey);
      if (!map.has(group)) map.set(group, new Set());
      map.get(group).add(row.indicator_name);
    });
    return Array.from(map.entries())
      .map(([group, indicators]) => ({
        group,
        indicators: Array.from(indicators).sort((a, b) => a.localeCompare(b)),
      }))
      .sort((a, b) => a.group.localeCompare(b.group));
  };

  const handleSectionToggle = (sectionKey, nextOpen) => {
    setSectionOpenState((prev) => ({
      ...prev,
      [sectionKey]: nextOpen,
    }));
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
    overviewMetricCode,
    headToHeadMetricCode,
    headToHeadLeftCity,
    headToHeadRightCity,
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
    if (state.overviewMetricCode) setOverviewMetricCode(state.overviewMetricCode);
    if (state.headToHeadMetricCode) setHeadToHeadMetricCode(state.headToHeadMetricCode);
    if (state.headToHeadLeftCity) setHeadToHeadLeftCity(state.headToHeadLeftCity);
    if (state.headToHeadRightCity) setHeadToHeadRightCity(state.headToHeadRightCity);
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

  const formatIndicatorValue = (value, unit) => {
    if (!Number.isFinite(value)) return '—';
    const normalizedUnit = (unit || '').toLowerCase();
    const isPercent = normalizedUnit.includes('prozent') || normalizedUnit.includes('percent') || normalizedUnit.includes('%');
    const maxFractionDigits = isPercent ? 1 : Math.abs(value) >= 100 ? 0 : 2;
    const formatted = value.toLocaleString('de-DE', {
      minimumFractionDigits: 0,
      maximumFractionDigits: maxFractionDigits,
    });
    return unit ? `${formatted} ${formatUnit(unit)}` : formatted;
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

  const overviewMetricConfig = OVERVIEW_KPI_CONFIG.find((kpi) => kpi.code === overviewMetricCode) || OVERVIEW_KPI_CONFIG[0];
  const headToHeadMetricConfig = OVERVIEW_KPI_CONFIG.find((kpi) => kpi.code === headToHeadMetricCode) || OVERVIEW_KPI_CONFIG[0];

  const overviewMetricValues = useMemo(() => {
    return overviewComparisonCities
      .map((cityName) => {
        const exactValue = getCityMetricPoint(overviewMetricConfig.code, cityName, selectedYear, true)?.value ?? null;
        const fallbackValue = getCityMetricPoint(overviewMetricConfig.code, cityName, selectedYear)?.value ?? null;
        const cityGeo = cities.find((city) => city.region_name === cityName);
        return {
          city: cityName,
          value: Number.isFinite(exactValue) ? exactValue : fallbackValue,
          latitude: Number.parseFloat(cityGeo?.latitude),
          longitude: Number.parseFloat(cityGeo?.longitude),
        };
      })
      .filter((row) => Number.isFinite(row.value));
  }, [overviewComparisonCities, overviewMetricConfig.code, selectedYear, overviewSeries, cities]);

  const mapPoints = overviewMetricValues.filter(
    (row) => Number.isFinite(row.latitude) && Number.isFinite(row.longitude)
  );

  const mapProjection = useMemo(() => {
    if (mapPoints.length === 0) return null;
    const minLat = Math.min(...mapPoints.map((row) => row.latitude));
    const maxLat = Math.max(...mapPoints.map((row) => row.latitude));
    const minLon = Math.min(...mapPoints.map((row) => row.longitude));
    const maxLon = Math.max(...mapPoints.map((row) => row.longitude));
    const latSpan = maxLat - minLat || 1;
    const lonSpan = maxLon - minLon || 1;
    const width = 780;
    const height = 360;
    const padding = 36;
    const maxValue = Math.max(...mapPoints.map((row) => row.value)) || 1;
    return {
      width,
      height,
      project: (row) => ({
        x: padding + ((row.longitude - minLon) / lonSpan) * (width - padding * 2),
        y: padding + ((maxLat - row.latitude) / latSpan) * (height - padding * 2),
        r: 8 + Math.sqrt(Math.max(row.value, 0) / maxValue) * 18,
      }),
    };
  }, [mapPoints]);

  const topBottomRankings = useMemo(() => {
    const sorted = [...overviewMetricValues].sort((a, b) => b.value - a.value);
    return {
      top: sorted.slice(0, Math.min(3, sorted.length)),
      bottom: [...sorted].reverse().slice(0, Math.min(3, sorted.length)),
    };
  }, [overviewMetricValues]);

  const headToHeadCities = useMemo(() => {
    return overviewComparisonCities.length > 0 ? overviewComparisonCities : sortCities(cities.map((city) => city.region_name));
  }, [overviewComparisonCities, cities, cityOrder]);

  useEffect(() => {
    if (headToHeadCities.length === 0) return;
    if (!headToHeadCities.includes(headToHeadLeftCity)) {
      setHeadToHeadLeftCity(headToHeadCities[0]);
    }
    if (!headToHeadCities.includes(headToHeadRightCity)) {
      const fallbackRight = headToHeadCities.find((city) => city !== headToHeadCities[0]) || headToHeadCities[0];
      setHeadToHeadRightCity(fallbackRight);
    }
  }, [headToHeadCities, headToHeadLeftCity, headToHeadRightCity]);

  const headToHeadLeftValue = getCityMetricPoint(headToHeadMetricConfig.code, headToHeadLeftCity, selectedYear)?.value ?? null;
  const headToHeadRightValue = getCityMetricPoint(headToHeadMetricConfig.code, headToHeadRightCity, selectedYear)?.value ?? null;
  const headToHeadDelta =
    Number.isFinite(headToHeadLeftValue) && Number.isFinite(headToHeadRightValue)
      ? headToHeadLeftValue - headToHeadRightValue
      : null;
  const headToHeadDeltaPct =
    headToHeadDelta !== null && Number.isFinite(headToHeadRightValue) && headToHeadRightValue !== 0
      ? (headToHeadDelta / headToHeadRightValue) * 100
      : null;

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

  const ictCities = useMemo(
    () => sortCities(ictData.map((d) => d.region_name)),
    [ictData, cityOrder]
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
    if (activeTab === 'ict') return ictCities;
    if (activeTab === 'finance') return publicFinanceCities;
    if (activeTab === 'trends' && viewMode === 'cities') return trendsCities;
    return sortCities(cities.map((city) => city.region_name));
  }, [activeTab, viewMode, demographicsCities, laborCities, businessCities, ictCities, publicFinanceCities, trendsCities, cities, cityOrder]);

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
  const visibleIctData = ictData;
  const ictRegions = sortCities(visibleIctData.map((row) => row.region_name));
  const ictRegionTypes = [...new Set(visibleIctData.map((row) => row.region_type).filter(Boolean))];
  const ictShownYears = [...new Set(visibleIctData
    .map((row) => Number.parseInt(row.year, 10))
    .filter((year) => Number.isFinite(year)))].sort((a, b) => a - b);
  const ictShownYear = ictShownYears.length > 0 ? ictShownYears[ictShownYears.length - 1] : null;
  const ictSingleRegion = ictRegions.length === 1;
  const ictIsStateScope = ictRegionTypes.length === 1 && ictRegionTypes[0] === 'state';
  const ictScopeLabel = ictSingleRegion
    ? `${ictRegions[0]} (${toTitleCase(ictRegionTypes[0] || 'region')})`
    : `${ictRegions.length} regions`;
  const showScaleToggle = activeTab !== 'overview' && !(activeTab === 'ict' && (ictSingleRegion || ictIsStateScope));
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

  const ictLeadingHint = visibleIctData.length > 0 ? (
    ictShownYear && ictShownYear !== selectedYear ? (
      <p className="data-hint data-hint--warning">
        ICT data is only available for {ictShownYear}. The year filter ({selectedYear}) has no effect on this tab — the figures below always reflect the latest available survey ({ictShownYear}).
      </p>
    ) : (
      <p className="data-hint">
        ICT scope: {ictScopeLabel}. Data year: {ictShownYear ?? selectedYear}.
      </p>
    )
  ) : null;

  const renderIctKpiSections = (rows, title, emptyMessage, leadingHint = null) => {
    if (!rows || rows.length === 0) {
      return (
        <div className="no-data">
          <p>{emptyMessage}</p>
        </div>
      );
    }

    const shownYear = rows[0]?.year ?? null;

    return (
      <div className="charts-section">
        <div className="ict-section-header">
          <h2>{title}</h2>
          <div className="ict-view-toggle">
            <button
              className={ictView === 'snapshot' ? 'ict-toggle-btn active' : 'ict-toggle-btn'}
              onClick={() => setIctView('snapshot')}
            >
              Snapshot
            </button>
            <button
              className={ictView === 'trends' ? 'ict-toggle-btn active' : 'ict-toggle-btn'}
              onClick={() => { setIctView('trends'); loadIctAllYears(); }}
            >
              Trends 2020–2024
            </button>
          </div>
        </div>
        {leadingHint}
        {ictView === 'snapshot' && (
          <IctDotChart data={rows} year={shownYear} />
        )}
        {ictView === 'trends' && (
          ictAllYearsLoading
            ? <div className="loading-indicator"><p>Loading multi-year data…</p></div>
            : Object.keys(ictAllYearsData).length >= 2
              ? <IctTrendChart yearData={ictAllYearsData} />
              : <div className="no-data"><p>Multi-year ICT data not yet loaded.</p></div>
        )}
      </div>
    );
  };

  const renderGroupedBarSections = (
    rows,
    tabKey,
    title,
    emptyMessage,
    leadingHint = null,
    options = {}
  ) => {
    const {
      xAxisLabel = 'City',
      highlightCity = 'Duisburg',
      colorMap = CITY_COLOR_MAP,
    } = options;

    if (!rows || rows.length === 0) {
      return (
        <div className="no-data">
          <p>{emptyMessage}</p>
        </div>
      );
    }

    const groups = getGroupedIndicators(rows, tabKey);
    if (groups.length === 0) {
      return (
        <div className="no-data">
          <p>{emptyMessage}</p>
        </div>
      );
    }

    return (
      <div className="charts-section">
        <h2>{title}</h2>
        {leadingHint}
        <div className="section-groups">
          {groups.map((group, groupIndex) => {
            const sectionKey = `${tabKey}:${group.group}`;
            const isOpen = sectionOpenState[sectionKey] ?? groupIndex === 0;
            return (
              <details
                key={sectionKey}
                className="section-group"
                open={isOpen}
                onToggle={(event) => handleSectionToggle(sectionKey, event.currentTarget.open)}
              >
                <summary className="section-group-summary">
                  <span>{group.group}</span>
                  <span className="section-group-meta">{group.indicators.length} indicators</span>
                </summary>
                <div className="section-group-content">
                  <div className="charts-grid">
                    {group.indicators.map((indicatorName) => {
                      const indicatorRow = rows.find((row) => row.indicator_name === indicatorName);
                      const unit = indicatorRow?.unit_of_measure;
                      const indicatorCode = indicatorRow?.indicator_code;
                      const { data: chartData, normalized } = buildCityComparisonBarData(
                        rows,
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
                            xLabel={xAxisLabel}
                            yLabel={chartYLabel(unit, normalized)}
                            highlightCity={highlightCity}
                            colorMap={colorMap}
                          />
                        </div>
                      );
                    })}
                  </div>
                </div>
              </details>
            );
          })}
        </div>
      </div>
    );
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
          className={activeTab === 'ict' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('ict')}
        >
          ICT / Digitization
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

        {showScaleToggle && (
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

          <div className="overview-block map-block">
            <div className="overview-block-head">
              <h2>Map & Ranking Insights</h2>
              <div className="control-group">
                <label htmlFor="overview-metric-select">Metric:</label>
                <select
                  id="overview-metric-select"
                  value={overviewMetricConfig.code}
                  onChange={(event) => setOverviewMetricCode(event.target.value)}
                >
                  {OVERVIEW_KPI_CONFIG.map((metric) => (
                    <option key={metric.code} value={metric.code}>
                      {metric.label}
                    </option>
                  ))}
                </select>
              </div>
            </div>
            {mapProjection && mapPoints.length > 0 ? (
              <div className="city-map-wrap">
                <svg
                  className="city-map"
                  viewBox={`0 0 ${mapProjection.width} ${mapProjection.height}`}
                  preserveAspectRatio="xMidYMid meet"
                >
                  <rect
                    x="0"
                    y="0"
                    width={mapProjection.width}
                    height={mapProjection.height}
                    fill="#f8fafc"
                    stroke="#e2e8f0"
                    rx="10"
                  />
                  {mapPoints.map((point) => {
                    const projected = mapProjection.project(point);
                    const color = CITY_COLOR_MAP[point.city] || '#64748b';
                    return (
                      <g key={point.city}>
                        <circle
                          cx={projected.x}
                          cy={projected.y}
                          r={projected.r}
                          fill={color}
                          fillOpacity="0.4"
                          stroke={color}
                          strokeWidth="2"
                        />
                        <text
                          x={projected.x}
                          y={projected.y + 4}
                          textAnchor="middle"
                          style={{ fontSize: '11px', fontWeight: 700, fill: '#0f172a' }}
                        >
                          {point.city}
                        </text>
                      </g>
                    );
                  })}
                </svg>
              </div>
            ) : (
              <p className="overview-muted">Map coordinates are unavailable for the selected cities.</p>
            )}

            <div className="ranking-two-col">
              <div className="rank-card">
                <h3>Top 3 (Highest)</h3>
                {topBottomRankings.top.map((row, index) => (
                  <div key={`top-${row.city}`} className="rank-row">
                    <span>#{index + 1} {row.city}</span>
                    <strong>{formatKpiValue(overviewMetricConfig.code, row.value)}</strong>
                  </div>
                ))}
              </div>
              <div className="rank-card">
                <h3>Bottom 3 (Lowest)</h3>
                {topBottomRankings.bottom.map((row, index) => (
                  <div key={`bottom-${row.city}`} className="rank-row">
                    <span>#{index + 1} {row.city}</span>
                    <strong>{formatKpiValue(overviewMetricConfig.code, row.value)}</strong>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="overview-block head-to-head-block">
            <h2>Head-to-Head Comparison</h2>
            <div className="head-to-head-controls">
              <div className="control-group">
                <label htmlFor="head-metric-select">Metric:</label>
                <select
                  id="head-metric-select"
                  value={headToHeadMetricConfig.code}
                  onChange={(event) => setHeadToHeadMetricCode(event.target.value)}
                >
                  {OVERVIEW_KPI_CONFIG.map((metric) => (
                    <option key={metric.code} value={metric.code}>
                      {metric.label}
                    </option>
                  ))}
                </select>
              </div>
              <div className="control-group">
                <label htmlFor="head-city-left">City A:</label>
                <select
                  id="head-city-left"
                  value={headToHeadLeftCity}
                  onChange={(event) => setHeadToHeadLeftCity(event.target.value)}
                >
                  {headToHeadCities.map((city) => (
                    <option key={`left-${city}`} value={city}>
                      {city}
                    </option>
                  ))}
                </select>
              </div>
              <div className="control-group">
                <label htmlFor="head-city-right">City B:</label>
                <select
                  id="head-city-right"
                  value={headToHeadRightCity}
                  onChange={(event) => setHeadToHeadRightCity(event.target.value)}
                >
                  {headToHeadCities.map((city) => (
                    <option key={`right-${city}`} value={city}>
                      {city}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="head-to-head-grid">
              <div className="head-city-card">
                <h3>{headToHeadLeftCity}</h3>
                <p>{formatKpiValue(headToHeadMetricConfig.code, headToHeadLeftValue)}</p>
              </div>
              <div className="head-city-card">
                <h3>{headToHeadRightCity}</h3>
                <p>{formatKpiValue(headToHeadMetricConfig.code, headToHeadRightValue)}</p>
              </div>
              <div className="head-summary-card">
                <h3>Difference</h3>
                {headToHeadDelta !== null ? (
                  <>
                    <p>{formatKpiValue(headToHeadMetricConfig.code, Math.abs(headToHeadDelta))}</p>
                    <span className={headToHeadDelta >= 0 ? 'kpi-up' : 'kpi-down'}>
                      {headToHeadLeftCity} {headToHeadDelta >= 0 ? 'higher' : 'lower'} by{' '}
                      {Math.abs(headToHeadDeltaPct ?? 0).toFixed(1)}%
                    </span>
                  </>
                ) : (
                  <p className="overview-muted">Not enough data for this comparison.</p>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {activeTab === 'demographics' &&
        renderGroupedBarSections(
          visibleDemographicsData,
          'demographics',
          `Demographics Comparison (${selectedYear})`,
          `No demographics data available for ${selectedYear} with current city filter.`
        )}

      {activeTab === 'labor' &&
        renderGroupedBarSections(
          visibleLaborMarketData,
          'labor',
          `Labor Market Comparison (${selectedYear})`,
          `No labor market data available for ${selectedYear} with current city filter.`
        )}

      {activeTab === 'business' &&
        renderGroupedBarSections(
          visibleBusinessEconomyData,
          'business',
          `Business & GDP Comparison (${selectedYear})`,
          `No business economy data available for ${selectedYear} with current city filter.`
        )}

      {activeTab === 'ict' &&
        ((ictSingleRegion || ictIsStateScope)
          ? renderIctKpiSections(
              visibleIctData,
              `ICT / Digitization Snapshot (${selectedYear})`,
              `No ICT or digitization data available for ${selectedYear} with available geographies.`,
              ictLeadingHint
            )
          : renderGroupedBarSections(
              visibleIctData,
              'ict',
              `ICT / Digitization Comparison (${selectedYear})`,
              `No ICT or digitization data available for ${selectedYear} with available geographies.`,
              ictLeadingHint,
              {
                xAxisLabel: 'Region',
                highlightCity: null,
                colorMap: null,
              }
            ))}

      {activeTab === 'finance' &&
        renderGroupedBarSections(
          visiblePublicFinanceData,
          'finance',
          `Public Finance Comparison (${selectedYear})`,
          `No public finance data available for ${selectedYear} with current city filter.`,
          publicFinanceUsesFallbackYear ? (
            <p className="data-hint">
              Some indicators are shown at their latest available year up to {selectedYear}.
            </p>
          ) : null
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
                maxFractionDigits={shouldNormalizeTrends ? 2 : (visibleTimeSeriesData[0]?.unit_of_measure?.match(/prozent|percent|%/i) ? 1 : 0)}
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
                maxFractionDigits={shouldNormalizeCategories ? 2 : (categoryData[0]?.unit_of_measure?.match(/prozent|percent|%/i) ? 1 : 0)}
              />
            )}
          </div>
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
