import axios from 'axios';

// Configure API base via env; fallback to window origin
const API_BASE =
  import.meta.env.VITE_API_BASE ||
  (typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3001');

const api = axios.create({
  baseURL: `${API_BASE.replace(/\/$/, '')}/api`,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const apiService = {
  // Health check
  healthCheck: () => api.get('/health'),

  // Get cities
  getCities: () => api.get('/cities'),

  // Get Duisburg info
  getDuisburg: () => api.get('/duisburg'),

  // Get demographics data
  getDemographics: (year = 2023) => api.get(`/demographics/${year}`),

  // Get labor market data
  getLaborMarket: (year = 2023) => api.get(`/labor-market/${year}`),

  // Get business economy data
  getBusinessEconomy: (year = 2023) => api.get(`/business-economy/${year}`),

  // Get public finance data
  getPublicFinance: (year = 2023) => api.get(`/public-finance/${year}`),

  // Get time series data
  getTimeSeries: (indicatorCode, params = {}) =>
    api.get(`/timeseries/${indicatorCode}`, { params }),

  // Get available indicators
  getIndicators: () => api.get('/indicators'),

  // Get available years
  getYears: () => api.get('/years'),

  // Get indicator metadata (includes year ranges)
  getIndicatorMetadata: () => api.get('/indicator-metadata'),

  // Get available years for a specific indicator
  getIndicatorYears: (indicatorCode) => api.get(`/indicator-years/${indicatorCode}`),

  // Get category breakdown for indicators (e.g., business types)
  getCategoryBreakdown: (indicatorCode, params = {}) =>
    api.get(`/timeseries/${indicatorCode}/categories`, { params }),
};

export default apiService;
