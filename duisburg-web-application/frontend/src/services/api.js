import axios from 'axios';

const API_BASE_URL = 'http://localhost:3001/api';

const api = axios.create({
  baseURL: API_BASE_URL,
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
};

export default apiService;
