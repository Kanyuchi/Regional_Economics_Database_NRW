import ChartToolbar from './ChartToolbar';
import { downloadCsvRows, sanitizeFilename } from '../utils/exportUtils';
import './DataTable.css';

const DataTable = ({ data, title, highlightCity = null, maxFractionDigits = 0 }) => {
  if (!data || data.length === 0) {
    return <div className="no-data">No data available</div>;
  }

  // Group data by city
  const cities = [...new Set(data.map((d) => d.city))];
  const years = [...new Set(data.map((d) => d.year))].sort((a, b) => a - b);

  // Create data matrix: cities x years
  const dataMatrix = {};
  cities.forEach((city) => {
    dataMatrix[city] = {};
    data
      .filter((d) => d.city === city)
      .forEach((d) => {
        dataMatrix[city][d.year] = d.value;
      });
  });

  // Format number with thousand separators
  const formatNumber = (num) => {
    if (num === null || num === undefined) return 'N/A';
    return new Intl.NumberFormat('de-DE', {
      minimumFractionDigits: 0,
      maximumFractionDigits: maxFractionDigits,
    }).format(num);
  };

  return (
    <div className="data-table-container">
      <div className="table-header">
        <h3 className="data-table-title">{title}</h3>
        <ChartToolbar
          compact
          onDownloadCsv={() => {
            const rows = cities.flatMap((city) =>
              years.map((year) => ({
                city,
                year,
                value: dataMatrix[city][year] ?? null,
              }))
            );
            downloadCsvRows(rows, sanitizeFilename(title || 'table-view'));
          }}
        />
      </div>
      <div className="table-wrapper">
        <table className="data-table">
          <thead>
            <tr>
              <th className="sticky-col">City / Year</th>
              {years.map((year) => (
                <th key={year}>{year}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {cities.map((city) => {
              const isHighlighted = highlightCity && city === highlightCity;
              return (
                <tr
                  key={city}
                  className={isHighlighted ? 'highlighted-row' : ''}
                >
                  <td className="sticky-col city-name">{city}</td>
                  {years.map((year) => (
                    <td key={`${city}-${year}`}>
                      {formatNumber(dataMatrix[city][year])}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default DataTable;
