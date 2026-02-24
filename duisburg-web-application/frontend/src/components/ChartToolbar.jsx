const ChartToolbar = ({
  onDownloadPng,
  onDownloadSvg,
  onDownloadCsv,
  compact = false,
}) => {
  return (
    <div className={`chart-toolbar ${compact ? 'compact' : ''}`}>
      {onDownloadPng ? (
        <button type="button" className="chart-tool-btn" onClick={onDownloadPng}>
          PNG
        </button>
      ) : null}
      {onDownloadSvg ? (
        <button type="button" className="chart-tool-btn" onClick={onDownloadSvg}>
          SVG
        </button>
      ) : null}
      {onDownloadCsv ? (
        <button type="button" className="chart-tool-btn" onClick={onDownloadCsv}>
          CSV
        </button>
      ) : null}
    </div>
  );
};

export default ChartToolbar;
