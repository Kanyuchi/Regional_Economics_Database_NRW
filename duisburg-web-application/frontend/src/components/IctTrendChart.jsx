import { useEffect, useRef } from 'react';
import * as d3 from 'd3';
import ChartToolbar from './ChartToolbar';
import {
  downloadCsvRows,
  downloadPngFromSvg,
  downloadSvgElement,
  sanitizeFilename,
} from '../utils/exportUtils';

// yearData: { 2020: [...rows], 2021: [...rows], ..., 2024: [...rows] }
// Each row has indicator_name and value (string or number)

const getDotColor = (v) => {
  if (v >= 75) return '#10b981';
  if (v >= 50) return '#3b82f6';
  if (v >= 25) return '#f59e0b';
  return '#ef4444';
};

const abbreviate = (name, max = 46) =>
  name.length > max ? `${name.slice(0, max - 1)}\u2026` : name;

const IctTrendChart = ({ yearData }) => {
  const svgRef = useRef();
  const filenameBase = sanitizeFilename('ict-trends');

  useEffect(() => {
    d3.select(svgRef.current).selectAll('*').remove();
    if (!yearData || Object.keys(yearData).length === 0) return;

    const years = Object.keys(yearData)
      .map(Number)
      .sort((a, b) => a - b);

    if (years.length < 2) return;

    // Build indicator → year → value map
    const indicatorMap = {};
    years.forEach((yr) => {
      (yearData[yr] || []).forEach((row) => {
        const name = row.indicator_name;
        if (!name) return;
        const v = parseFloat(row.value);
        if (!Number.isFinite(v)) return;
        if (!indicatorMap[name]) indicatorMap[name] = {};
        indicatorMap[name][yr] = v;
      });
    });

    // Keep only indicators present in ≥ 2 years
    const indicators = Object.entries(indicatorMap)
      .filter(([, yrMap]) => Object.keys(yrMap).length >= 2)
      .map(([name, yrMap]) => ({ name, yrMap }));

    if (indicators.length === 0) return;

    // Sort by latest available year value (descending)
    const latestYear = years[years.length - 1];
    indicators.sort((a, b) => {
      const av = a.yrMap[latestYear] ?? a.yrMap[Math.max(...Object.keys(a.yrMap).map(Number))];
      const bv = b.yrMap[latestYear] ?? b.yrMap[Math.max(...Object.keys(b.yrMap).map(Number))];
      return bv - av;
    });

    const margin = { top: 50, right: 72, bottom: 44, left: 300 };
    const rowH = 30;
    const innerH = indicators.length * rowH;
    const innerW = 560;

    const totalW = innerW + margin.left + margin.right;
    const totalH = innerH + margin.top + margin.bottom;

    const svg = d3
      .select(svgRef.current)
      .attr('width', totalW)
      .attr('height', totalH)
      .attr('viewBox', `0 0 ${totalW} ${totalH}`)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // X scale: years along the horizontal axis
    const x = d3
      .scalePoint()
      .domain(years)
      .range([0, innerW])
      .padding(0.2);

    // Y scale: indicator rows
    const y = d3
      .scaleBand()
      .domain(indicators.map((d) => d.name))
      .range([0, innerH])
      .padding(0.15);

    // ── Subtle alternating row shading ───────────────────────
    indicators.forEach((ind, i) => {
      if (i % 2 === 0) {
        svg
          .append('rect')
          .attr('x', -margin.left + 4)
          .attr('width', innerW + margin.left - 4)
          .attr('y', y(ind.name) ?? 0)
          .attr('height', y.bandwidth())
          .attr('fill', '#f8fafc')
          .attr('rx', 2);
      }
    });

    // ── Vertical year gridlines ───────────────────────────────
    years.forEach((yr) => {
      svg
        .append('line')
        .attr('x1', x(yr) ?? 0)
        .attr('x2', x(yr) ?? 0)
        .attr('y1', 0)
        .attr('y2', innerH)
        .attr('stroke', '#e2e8f0')
        .attr('stroke-width', 1);
    });

    // ── Top axis: years ──────────────────────────────────────
    svg
      .append('g')
      .call(d3.axisTop(x).tickFormat((d) => `${d}`))
      .style('font-size', '12px')
      .style('font-weight', '600')
      .select('.domain')
      .remove();

    // ── Left axis: indicator names ───────────────────────────
    svg
      .append('g')
      .call(d3.axisLeft(y).tickFormat((name) => abbreviate(name)))
      .style('font-size', '11px')
      .selectAll('.tick text')
      .style('text-anchor', 'end');

    // ── Tooltip ──────────────────────────────────────────────
    const tooltip = d3
      .select('body')
      .append('div')
      .style('position', 'absolute')
      .style('background', 'white')
      .style('border', '1px solid #e2e8f0')
      .style('border-radius', '8px')
      .style('padding', '10px 14px')
      .style('font-size', '13px')
      .style('line-height', '1.6')
      .style('opacity', 0)
      .style('pointer-events', 'none')
      .style('max-width', '360px')
      .style('box-shadow', '0 4px 16px rgba(0,0,0,0.12)');

    // ── Connection lines between years ───────────────────────
    indicators.forEach((ind) => {
      const cy = (y(ind.name) ?? 0) + y.bandwidth() / 2;
      const pts = years.filter((yr) => ind.yrMap[yr] != null);

      for (let i = 0; i < pts.length - 1; i++) {
        const y1val = ind.yrMap[pts[i]];
        const y2val = ind.yrMap[pts[i + 1]];
        const color = getDotColor((y1val + y2val) / 2);
        svg
          .append('line')
          .attr('x1', x(pts[i]) ?? 0)
          .attr('x2', x(pts[i + 1]) ?? 0)
          .attr('y1', cy)
          .attr('y2', cy)
          .attr('stroke', color)
          .attr('stroke-width', 1.5)
          .attr('opacity', 0.4);
      }
    });

    // ── Dots ─────────────────────────────────────────────────
    const dotData = [];
    indicators.forEach((ind) => {
      years.forEach((yr) => {
        if (ind.yrMap[yr] != null) {
          dotData.push({ name: ind.name, year: yr, value: ind.yrMap[yr], yrMap: ind.yrMap });
        }
      });
    });

    svg
      .selectAll('circle.trend-dot')
      .data(dotData)
      .enter()
      .append('circle')
      .attr('class', 'trend-dot')
      .attr('cx', (d) => x(d.year) ?? 0)
      .attr('cy', (d) => (y(d.name) ?? 0) + y.bandwidth() / 2)
      .attr('r', 5)
      .attr('fill', (d) => getDotColor(d.value))
      .attr('stroke', 'white')
      .attr('stroke-width', 1.5)
      .style('cursor', 'pointer')
      .on('mouseover', function (event, d) {
        d3.select(this).attr('r', 8);
        // Build year-by-year summary
        const rows = years
          .filter((yr) => d.yrMap[yr] != null)
          .map((yr) => {
            const v = d.yrMap[yr];
            const highlight = yr === d.year ? 'font-weight:700' : 'color:#64748b';
            return `<tr><td style="${highlight};padding-right:8px">${yr}</td><td style="${highlight};color:${getDotColor(v)}">${v.toFixed(1)} %</td></tr>`;
          })
          .join('');
        tooltip.transition().duration(120).style('opacity', 1);
        tooltip
          .html(
            `<strong style="font-size:13px">${d.name}</strong><br/>` +
              `<table style="margin-top:6px;font-size:12px;border-collapse:collapse">${rows}</table>`
          )
          .style('left', `${event.pageX + 16}px`)
          .style('top', `${event.pageY - 40}px`);
      })
      .on('mouseout', function () {
        d3.select(this).attr('r', 5);
        tooltip.transition().duration(200).style('opacity', 0);
      });

    // ── Value labels (only for first and last year) ──────────
    // Both labels sit to the RIGHT of their dot so they stay inside the
    // chart area and never collide with the y-axis tick labels.
    dotData
      .filter((d) => d.year === years[0] || d.year === latestYear)
      .forEach((d) => {
        svg
          .append('text')
          .attr('x', (x(d.year) ?? 0) + 10)
          .attr('y', (y(d.name) ?? 0) + y.bandwidth() / 2)
          .attr('dy', '0.35em')
          .attr('text-anchor', 'start')
          .style('font-size', '9px')
          .style('fill', getDotColor(d.value))
          .style('font-weight', '500')
          .text(`${d.value.toFixed(0)}%`);
      });

    return () => {
      tooltip.remove();
    };
  }, [yearData]);

  // Flatten for CSV export
  const csvRows = Object.entries(yearData || {}).flatMap(([yr, rows]) =>
    (rows || []).map((r) => ({
      year: yr,
      indicator: r.indicator_name,
      value: r.value,
      unit: r.unit_of_measure,
      region: r.region_name,
    }))
  );

  return (
    <div className="chart-figure">
      <ChartToolbar
        onDownloadPng={() => downloadPngFromSvg(svgRef.current, filenameBase)}
        onDownloadSvg={() => downloadSvgElement(svgRef.current, filenameBase)}
        onDownloadCsv={() => downloadCsvRows(csvRows, filenameBase)}
      />
      <svg ref={svgRef} />
    </div>
  );
};

export default IctTrendChart;
