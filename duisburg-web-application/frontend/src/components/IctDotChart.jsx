import { useEffect, useRef } from 'react';
import * as d3 from 'd3';
import ChartToolbar from './ChartToolbar';
import {
  downloadCsvRows,
  downloadPngFromSvg,
  downloadSvgElement,
  sanitizeFilename,
} from '../utils/exportUtils';

// Quartile config: background fill + dot color
const QUARTILES = [
  { min: 0,  max: 25,  fill: '#fef2f2', dot: '#ef4444', label: '0–25 %' },
  { min: 25, max: 50,  fill: '#fffbeb', dot: '#f59e0b', label: '25–50 %' },
  { min: 50, max: 75,  fill: '#eff6ff', dot: '#3b82f6', label: '50–75 %' },
  { min: 75, max: 100, fill: '#f0fdf4', dot: '#10b981', label: '75–100 %' },
];

const getDotColor = (v) => {
  if (v >= 75) return '#10b981';
  if (v >= 50) return '#3b82f6';
  if (v >= 25) return '#f59e0b';
  return '#ef4444';
};

const abbreviate = (name, max = 48) =>
  name.length > max ? `${name.slice(0, max - 1)}\u2026` : name;

const IctDotChart = ({ data, year }) => {
  const svgRef = useRef();
  const filenameBase = sanitizeFilename(`ict-indicators-${year ?? 'snapshot'}`);

  useEffect(() => {
    d3.select(svgRef.current).selectAll('*').remove();
    if (!data || data.length === 0) return;

    const clean = [...data]
      .filter((d) => d?.indicator_name && Number.isFinite(parseFloat(d.value)))
      .map((d) => ({ ...d, value: parseFloat(d.value) }))
      .sort((a, b) => a.value - b.value); // ascending → top of chart = highest

    if (clean.length === 0) return;

    const margin = { top: 50, right: 72, bottom: 44, left: 300 };
    const rowH = 30;
    const innerH = clean.length * rowH;
    const innerW = 580;

    const totalW = innerW + margin.left + margin.right;
    const totalH = innerH + margin.top + margin.bottom;

    const svg = d3
      .select(svgRef.current)
      .attr('width', totalW)
      .attr('height', totalH)
      .attr('viewBox', `0 0 ${totalW} ${totalH}`)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleLinear().domain([0, 100]).range([0, innerW]);
    const y = d3
      .scaleBand()
      .domain(clean.map((d) => d.indicator_name))
      .range([innerH, 0])
      .padding(0.18);

    // ── Quartile background bands ─────────────────────────────
    QUARTILES.forEach((q) => {
      svg
        .append('rect')
        .attr('x', x(q.min))
        .attr('width', x(q.max) - x(q.min))
        .attr('y', 0)
        .attr('height', innerH)
        .attr('fill', q.fill);
    });

    // ── Gridlines at quartile boundaries ─────────────────────
    [0, 25, 50, 75, 100].forEach((v) => {
      svg
        .append('line')
        .attr('x1', x(v))
        .attr('x2', x(v))
        .attr('y1', 0)
        .attr('y2', innerH)
        .attr('stroke', v === 0 || v === 100 ? '#94a3b8' : '#cbd5e1')
        .attr('stroke-width', v === 0 || v === 100 ? 1.5 : 1)
        .attr('stroke-dasharray', v === 0 || v === 100 ? null : '4,3');
    });

    // ── Axes ─────────────────────────────────────────────────
    svg
      .append('g')
      .attr('transform', `translate(0,${innerH})`)
      .call(
        d3
          .axisBottom(x)
          .ticks(5)
          .tickFormat((d) => `${d}%`)
      )
      .style('font-size', '11px');

    svg
      .append('g')
      .call(
        d3.axisLeft(y).tickFormat((name) => abbreviate(name))
      )
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
      .style('padding', '8px 12px')
      .style('font-size', '13px')
      .style('line-height', '1.5')
      .style('opacity', 0)
      .style('pointer-events', 'none')
      .style('max-width', '340px')
      .style('box-shadow', '0 4px 16px rgba(0,0,0,0.12)');

    // ── Lollipop stems ───────────────────────────────────────
    svg
      .selectAll('line.stem')
      .data(clean)
      .enter()
      .append('line')
      .attr('class', 'stem')
      .attr('x1', 0)
      .attr('x2', (d) => x(d.value))
      .attr('y1', (d) => (y(d.indicator_name) ?? 0) + y.bandwidth() / 2)
      .attr('y2', (d) => (y(d.indicator_name) ?? 0) + y.bandwidth() / 2)
      .attr('stroke', (d) => getDotColor(d.value))
      .attr('stroke-width', 2)
      .attr('opacity', 0.45);

    // ── Dots ─────────────────────────────────────────────────
    svg
      .selectAll('circle.dot')
      .data(clean)
      .enter()
      .append('circle')
      .attr('class', 'dot')
      .attr('cx', 0)
      .attr('cy', (d) => (y(d.indicator_name) ?? 0) + y.bandwidth() / 2)
      .attr('r', 7)
      .attr('fill', (d) => getDotColor(d.value))
      .attr('stroke', 'white')
      .attr('stroke-width', 2)
      .style('cursor', 'pointer')
      .on('mouseover', function (event, d) {
        d3.select(this).attr('r', 10);
        tooltip.transition().duration(120).style('opacity', 1);
        tooltip
          .html(
            `<strong style="font-size:13px">${d.indicator_name}</strong>` +
              `<br/><span style="font-size:17px;font-weight:700;color:${getDotColor(d.value)}">${d.value.toLocaleString('de-DE', { minimumFractionDigits: 1, maximumFractionDigits: 1 })} %</span>` +
              `<br/><span style="color:#64748b;font-size:11px">Nordrhein-Westfalen · ${d.year}</span>`
          )
          .style('left', `${event.pageX + 16}px`)
          .style('top', `${event.pageY - 36}px`);
      })
      .on('mouseout', function () {
        d3.select(this).attr('r', 7);
        tooltip.transition().duration(200).style('opacity', 0);
      })
      .transition()
      .duration(700)
      .delay((_, i) => i * 16)
      .attr('cx', (d) => x(d.value));

    // ── Value labels ─────────────────────────────────────────
    svg
      .selectAll('text.val')
      .data(clean)
      .enter()
      .append('text')
      .attr('class', 'val')
      .attr('x', (d) => x(d.value) + 12)
      .attr('y', (d) => (y(d.indicator_name) ?? 0) + y.bandwidth() / 2)
      .attr('dy', '0.35em')
      .style('font-size', '10px')
      .style('font-weight', '600')
      .style('fill', (d) => getDotColor(d.value))
      .text((d) => `${d.value.toFixed(1)}%`);

    // ── Legend ───────────────────────────────────────────────
    const lgd = svg.append('g').attr('transform', `translate(0,${-32})`);
    QUARTILES.forEach((q, i) => {
      const g = lgd.append('g').attr('transform', `translate(${i * 136},0)`);
      g.append('circle')
        .attr('r', 6)
        .attr('fill', q.dot)
        .attr('stroke', 'white')
        .attr('stroke-width', 1.5);
      g.append('text')
        .attr('x', 10)
        .attr('dy', '0.35em')
        .style('font-size', '11px')
        .style('fill', '#475569')
        .text(q.label);
    });

    return () => {
      tooltip.remove();
    };
  }, [data, year]);

  return (
    <div className="chart-figure">
      <ChartToolbar
        onDownloadPng={() => downloadPngFromSvg(svgRef.current, filenameBase)}
        onDownloadSvg={() => downloadSvgElement(svgRef.current, filenameBase)}
        onDownloadCsv={() =>
          downloadCsvRows(
            (data || []).map((r) => ({
              indicator: r.indicator_name,
              value: r.value,
              unit: r.unit_of_measure,
              year: r.year,
              region: r.region_name,
            })),
            filenameBase
          )
        }
      />
      <svg ref={svgRef} />
    </div>
  );
};

export default IctDotChart;
