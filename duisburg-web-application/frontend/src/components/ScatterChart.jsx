import { useEffect, useRef } from 'react';
import * as d3 from 'd3';
import { DEFAULT_SERIES_COLORS } from '../constants/cityColors';
import ChartToolbar from './ChartToolbar';
import {
  downloadCsvRows,
  downloadPngFromSvg,
  downloadSvgElement,
  sanitizeFilename,
} from '../utils/exportUtils';

const ScatterChart = ({
  data,
  title,
  xLabel,
  yLabel,
  highlightCity = 'Duisburg',
  colorMap = null,
}) => {
  const svgRef = useRef();
  const filenameBase = sanitizeFilename(title || 'scatter-chart');

  useEffect(() => {
    d3.select(svgRef.current).selectAll('*').remove();
    if (!data || data.length === 0) return;

    const cleanData = data.filter(
      (d) => d && d.city && Number.isFinite(d.x) && Number.isFinite(d.y)
    );
    if (cleanData.length === 0) return;

    const margin = { top: 44, right: 180, bottom: 72, left: 90 };
    const width = 980 - margin.left - margin.right;
    const height = 520 - margin.top - margin.bottom;

    const svg = d3
      .select(svgRef.current)
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    const xExtent = d3.extent(cleanData, (d) => d.x);
    const yExtent = d3.extent(cleanData, (d) => d.y);
    const xPad = Math.max((xExtent[1] - xExtent[0]) * 0.08, 1);
    const yPad = Math.max((yExtent[1] - yExtent[0]) * 0.08, 1);

    const x = d3
      .scaleLinear()
      .domain([xExtent[0] - xPad, xExtent[1] + xPad])
      .nice()
      .range([0, width]);

    const y = d3
      .scaleLinear()
      .domain([yExtent[0] - yPad, yExtent[1] + yPad])
      .nice()
      .range([height, 0]);

    const colorScale = d3
      .scaleOrdinal()
      .domain(cleanData.map((d) => d.city))
      .range(DEFAULT_SERIES_COLORS);

    const getColor = (city) => {
      if (colorMap && colorMap[city]) return colorMap[city];
      return colorScale(city);
    };

    svg
      .append('g')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(x))
      .style('font-size', '12px');

    svg.append('g').call(d3.axisLeft(y)).style('font-size', '12px');

    svg
      .append('g')
      .attr('class', 'grid')
      .call(d3.axisLeft(y).tickSize(-width).tickFormat(''))
      .selectAll('line')
      .style('stroke', '#e2e8f0')
      .style('stroke-dasharray', '2,2');

    svg
      .append('g')
      .attr('class', 'grid')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(x).tickSize(-height).tickFormat(''))
      .selectAll('line')
      .style('stroke', '#e2e8f0')
      .style('stroke-dasharray', '2,2');

    svg
      .append('text')
      .attr('x', width / 2)
      .attr('y', height + margin.bottom - 20)
      .attr('text-anchor', 'middle')
      .style('font-size', '14px')
      .text(xLabel);

    svg
      .append('text')
      .attr('transform', 'rotate(-90)')
      .attr('x', -height / 2)
      .attr('y', -margin.left + 22)
      .attr('text-anchor', 'middle')
      .style('font-size', '14px')
      .text(yLabel);

    svg
      .append('text')
      .attr('x', width / 2)
      .attr('y', -margin.top / 2)
      .attr('text-anchor', 'middle')
      .style('font-size', '18px')
      .style('font-weight', 'bold')
      .text(title);

    const tooltip = d3
      .select('body')
      .append('div')
      .style('position', 'absolute')
      .style('background-color', 'white')
      .style('border', '1px solid #ddd')
      .style('border-radius', '6px')
      .style('padding', '10px')
      .style('opacity', 0)
      .style('pointer-events', 'none')
      .style('font-size', '14px');

    svg
      .selectAll('circle.point')
      .data(cleanData)
      .enter()
      .append('circle')
      .attr('class', 'point')
      .attr('cx', (d) => x(d.x))
      .attr('cy', (d) => y(d.y))
      .attr('r', (d) => (highlightCity && d.city === highlightCity ? 7 : 5.5))
      .attr('fill', (d) => getColor(d.city))
      .attr('opacity', (d) => (highlightCity && d.city === highlightCity ? 1 : 0.84))
      .attr('stroke', '#0f172a')
      .attr('stroke-width', 0.5)
      .on('mouseover', function (event, d) {
        d3.select(this).attr('r', 8.5);
        tooltip.transition().duration(180).style('opacity', 0.95);
        tooltip
          .html(
            `<strong>${d.city}</strong><br/>${xLabel}: ${d.x.toLocaleString('de-DE')}<br/>${yLabel}: ${d.y.toLocaleString('de-DE')}`
          )
          .style('left', `${event.pageX + 12}px`)
          .style('top', `${event.pageY - 28}px`);
      })
      .on('mouseout', function (event, d) {
        d3.select(this).attr('r', highlightCity && d.city === highlightCity ? 7 : 5.5);
        tooltip.transition().duration(260).style('opacity', 0);
      });

    svg
      .selectAll('text.city-label')
      .data(cleanData)
      .enter()
      .append('text')
      .attr('class', 'city-label')
      .attr('x', (d) => x(d.x) + 8)
      .attr('y', (d) => y(d.y) - 8)
      .style('font-size', '11px')
      .style('font-weight', (d) => (highlightCity && d.city === highlightCity ? '700' : '500'))
      .style('fill', '#334155')
      .text((d) => d.city);

    const legendCities = cleanData.map((d) => d.city);
    const legend = svg
      .selectAll('.legend')
      .data(legendCities)
      .enter()
      .append('g')
      .attr('class', 'legend')
      .attr('transform', (d, i) => `translate(${width + 20},${i * 24})`);

    legend
      .append('rect')
      .attr('width', 14)
      .attr('height', 14)
      .style('fill', (d) => getColor(d));

    legend
      .append('text')
      .attr('x', 20)
      .attr('y', 7)
      .attr('dy', '.35em')
      .style('font-size', '12px')
      .style('font-weight', (d) => (d === highlightCity ? '700' : '500'))
      .text((d) => d);

    return () => {
      tooltip.remove();
    };
  }, [data, title, xLabel, yLabel, highlightCity, colorMap]);

  return (
    <div className="chart-figure">
      <ChartToolbar
        onDownloadPng={() => downloadPngFromSvg(svgRef.current, filenameBase)}
        onDownloadSvg={() => downloadSvgElement(svgRef.current, filenameBase)}
        onDownloadCsv={() =>
          downloadCsvRows(
            (data || []).map((row) => ({
              city: row.city,
              x: row.x,
              y: row.y,
            })),
            filenameBase
          )
        }
      />
      <svg ref={svgRef}></svg>
    </div>
  );
};

export default ScatterChart;
