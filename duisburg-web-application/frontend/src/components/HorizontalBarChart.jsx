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

const HorizontalBarChart = ({
  data,
  title,
  xLabel,
  yLabel,
  highlightCity = 'Duisburg',
  colorMap = null,
}) => {
  const svgRef = useRef();
  const filenameBase = sanitizeFilename(title || 'horizontal-bar-chart');

  useEffect(() => {
    d3.select(svgRef.current).selectAll('*').remove();
    if (!data || data.length === 0) return;

    const cleanData = data
      .filter((d) => d && d.city && Number.isFinite(d.value))
      .sort((a, b) => b.value - a.value);
    if (cleanData.length === 0) return;

    const margin = { top: 44, right: 70, bottom: 60, left: 250 };
    const width = 960 - margin.left - margin.right;
    const height = Math.max(320, cleanData.length * 44);

    const svg = d3
      .select(svgRef.current)
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    const maxValue = d3.max(cleanData, (d) => d.value) || 0;
    const x = d3
      .scaleLinear()
      .domain([0, maxValue > 0 ? maxValue * 1.1 : 1])
      .range([0, width]);

    const y = d3
      .scaleBand()
      .domain(cleanData.map((d) => d.city))
      .range([0, height])
      .padding(0.22);

    const colorScale = d3
      .scaleOrdinal()
      .domain(cleanData.map((d) => d.city))
      .range(DEFAULT_SERIES_COLORS);

    const getColor = (city) => {
      if (colorMap && colorMap[city]) return colorMap[city];
      if (!colorMap) {
        if (highlightCity && city === highlightCity) return '#2563eb';
        if (highlightCity) return '#64748b';
      }
      return colorScale(city);
    };

    svg
      .append('g')
      .call(d3.axisLeft(y))
      .style('font-size', '12px');

    svg
      .append('g')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(x))
      .style('font-size', '12px');

    svg
      .append('text')
      .attr('x', width / 2)
      .attr('y', height + margin.bottom - 12)
      .attr('text-anchor', 'middle')
      .style('font-size', '14px')
      .text(xLabel);

    svg
      .append('text')
      .attr('transform', 'rotate(-90)')
      .attr('x', -height / 2)
      .attr('y', -margin.left + 18)
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
      .selectAll('rect.bar')
      .data(cleanData)
      .enter()
      .append('rect')
      .attr('class', 'bar')
      .attr('x', 0)
      .attr('y', (d) => y(d.city))
      .attr('height', y.bandwidth())
      .attr('width', 0)
      .attr('rx', 4)
      .attr('fill', (d) => getColor(d.city))
      .on('mouseover', function (event, d) {
        d3.select(this).attr('opacity', 0.78);
        tooltip.transition().duration(200).style('opacity', 0.95);
        tooltip
          .html(`<strong>${d.city}</strong><br/>${xLabel}: ${d.value.toLocaleString('de-DE')}`)
          .style('left', `${event.pageX + 12}px`)
          .style('top', `${event.pageY - 28}px`);
      })
      .on('mouseout', function () {
        d3.select(this).attr('opacity', 1);
        tooltip.transition().duration(300).style('opacity', 0);
      })
      .transition()
      .duration(800)
      .attr('width', (d) => x(d.value));

    svg
      .selectAll('text.value-label')
      .data(cleanData)
      .enter()
      .append('text')
      .attr('class', 'value-label')
      .attr('x', (d) => x(d.value) + 8)
      .attr('y', (d) => (y(d.city) || 0) + y.bandwidth() / 2)
      .attr('dy', '0.35em')
      .style('font-size', '11px')
      .style('fill', '#334155')
      .text((d) => d.value.toLocaleString('de-DE'));

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
              value: row.value,
            })),
            filenameBase
          )
        }
      />
      <svg ref={svgRef}></svg>
    </div>
  );
};

export default HorizontalBarChart;
