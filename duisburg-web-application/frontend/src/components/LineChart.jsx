import { useEffect, useRef } from 'react';
import * as d3 from 'd3';

const LineChart = ({ data, title, xLabel, yLabel, highlightCity = 'Duisburg' }) => {
  const svgRef = useRef();

  useEffect(() => {
    if (!data || data.length === 0) return;

    // Clear previous chart
    d3.select(svgRef.current).selectAll('*').remove();

    // Set dimensions
    const margin = { top: 40, right: 150, bottom: 60, left: 70 };
    const width = 900 - margin.left - margin.right;
    const height = 400 - margin.top - margin.bottom;

    // Create SVG
    const svg = d3
      .select(svgRef.current)
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // Group data by city
    const groupedData = d3.group(data, (d) => d.city);

    // Create scales
    const x = d3
      .scaleLinear()
      .domain(d3.extent(data, (d) => d.year))
      .range([0, width]);

    const y = d3
      .scaleLinear()
      .domain([
        d3.min(data, (d) => d.value) * 0.95,
        d3.max(data, (d) => d.value) * 1.05,
      ])
      .range([height, 0]);

    // Color scale
    const cities = Array.from(new Set(data.map((d) => d.city)));
    const colorScale = d3
      .scaleOrdinal()
      .domain(cities)
      .range(['#2563eb', '#64748b', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6']);

    // Add X axis
    svg
      .append('g')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(x).tickFormat(d3.format('d')))
      .style('font-size', '12px');

    // Add Y axis
    svg.append('g').call(d3.axisLeft(y)).style('font-size', '12px');

    // Add X axis label
    svg
      .append('text')
      .attr('text-anchor', 'middle')
      .attr('x', width / 2)
      .attr('y', height + margin.bottom - 10)
      .style('font-size', '14px')
      .text(xLabel);

    // Add Y axis label
    svg
      .append('text')
      .attr('text-anchor', 'middle')
      .attr('transform', 'rotate(-90)')
      .attr('y', -margin.left + 20)
      .attr('x', -height / 2)
      .style('font-size', '14px')
      .text(yLabel);

    // Add title
    svg
      .append('text')
      .attr('x', width / 2)
      .attr('y', -margin.top / 2)
      .attr('text-anchor', 'middle')
      .style('font-size', '18px')
      .style('font-weight', 'bold')
      .text(title);

    // Line generator
    const line = d3
      .line()
      .x((d) => x(d.year))
      .y((d) => y(d.value))
      .curve(d3.curveMonotoneX);

    // Add lines for each city
    groupedData.forEach((values, city) => {
      const sortedValues = values.sort((a, b) => a.year - b.year);

      svg
        .append('path')
        .datum(sortedValues)
        .attr('fill', 'none')
        .attr('stroke', colorScale(city))
        .attr('stroke-width', city === highlightCity ? 3 : 1.5)
        .attr('d', line)
        .attr('opacity', city === highlightCity ? 1 : 0.6);

      // Add dots for each data point
      svg
        .selectAll(`.dot-${city.replace(/\s+/g, '-')}`)
        .data(sortedValues)
        .enter()
        .append('circle')
        .attr('class', `dot-${city.replace(/\s+/g, '-')}`)
        .attr('cx', (d) => x(d.year))
        .attr('cy', (d) => y(d.value))
        .attr('r', city === highlightCity ? 4 : 3)
        .attr('fill', colorScale(city))
        .attr('opacity', city === highlightCity ? 1 : 0.6);
    });

    // Add legend
    const legend = svg
      .selectAll('.legend')
      .data(cities)
      .enter()
      .append('g')
      .attr('class', 'legend')
      .attr('transform', (d, i) => `translate(${width + 20},${i * 25})`);

    legend
      .append('rect')
      .attr('width', 18)
      .attr('height', 18)
      .style('fill', (d) => colorScale(d))
      .attr('opacity', (d) => (d === highlightCity ? 1 : 0.6));

    legend
      .append('text')
      .attr('x', 24)
      .attr('y', 9)
      .attr('dy', '.35em')
      .style('text-anchor', 'start')
      .style('font-size', '12px')
      .style('font-weight', (d) => (d === highlightCity ? 'bold' : 'normal'))
      .text((d) => d);

  }, [data, title, xLabel, yLabel, highlightCity]);

  return <svg ref={svgRef}></svg>;
};

export default LineChart;
