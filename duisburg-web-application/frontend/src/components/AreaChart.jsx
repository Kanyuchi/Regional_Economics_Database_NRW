import { useEffect, useRef } from 'react';
import * as d3 from 'd3';

const AreaChart = ({ data, title, xLabel = 'X Axis', yLabel = 'Y Axis', highlightCity = null }) => {
  const svgRef = useRef();

  useEffect(() => {
    if (!data || data.length === 0) return;

    // Clear previous chart
    d3.select(svgRef.current).selectAll('*').remove();

    // Set up dimensions
    const margin = { top: 40, right: 150, bottom: 60, left: 80 };
    const width = 900 - margin.left - margin.right;
    const height = 500 - margin.top - margin.bottom;

    // Create SVG
    const svg = d3
      .select(svgRef.current)
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // Group data by city
    const groupedData = d3.group(data, (d) => d.city);
    const cities = Array.from(groupedData.keys());

    // Color scale
    const colorScale = d3
      .scaleOrdinal()
      .domain(cities)
      .range(d3.schemeCategory10);

    // Scales
    const xScale = d3
      .scaleLinear()
      .domain(d3.extent(data, (d) => d.year))
      .range([0, width]);

    const yScale = d3
      .scaleLinear()
      .domain([0, d3.max(data, (d) => d.value)])
      .nice()
      .range([height, 0]);

    // Area generator
    const area = d3
      .area()
      .x((d) => xScale(d.year))
      .y0(height)
      .y1((d) => yScale(d.value))
      .curve(d3.curveMonotoneX);

    // Line generator for border
    const line = d3
      .line()
      .x((d) => xScale(d.year))
      .y((d) => yScale(d.value))
      .curve(d3.curveMonotoneX);

    // Draw areas for each city
    cities.forEach((city) => {
      const cityData = groupedData.get(city).sort((a, b) => a.year - b.year);
      const isHighlighted = highlightCity && city === highlightCity;

      // Draw area
      svg
        .append('path')
        .datum(cityData)
        .attr('fill', colorScale(city))
        .attr('opacity', isHighlighted ? 0.7 : 0.4)
        .attr('d', area)
        .on('mouseover', function () {
          d3.select(this).attr('opacity', 0.8);
        })
        .on('mouseout', function () {
          d3.select(this).attr('opacity', isHighlighted ? 0.7 : 0.4);
        });

      // Draw border line
      svg
        .append('path')
        .datum(cityData)
        .attr('fill', 'none')
        .attr('stroke', colorScale(city))
        .attr('stroke-width', isHighlighted ? 3 : 2)
        .attr('d', line);
    });

    // Add axes
    const xAxis = d3.axisBottom(xScale).tickFormat(d3.format('d'));
    const yAxis = d3.axisLeft(yScale);

    svg
      .append('g')
      .attr('transform', `translate(0,${height})`)
      .call(xAxis)
      .append('text')
      .attr('x', width / 2)
      .attr('y', 40)
      .attr('fill', '#000')
      .attr('font-size', '14px')
      .attr('text-anchor', 'middle')
      .text(xLabel);

    svg
      .append('g')
      .call(yAxis)
      .append('text')
      .attr('transform', 'rotate(-90)')
      .attr('x', -height / 2)
      .attr('y', -50)
      .attr('fill', '#000')
      .attr('font-size', '14px')
      .attr('text-anchor', 'middle')
      .text(yLabel);

    // Add legend
    const legend = svg
      .append('g')
      .attr('transform', `translate(${width + 20}, 0)`);

    cities.forEach((city, i) => {
      const legendRow = legend
        .append('g')
        .attr('transform', `translate(0, ${i * 25})`);

      legendRow
        .append('rect')
        .attr('width', 15)
        .attr('height', 15)
        .attr('fill', colorScale(city))
        .attr('opacity', 0.6);

      legendRow
        .append('text')
        .attr('x', 20)
        .attr('y', 12)
        .attr('font-size', '12px')
        .attr('fill', '#333')
        .style('font-weight', highlightCity === city ? 'bold' : 'normal')
        .text(city);
    });

    // Add title
    svg
      .append('text')
      .attr('x', width / 2)
      .attr('y', -15)
      .attr('text-anchor', 'middle')
      .attr('font-size', '16px')
      .attr('font-weight', 'bold')
      .attr('fill', '#1e293b')
      .text(title);
  }, [data, title, xLabel, yLabel, highlightCity]);

  return (
    <div style={{ overflowX: 'auto' }}>
      <svg ref={svgRef}></svg>
    </div>
  );
};

export default AreaChart;
