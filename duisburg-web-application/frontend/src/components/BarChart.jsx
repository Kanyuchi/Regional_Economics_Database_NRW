import { useEffect, useRef } from 'react';
import * as d3 from 'd3';

const BarChart = ({ data, title, xLabel, yLabel, highlightCity = 'Duisburg' }) => {
  const svgRef = useRef();

  useEffect(() => {
    if (!data || data.length === 0) return;

    // Clear previous chart
    d3.select(svgRef.current).selectAll('*').remove();

    // Set dimensions
    const margin = { top: 40, right: 30, bottom: 80, left: 70 };
    const width = 800 - margin.left - margin.right;
    const height = 400 - margin.top - margin.bottom;

    // Create SVG
    const svg = d3
      .select(svgRef.current)
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // Create scales
    const x = d3
      .scaleBand()
      .domain(data.map((d) => d.city))
      .range([0, width])
      .padding(0.3);

    const y = d3
      .scaleLinear()
      .domain([0, d3.max(data, (d) => d.value) * 1.1])
      .range([height, 0]);

    // Add X axis
    svg
      .append('g')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(x))
      .selectAll('text')
      .attr('transform', 'rotate(-45)')
      .style('text-anchor', 'end')
      .style('font-size', '12px');

    // Add Y axis
    svg.append('g').call(d3.axisLeft(y));

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

    // Add bars with tooltip
    const tooltip = d3
      .select('body')
      .append('div')
      .style('position', 'absolute')
      .style('background-color', 'white')
      .style('border', '1px solid #ddd')
      .style('border-radius', '4px')
      .style('padding', '10px')
      .style('opacity', 0)
      .style('pointer-events', 'none')
      .style('font-size', '14px');

    svg
      .selectAll('rect')
      .data(data)
      .enter()
      .append('rect')
      .attr('x', (d) => x(d.city))
      .attr('y', height)
      .attr('width', x.bandwidth())
      .attr('height', 0)
      .attr('fill', (d) => (d.city === highlightCity ? '#2563eb' : '#64748b'))
      .attr('rx', 4)
      .on('mouseover', function (event, d) {
        d3.select(this).attr('opacity', 0.7);
        tooltip.transition().duration(200).style('opacity', 0.9);
        tooltip
          .html(
            `<strong>${d.city}</strong><br/>${yLabel}: ${d.value.toLocaleString()}`
          )
          .style('left', event.pageX + 10 + 'px')
          .style('top', event.pageY - 28 + 'px');
      })
      .on('mouseout', function () {
        d3.select(this).attr('opacity', 1);
        tooltip.transition().duration(500).style('opacity', 0);
      })
      .transition()
      .duration(800)
      .attr('y', (d) => y(d.value))
      .attr('height', (d) => height - y(d.value));

    return () => {
      tooltip.remove();
    };
  }, [data, title, xLabel, yLabel, highlightCity]);

  return <svg ref={svgRef}></svg>;
};

export default BarChart;
