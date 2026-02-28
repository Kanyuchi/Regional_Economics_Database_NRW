"""
Dashboard Statistics for CV/Portfolio
Duisburg Economic Dashboard

Generates accurate statistics about the web application for CV purposes.
"""

import os
from pathlib import Path
import json

PROJECT_ROOT = Path(__file__).parent.parent.parent
FRONTEND_DIR = PROJECT_ROOT / "duisburg-web-application" / "frontend"
BACKEND_DIR = PROJECT_ROOT / "duisburg-web-application" / "backend"


def count_code_files(directory, extensions):
    """Count files with specific extensions."""
    count = 0
    for ext in extensions:
        count += len(list(directory.rglob(f"*.{ext}")))
    return count


def count_lines_of_code(directory, extensions):
    """Count total lines of code."""
    total_lines = 0
    for ext in extensions:
        for file in directory.rglob(f"*.{ext}"):
            # Skip node_modules and build directories
            if 'node_modules' in str(file) or 'dist' in str(file) or 'build' in str(file):
                continue
            try:
                with open(file, 'r', encoding='utf-8', errors='ignore') as f:
                    total_lines += len(f.readlines())
            except:
                pass
    return total_lines


def count_api_endpoints(backend_dir):
    """Count API endpoints in server.js."""
    server_file = backend_dir / "server.js"
    endpoints = []

    if server_file.exists():
        with open(server_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Count app.get, app.post, app.put, app.delete
        import re
        patterns = [
            r"app\.get\(['\"]([^'\"]+)",
            r"app\.post\(['\"]([^'\"]+)",
            r"app\.put\(['\"]([^'\"]+)",
            r"app\.delete\(['\"]([^'\"]+)"
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content)
            endpoints.extend(matches)

    return endpoints


def analyze_frontend_components():
    """Analyze React components."""
    components_dir = FRONTEND_DIR / "src" / "components"
    components = []

    if components_dir.exists():
        for file in components_dir.glob("*.jsx"):
            components.append(file.stem)

    return components


def read_package_json(directory):
    """Read package.json for dependencies."""
    package_file = directory / "package.json"
    if package_file.exists():
        with open(package_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def main():
    print("\n" + "="*100)
    print("DUISBURG ECONOMIC DASHBOARD - COMPREHENSIVE STATISTICS")
    print("="*100)

    # Frontend analysis
    print("\n📱 FRONTEND (React + Vite + D3.js)")
    print("-" * 80)

    frontend_pkg = read_package_json(FRONTEND_DIR)
    if frontend_pkg:
        print(f"  Technology Stack:")
        deps = frontend_pkg.get('dependencies', {})
        dev_deps = frontend_pkg.get('devDependencies', {})

        print(f"    - React: {deps.get('react', 'N/A')}")
        print(f"    - D3.js: {deps.get('d3', 'N/A')}")
        print(f"    - Vite: {dev_deps.get('vite', 'N/A')}")
        print(f"    - Axios: {deps.get('axios', 'N/A')}")

    components = analyze_frontend_components()
    print(f"\n  Components: {len(components)}")
    for comp in components:
        print(f"    - {comp}")

    frontend_files = count_code_files(FRONTEND_DIR, ['jsx', 'js', 'css'])
    frontend_loc = count_lines_of_code(FRONTEND_DIR, ['jsx', 'js', 'css'])
    print(f"\n  Code Statistics:")
    print(f"    - Files: {frontend_files}")
    print(f"    - Lines of Code: {frontend_loc:,}")

    # Backend analysis
    print("\n\n🔧 BACKEND (Node.js + Express + PostgreSQL)")
    print("-" * 80)

    backend_pkg = read_package_json(BACKEND_DIR)
    if backend_pkg:
        print(f"  Technology Stack:")
        deps = backend_pkg.get('dependencies', {})

        print(f"    - Express: {deps.get('express', 'N/A')}")
        print(f"    - PostgreSQL (pg): {deps.get('pg', 'N/A')}")
        print(f"    - OpenAI: {deps.get('openai', 'N/A')}")
        print(f"    - CORS: {deps.get('cors', 'N/A')}")
        print(f"    - MCP Server: {deps.get('@modelcontextprotocol/server-postgres', 'N/A')}")

    endpoints = count_api_endpoints(BACKEND_DIR)
    print(f"\n  API Endpoints: {len(endpoints)}")

    # Group by category
    endpoint_categories = {
        'Data': [],
        'Chat': [],
        'Metadata': [],
        'Health': []
    }

    for ep in endpoints:
        if '/chat' in ep:
            endpoint_categories['Chat'].append(ep)
        elif '/health' in ep:
            endpoint_categories['Health'].append(ep)
        elif any(x in ep for x in ['/indicators', '/years', '/metadata']):
            endpoint_categories['Metadata'].append(ep)
        else:
            endpoint_categories['Data'].append(ep)

    for category, eps in endpoint_categories.items():
        if eps:
            print(f"    {category}: {len(eps)} endpoints")
            for ep in eps[:3]:  # Show first 3
                print(f"      - {ep}")
            if len(eps) > 3:
                print(f"      - ... and {len(eps) - 3} more")

    backend_files = count_code_files(BACKEND_DIR, ['js'])
    backend_loc = count_lines_of_code(BACKEND_DIR, ['js'])
    print(f"\n  Code Statistics:")
    print(f"    - Files: {backend_files}")
    print(f"    - Lines of Code: {backend_loc:,}")

    # Features
    print("\n\n✨ KEY FEATURES")
    print("-" * 80)
    print("  Data Visualization:")
    print("    ✓ Interactive Line Charts (D3.js)")
    print("    ✓ Area Charts with city highlighting")
    print("    ✓ Bar Charts for city comparisons")
    print("    ✓ Data Table view with export")
    print()
    print("  Dashboard Tabs:")
    print("    ✓ Overview - City information and context")
    print("    ✓ Demographics - Population and demographic indicators")
    print("    ✓ Labor Market - Employment and unemployment data")
    print("    ✓ Trends - Historical time series analysis")
    print()
    print("  Advanced Features:")
    print("    ✓ AI Chatbot (OpenAI GPT-4o-mini integration)")
    print("    ✓ Context-aware responses with chart data")
    print("    ✓ Dynamic year and indicator selection")
    print("    ✓ Category breakdown views")
    print("    ✓ City comparison mode")
    print("    ✓ Responsive design")
    print()
    print("  AI/ML Integration:")
    print("    ✓ Natural language queries")
    print("    ✓ Database-grounded responses (no hallucinations)")
    print("    ✓ Automatic data retrieval based on user questions")
    print("    ✓ Context-aware conversations with UI state")

    # Total statistics
    total_files = frontend_files + backend_files
    total_loc = frontend_loc + backend_loc

    print("\n\n📊 TOTAL PROJECT STATISTICS")
    print("-" * 80)
    print(f"  Total Files: {total_files}")
    print(f"  Total Lines of Code: {total_loc:,}")
    print(f"  API Endpoints: {len(endpoints)}")
    print(f"  React Components: {len(components)}")
    print(f"  Visualization Types: 4 (Line, Area, Bar, Table)")
    print(f"  Dashboard Tabs: 4 (Overview, Demographics, Labor, Trends)")
    print(f"  Database Indicators: 103+")
    print(f"  Years Coverage: 1975-2024 (50 years)")
    print(f"  Cities: 8 NRW urban districts")

    # Generate CV statements
    print("\n\n" + "="*100)
    print("📝 CV-READY STATEMENTS")
    print("="*100)

    print(f"""
Option 1 (Full-stack emphasis):
    Developed full-stack Economic Dashboard (React + D3.js + Node.js + PostgreSQL) with {len(endpoints)}
    REST API endpoints, {len(components)} interactive components, and AI chatbot enabling natural language
    queries over 481K+ records spanning 50 years (1975-2024)

Option 2 (Features emphasis):
    Built interactive Economic Dashboard with 4 visualization types (D3.js), AI-powered chatbot (OpenAI),
    and real-time data analysis across 103 indicators for 8 NRW cities with {total_loc:,} lines of code

Option 3 (Technical depth):
    Engineered full-stack web application: React 19 frontend with D3.js visualizations, Express.js
    REST API ({len(endpoints)} endpoints), PostgreSQL integration, and GPT-4-powered conversational interface
    for economic data exploration

Option 4 (Impact-focused):
    Created data visualization platform enabling policy stakeholders to explore 50 years of economic trends
    across 8 cities through interactive charts, natural language queries, and AI-assisted analysis

Option 5 (AI/ML emphasis):
    Developed AI-powered Economic Dashboard integrating GPT-4 for context-aware natural language queries,
    database-grounded responses preventing hallucinations, and automatic time-series retrieval
    across 103 socioeconomic indicators

Option 6 (Concise - 2 lines):
    • Built full-stack Economic Dashboard (React + D3.js + Express + PostgreSQL) with AI chatbot,
      {len(endpoints)} REST endpoints, 4 visualization types covering 481K+ records (1975-2024)
    """)

    print("\n" + "="*100)
    print("💡 RECOMMENDED COMBINED BULLETS (2-3 bullets for CV)")
    print("="*100)

    print(f"""
    • Engineered Regional Economics Database with 103 indicators spanning 50 years (481,575+ records)
      across 61 NRW regions, enabling data-driven urban economic policy analysis

    • Developed full-stack Economic Dashboard (React + D3.js + Express + PostgreSQL) with {len(endpoints)} REST endpoints,
      4 interactive visualization types, and AI-powered chatbot (GPT-4) enabling natural language queries
      over historical trends (1975-2024)

    OR (if space for 3 bullets):

    • Engineered Regional Economics Database with 103 indicators spanning 50 years (481,575+ records)
      across 61 NRW regions integrating 3 government APIs with automated ETL pipelines

    • Built full-stack Economic Dashboard (React + D3.js + Node.js + PostgreSQL) with {len(endpoints)} REST endpoints
      and 4 visualization types enabling interactive exploration of demographics, labor, and business data

    • Integrated GPT-4-powered conversational interface with context-aware responses, database grounding
      to prevent hallucinations, and automatic time-series retrieval based on natural language queries
    """)

    print("\n" + "="*100)
    print("🎯 TECHNICAL SKILLS TO HIGHLIGHT")
    print("="*100)
    print("""
    Frontend:
      • React 19, JavaScript/ES6+
      • D3.js (data visualization)
      • Vite (build tool)
      • Axios (HTTP client)
      • Responsive CSS

    Backend:
      • Node.js + Express.js
      • RESTful API design
      • PostgreSQL + pg driver
      • CORS configuration
      • MCP (Model Context Protocol)

    AI/ML:
      • OpenAI GPT-4 integration
      • Natural language processing
      • Context-aware AI responses
      • Database-grounded generation
      • Prompt engineering

    Data & Database:
      • PostgreSQL (dimensional modeling)
      • Complex SQL queries
      • Time-series analysis
      • Data aggregation & grouping
      • Star schema design

    DevOps/Deployment:
      • Environment configuration
      • API versioning
      • Error handling & logging
      • Production-ready CORS
      • Health check endpoints
    """)

    print("\n" + "="*100)


if __name__ == "__main__":
    main()
