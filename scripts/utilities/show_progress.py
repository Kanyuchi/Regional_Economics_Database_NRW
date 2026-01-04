"""
Show ETL Progress
==================
Displays the current status of all table ETL pipelines.

Usage:
    python scripts/show_progress.py
"""

import json
from pathlib import Path
from datetime import datetime

def load_registry():
    """Load the table registry."""
    registry_path = Path(__file__).parent.parent / "data" / "reference" / "table_registry.json"
    with open(registry_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def print_progress():
    """Print formatted progress report."""
    registry = load_registry()
    
    print("=" * 70)
    print("REGIONAL ECONOMICS DATABASE - ETL PROGRESS")
    print(f"Last Updated: {registry['metadata']['last_updated']}")
    print("=" * 70)
    
    summary = registry['summary']
    completed = summary['completed']
    total = summary['total_tables']
    pct = (completed / total) * 100
    
    print(f"\nOVERALL PROGRESS: {completed}/{total} tables ({pct:.1f}%)")
    print("-" * 40)
    
    # Progress bar (ASCII compatible)
    bar_width = 40
    filled = int(bar_width * completed / total)
    bar = "#" * filled + "-" * (bar_width - filled)
    print(f"[{bar}]")
    
    # By source
    print(f"\nBY DATA SOURCE:")
    for source, stats in summary['by_source'].items():
        src_pct = (stats['completed'] / stats['total']) * 100 if stats['total'] > 0 else 0
        status = "[DONE]" if stats['completed'] == stats['total'] else "[WIP]"
        print(f"  {status} {source}: {stats['completed']}/{stats['total']} ({src_pct:.0f}%)")
    
    # Completed tables
    print(f"\n{'='*70}")
    print("COMPLETED TABLES")
    print("-" * 70)
    
    for source, tables in registry['tables'].items():
        for table in tables:
            if isinstance(table, dict) and table.get('status') == 'completed':
                print(f"  [OK] [{source}] {table['table_id']}")
                print(f"     {table['description'][:60]}...")
                print(f"     Records: {table.get('records_loaded', 'N/A'):,} | Completed: {table.get('completed_at', 'N/A')}")
                print(f"     Script: {table.get('etl_script', 'N/A')}")
                print()
    
    # Next table to process
    print(f"{'='*70}")
    print("NEXT TABLE TO PROCESS")
    print("-" * 70)
    
    for source, tables in registry['tables'].items():
        for table in tables:
            if isinstance(table, dict) and table.get('status') == 'pending':
                print(f"  >>> [{source}] {table['table_id']}")
                print(f"     {table['description']}")
                print(f"     Category: {table.get('category', 'N/A')}")
                if table.get('notes'):
                    print(f"     Notes: {table['notes']}")
                print()
                return  # Show only next one
    
    print("  All tables completed!")

def print_all_pending():
    """Print all pending tables."""
    registry = load_registry()
    
    print("\n" + "=" * 70)
    print("ALL PENDING TABLES")
    print("=" * 70)
    
    for source, tables in registry['tables'].items():
        pending = [t for t in tables if isinstance(t, dict) and t.get('status') == 'pending']
        if pending:
            print(f"\n{source.upper()} ({len(pending)} pending)")
            print("-" * 40)
            for i, table in enumerate(pending, 1):
                print(f"  {i:2}. {table['table_id']}: {table['description'][:50]}...")

if __name__ == "__main__":
    print_progress()
    print_all_pending()

