"""
Fix unemployment indicator modeling:
1) Rename legacy indicator_id=18 to unemployment_persons.
2) Create/update unemployment_rate_percent metadata.
3) Backfill unemployment rate (%) facts from source table 13211-02-05-4.
"""

import argparse
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.extractors.regional_db.employment_extractor import EmploymentExtractor
from src.loaders.db_loader import DataLoader
from src.transformers.employment_transformer import EmploymentTransformer
from src.utils.logging import get_logger, setup_logging


setup_logging()
logger = get_logger(__name__)


def ensure_indicator_metadata(loader: DataLoader) -> tuple[int, int]:
    """Ensure unemployment persons/rate indicators are configured correctly."""
    with loader.db.get_connection() as conn:
        existing_persons = conn.execute(
            text("SELECT indicator_id FROM dim_indicator WHERE indicator_code = 'unemployment_persons'")
        ).fetchone()
        if existing_persons and int(existing_persons[0]) != 18:
            raise RuntimeError(
                f"Found unemployment_persons on indicator_id={existing_persons[0]}, "
                "expected indicator_id=18 for legacy data continuity."
            )

        id18_exists = conn.execute(
            text("SELECT indicator_id FROM dim_indicator WHERE indicator_id = 18")
        ).fetchone()
        if not id18_exists:
            raise RuntimeError("indicator_id=18 not found in dim_indicator.")

        conn.execute(
            text(
                """
                UPDATE dim_indicator
                SET indicator_code = 'unemployment_persons',
                    indicator_name = 'Arbeitslose (Jahresdurchschnitt)',
                    indicator_name_en = 'Unemployed Persons (Annual Average)',
                    indicator_subcategory = 'unemployment',
                    unit_of_measure = 'persons',
                    unit_description = 'Number of unemployed persons',
                    description = 'Annual average number of unemployed persons by region.',
                    data_type = 'count',
                    aggregation_method = 'sum',
                    is_active = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE indicator_id = 18
                """
            )
        )

        # Ensure SERIAL sequence is aligned to avoid duplicate key on implicit indicator_id.
        conn.execute(
            text(
                """
                SELECT setval(
                    pg_get_serial_sequence('dim_indicator', 'indicator_id'),
                    COALESCE((SELECT MAX(indicator_id) FROM dim_indicator), 1),
                    true
                )
                """
            )
        )

        rate_row = conn.execute(
            text(
                """
                INSERT INTO dim_indicator (
                    indicator_code,
                    indicator_name,
                    indicator_name_en,
                    indicator_category,
                    indicator_subcategory,
                    source_system,
                    source_table_id,
                    unit_of_measure,
                    unit_description,
                    description,
                    update_frequency,
                    typical_reference_date,
                    data_type,
                    is_derived,
                    aggregation_method,
                    is_active
                )
                VALUES (
                    'unemployment_rate_percent',
                    'Arbeitslosenquote (Jahresdurchschnitt)',
                    'Unemployment Rate (Annual Average)',
                    'labor_market',
                    'unemployment',
                    'regional_db',
                    '13211-02-05-4',
                    'percent',
                    'Unemployment rate in percent',
                    'Annual average unemployment rate by region.',
                    'annual',
                    'Annual average',
                    'percentage',
                    FALSE,
                    'average',
                    TRUE
                )
                ON CONFLICT (indicator_code) DO UPDATE
                SET indicator_name = EXCLUDED.indicator_name,
                    indicator_name_en = EXCLUDED.indicator_name_en,
                    indicator_category = EXCLUDED.indicator_category,
                    indicator_subcategory = EXCLUDED.indicator_subcategory,
                    source_system = EXCLUDED.source_system,
                    source_table_id = EXCLUDED.source_table_id,
                    unit_of_measure = EXCLUDED.unit_of_measure,
                    unit_description = EXCLUDED.unit_description,
                    description = EXCLUDED.description,
                    update_frequency = EXCLUDED.update_frequency,
                    typical_reference_date = EXCLUDED.typical_reference_date,
                    data_type = EXCLUDED.data_type,
                    aggregation_method = EXCLUDED.aggregation_method,
                    is_active = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING indicator_id
                """
            )
        ).fetchone()

    persons_id = 18
    rate_id = int(rate_row[0])
    logger.info("Indicator metadata ready: unemployment_persons=%s, unemployment_rate_percent=%s", persons_id, rate_id)
    return persons_id, rate_id


def delete_existing_rate_rows(loader: DataLoader, rate_indicator_id: int, years: list[int]) -> int:
    years = sorted(set(int(y) for y in years))
    if not years:
        return 0
    year_csv = ",".join(str(y) for y in years)
    deleted = loader.db.execute_statement(
        f"""
        DELETE FROM fact_demographics fd
        USING dim_time t
        WHERE fd.time_id = t.time_id
          AND fd.indicator_id = {int(rate_indicator_id)}
          AND t.year IN ({year_csv})
        """
    )
    return deleted


def backfill_unemployment_rate(loader: DataLoader, persons_id: int, rate_id: int, years: list[int]) -> int:
    extractor = EmploymentExtractor()
    try:
        raw_data = extractor.extract_unemployment(years=years)
    finally:
        extractor.close()

    if raw_data is None or raw_data.empty:
        raise RuntimeError("No unemployment source data extracted.")

    transformer = EmploymentTransformer()
    transformed = transformer.transform_unemployment(
        raw_data,
        indicator_id={
            "persons_indicator_id": persons_id,
            "rate_indicator_id": rate_id,
        },
        years_filter=years,
    )

    if transformed is None or transformed.empty:
        raise RuntimeError("Unemployment transformation returned no rows.")

    rate_df = transformed[transformed["indicator_id"] == rate_id].copy()
    if rate_df.empty:
        raise RuntimeError("No unemployment_rate_percent rows found after transformation.")

    deleted = delete_existing_rate_rows(loader, rate_id, years)
    logger.info("Deleted %s existing unemployment rate rows before reload", deleted)

    loaded = loader.load_demographics_data(rate_df)
    logger.info("Loaded %s unemployment_rate_percent rows", loaded)
    return loaded


def main() -> int:
    parser = argparse.ArgumentParser(description="Fix and backfill unemployment persons/rate indicators.")
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument("--end-year", type=int, default=2024)
    args = parser.parse_args()

    years = list(range(args.start_year, args.end_year + 1))
    logger.info("Running unemployment indicator fix for years %s-%s", args.start_year, args.end_year)

    loader = DataLoader()
    try:
        persons_id, rate_id = ensure_indicator_metadata(loader)
        loaded = backfill_unemployment_rate(loader, persons_id, rate_id, years)
        logger.info("✅ Completed unemployment indicator fix. Rate rows loaded: %s", loaded)
        return 0
    except Exception as exc:
        logger.error("❌ Unemployment indicator fix failed: %s", exc)
        return 1
    finally:
        loader.close()


if __name__ == "__main__":
    raise SystemExit(main())
