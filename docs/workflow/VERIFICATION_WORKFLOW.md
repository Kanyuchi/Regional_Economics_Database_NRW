# Data Verification Workflow
## Regional Economics Database for NRW

---

## Overview

Every data extraction MUST be followed by a comprehensive verification to ensure:
1. **Accuracy**: Data values are correct and reasonable
2. **Completeness**: All expected years and regions are present
3. **Queryability**: Data can be queried for time series analysis
4. **Ruhr Region Coverage**: All 5 key cities have complete data

---

## Verification Steps (After Every Extraction)

### Step 1: Run Verification Script

After extracting any table, immediately run:

```bash
python verify_extraction_timeseries.py --indicator <INDICATOR_ID>
```

**Example for indicator 19:**
```bash
python verify_extraction_timeseries.py --indicator 19
```

### Step 2: Review Verification Report

The script will output a comprehensive report checking:

#### A. Indicator Information
- Indicator ID, code, and name
- Category and source table
- Unit of measure

#### B. Data Completeness
- Total records count
- Year range coverage
- Number of regions covered
- NULL values percentage
- Quality status (EXCELLENT/GOOD/FAIR/POOR)

#### C. Ruhr Region Cities Coverage
- **Duisburg** (05112)
- **Bochum** (05911)
- **Essen** (05113)
- **Dortmund** (05913)
- **Gelsenkirchen** (05513)

Each city shows:
- Number of records
- Year range
- Status (OK/MISSING)

#### D. Time Series Analysis
For each Ruhr city:
- Data points (years with data)
- Mean value over time period
- First and last values
- Absolute and percentage change

#### E. Verification Summary
- List of passed checks
- List of failed checks
- Overall verdict (PASSED/WARNINGS/FAILED)

### Step 3: Export Time Series Data (Optional)

For detailed analysis or visualization:

```bash
python verify_extraction_timeseries.py --indicator <ID> --export-csv
```

This creates a CSV file in `data/analysis/timeseries/` with full time series data for all Ruhr cities.

**File naming:** `timeseries_indicator_<ID>_<TIMESTAMP>.csv`

---

## Verification Checklist

After running the verification script, confirm:

- [ ] **Records exist**: Total records > 0
- [ ] **High completeness**: >= 95% non-NULL values
- [ ] **All Ruhr cities covered**: All 5 cities show "OK" status
- [ ] **Sufficient time series**: >= 10 years of data
- [ ] **Reasonable values**: Min/max values make sense for the indicator
- [ ] **Year continuity**: No unexpected gaps in year coverage

---

## Example Verification Output

### Indicator 19: Employed Persons by Sector

```
====================================================================================================
 DATA EXTRACTION VERIFICATION REPORT
====================================================================================================

 INDICATOR INFORMATION
----------------------------------------------------------------------------------------------------
Indicator ID:      19
Indicator Code:    employed_by_sector_annual
Indicator Name:    Employed persons by economic sectors
Category:          labor_market
Source Table:      13312-01-05-4
Unit:              persons

 DATA COMPLETENESS
----------------------------------------------------------------------------------------------------
Total Records:     1,368
Year Range:        2000 - 2023 (24 years)
Regions Covered:   57
Non-NULL Values:   1,368 (100.0%)
NULL Values:       0
Quality Status:    EXCELLENT

 RUHR REGION CITIES COVERAGE
----------------------------------------------------------------------------------------------------
City                 Region Code  Records    Year Range      Status    
----------------------------------------------------------------------------------------------------
Duisburg             05112        24         2000-2023       OK        
Bochum               05911        24         2000-2023       OK        
Essen                05113        24         2000-2023       OK        
Dortmund             05913        24         2000-2023       OK        
Gelsenkirchen        05513        24         2000-2023       OK        

[OK] All Ruhr region cities have data

 TIME SERIES ANALYSIS - RUHR REGION CITIES
----------------------------------------------------------------------------------------------------
City                 Data Points  Mean Value      First -> Last             Change         
----------------------------------------------------------------------------------------------------
Duisburg             24           225.2           223.2 -> 232.0            +8.8 (+3.9%)   
Essen                24           324.2           314.5 -> 349.9            +35.4 (+11.3%) 
Gelsenkirchen        24           113.0           111.1 -> 117.9            +6.8 (+6.1%)   
Bochum               24           186.0           188.9 -> 199.8            +10.9 (+5.8%)  
Dortmund             24           308.6           281.7 -> 346.8            +65.1 (+23.1%) 

 VERIFICATION SUMMARY
----------------------------------------------------------------------------------------------------
[PASSED CHECKS]
  [OK] Records exist in database
  [OK] Data completeness >= 95%
  [OK] All Ruhr cities have data
  [OK] Sufficient time series depth (>=10 years)

[OK] VERIFICATION PASSED - Data is accurate and complete
====================================================================================================
```

---

## What to Do if Verification Fails

### Issue: Missing Ruhr Cities

**Problem:** One or more Ruhr cities show "MISSING" status

**Actions:**
1. Check if the city exists in `dim_geography` table
2. Verify region codes are correct (05112, 05911, etc.)
3. Review raw data extraction logs
4. Check if the source table has data for these regions
5. Re-run extraction if needed

### Issue: Low Completeness Rate

**Problem:** Completeness < 95%

**Actions:**
1. Identify which records have NULL values
2. Check if NULLs are expected (e.g., for aggregate totals)
3. Review transformer logic
4. Check source data quality

### Issue: Insufficient Time Series

**Problem:** < 10 years of data

**Actions:**
1. Verify the table's available period from source
2. Check if extraction years parameter was set correctly
3. Review extraction logs for any year-specific failures
4. Re-extract missing years if needed

### Issue: Unreasonable Values

**Problem:** Min/max values seem wrong

**Actions:**
1. Check unit of measure (thousands, percentages, etc.)
2. Verify transformation logic
3. Compare against source documentation
4. Inspect specific records with anomalous values

---

## Integration with ETL Pipelines

### Standard Pipeline Flow

1. **Extract** → Raw data from source API
2. **Transform** → Clean and structure data
3. **Load** → Insert into database
4. **✅ VERIFY** → Run verification script
5. **Document** → Update table registry

### Adding Verification to Pipelines

At the end of each ETL script, add:

```python
# After successful loading
logger.info("=" * 60)
logger.info("Running verification...")
logger.info("=" * 60)

import subprocess
result = subprocess.run(
    ['python', 'verify_extraction_timeseries.py', '--indicator', str(INDICATOR_ID)],
    capture_output=True,
    text=True
)

if result.returncode == 0:
    print(result.stdout)
    logger.info("Verification passed")
else:
    print(result.stderr)
    logger.error("Verification failed")
```

---

## CSV Export for Analysis

The exported CSV contains columns:
- `region_code`: AGS code (05112, 05911, etc.)
- `region_name`: Full city name
- `year`: Year of observation
- `value`: Indicator value
- `gender`: Gender category (if applicable)
- `nationality`: Nationality category (if applicable)
- `age_group`: Age group (if applicable)
- `notes`: Additional metadata

### Use Cases for Exported Data

1. **Time series visualization** in Python/R
2. **Statistical analysis** (trends, correlations)
3. **Comparative analysis** between cities
4. **Integration** with other datasets
5. **Thesis documentation** and charts

---

## Verification Database Queries

For manual verification, useful queries:

### Check Record Count by Year
```sql
SELECT t.year, COUNT(*) as records
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = <ID>
GROUP BY t.year
ORDER BY t.year;
```

### Check Ruhr Cities Coverage
```sql
SELECT g.region_name, COUNT(*) as records
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = <ID>
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY g.region_name;
```

### Time Series for Single City
```sql
SELECT t.year, f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = <ID>
  AND g.region_code = '05913'  -- Dortmund
ORDER BY t.year;
```

---

## Verification Logs

All verification outputs are logged automatically. Review logs for:
- Timestamp of verification
- Indicator verified
- All check results
- Any warnings or errors

Logs location: Check your logging configuration in `src/utils/logging.py`

---

## Quality Standards

### Minimum Requirements for "PASSED" Status

- ✅ At least 1 record in database
- ✅ Completeness rate >= 85%
- ✅ All 5 Ruhr cities have data
- ✅ At least 1 year of data

### Target Standards

- 🎯 Completeness rate >= 95%
- 🎯 At least 10 years of continuous data
- 🎯 No missing years in the available period
- 🎯 Values within expected ranges

---

## Thesis Research Focus

The verification specifically covers the 5 Ruhr region cities because they are:
- Core of the former coal mining region
- Focus of sustainable transformation research
- Subject of the thesis on institutional and behavioral interventions

**Research Questions Enabled:**
1. How have employment patterns evolved in the Ruhr region?
2. What are the comparative trajectories of different cities?
3. Which cities show stronger transformation success?
4. What temporal patterns emerge from policy interventions?

---

## Next Steps After Verification

Once verification passes:

1. ✅ Update `table_registry.json` with status="completed"
2. ✅ Document any data quality notes
3. ✅ Export CSV for thesis analysis if needed
4. ✅ Move to next table extraction
5. ✅ Consider creating visualizations for thesis

---

## Contact & Support

For issues with verification:
- Review error messages in the output
- Check database connection settings
- Verify indicator ID exists in `dim_indicator`
- Consult `DATABASE_STATUS_REPORT.md` for current state

---

**Last Updated:** 2025-12-18
**Script Location:** `verify_extraction_timeseries.py`
**Documentation:** `VERIFICATION_WORKFLOW.md` (this file)
