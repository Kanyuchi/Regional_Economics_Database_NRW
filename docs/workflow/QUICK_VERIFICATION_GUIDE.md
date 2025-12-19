# Quick Verification Guide

## After Every Table Extraction

### 1. Run Verification
```bash
python verify_extraction_timeseries.py --indicator <ID>
```

### 2. Check Output
Look for this line at the bottom:
- ✅ `[OK] VERIFICATION PASSED` → Good to proceed
- ⚠️ `[!] VERIFICATION PASSED WITH WARNINGS` → Review warnings
- ❌ `[X] VERIFICATION FAILED` → Fix issues before proceeding

### 3. Verify Ruhr Cities
Must show **"OK"** for all 5 cities:
- Duisburg (05112)
- Bochum (05911)
- Essen (05113)
- Dortmund (05913)
- Gelsenkirchen (05513)

### 4. Check Time Series
- Data points should match expected years
- Values should be reasonable
- Check for significant trends

### 5. Export if Needed
```bash
python verify_extraction_timeseries.py --indicator <ID> --export-csv
```

---

## Example: After Extracting Table 13312-01-05-4

```bash
# Run extraction
python pipelines/regional_db/etl_13312_01_05_4_employed_sector.py

# Verify (indicator_id = 19)
python verify_extraction_timeseries.py --indicator 19

# Export for analysis
python verify_extraction_timeseries.py --indicator 19 --export-csv
```

---

## What "PASSED" Means

✅ Records exist in database  
✅ Data completeness >= 95%  
✅ All Ruhr cities have data  
✅ Sufficient time series depth (>=10 years)

---

## Current Verified Indicators

| ID | Indicator | Status | Years | Cities |
|----|-----------|--------|-------|--------|
| 18 | Unemployment | ✅ PASSED | 2001-2024 | 5/5 |
| 19 | Employed by Sector | ✅ PASSED | 2000-2023 | 5/5 |

---

**Next:** Add verification after each new extraction!
