# BA Commuter Statistics - Data Analysis

## Overview
Analysis of downloaded commuter data files from BA (Federal Employment Agency).

**Download Date**: January 3, 2026
**Source URL**: https://statistik.arbeitsagentur.de/DE/Navigation/Statistiken/Interaktive-Statistiken/Pendler/Pendler-Nav.html
**Files Downloaded**: 24 CSV files
**Total Data**: ~27,000 rows across all files

---

## Data Inventory

### ✓ What We Have: INCOMING COMMUTERS (Einpendler) ONLY

**All 24 files contain "Einpendler Kreise" (Incoming Commuters at District Level)**

#### File Categories:

**1. Detailed Breakdown Files (2 files)**
- **2024 Detailed** (`statistik_pendler_2026010323510.csv`)
  - Lines: 14,521
  - Coverage: Full geographic breakdown of where commuters come from to work in Düsseldorf
  - Shows origin districts across all of Germany

- **2009 Detailed** (`statistik_pendler_2026010323814.csv`)
  - Lines: 11,852
  - Coverage: Historical baseline year with full geographic breakdown
  - Same structure as 2024 file

**2. Summary Files (22 files - Years 2002-2023, excluding 2009)**
- Each file: ~69-70 lines
- Coverage: Total incoming commuters for each NRW district (no geographic breakdown)
- Shows only aggregate totals

---

## Data Structure

### Columns (All Files)
```
Workplace | Regional Key | Place of Residence | Regional Key | Total | Men | Women | Germans | Foreigners | Trainees
```

### Sample Data (2024 - Düsseldorf)
```csv
"Workplace";"Regional key";"Place of residence";"Regional key";"Total";"Men";"Women";"Germans";"Foreigners";"Trainees"
"Düsseldorf, Stadt";"05111";"Einpendler insgesamt";"0";"290.700";"163.100";"127.600";"245.230";"45.470";"7.400"
"Düsseldorf, Stadt";"05111";"Einpendler aus dem Bundesgebiet";"00";"289.690";"162.310";"127.380";"245.070";"44.620";"7.400"
"Düsseldorf, Stadt";"05111";"Schleswig-Holstein";"01";"1.910";"1.050";"860";"1.690";"220";"30"
```

**Interpretation**:
- **Total incoming commuters to Düsseldorf**: 290,700 (2024)
- **Breakdown by demographics**: Men, Women, Germans, Foreigners, Trainees
- **Geographic origin**: Detailed down to district level across Germany

---

## Temporal Coverage

| Period | Years Available | Type | Status |
|--------|----------------|------|--------|
| **2024** | 1 year | Detailed breakdown | ✓ Downloaded |
| **2010-2023** | 14 years | Summary totals only | ✓ Downloaded |
| **2009** | 1 year | Detailed breakdown | ✓ Downloaded |
| **2002-2008** | 7 years | Summary totals only | ✓ Downloaded |
| **TOTAL** | **23 years (2002-2024)** | Mixed | ✓ Downloaded |

**Note**: Years 2010-2023 have summary data only (just totals per district), not full geographic breakdowns.

---

## ✗ What's MISSING: OUTGOING COMMUTERS (Auspendler)

### Critical Gap
According to the original requirements:
> "Both incoming and outgoing commuters (must be queried separately)"

**Status**: ✗ **OUTGOING COMMUTER DATA NOT DOWNLOADED**

The BA portal requires **separate queries** for:
1. **Einpendler (Incoming)**: People who commute INTO an NRW district to work ✓ **HAVE THIS**
2. **Auspendler (Outgoing)**: People who commute OUT OF an NRW district to work ✗ **MISSING**

### Why Outgoing Commuters Matter
- **Net commuter balance**: Incoming - Outgoing = Net gain/loss
- **Regional employment patterns**: Where do NRW residents actually work?
- **Economic dependency**: How reliant is a district on external workplaces?

**Example Use Case**:
- Düsseldorf has 290,700 incoming commuters (2024)
- But how many Düsseldorf residents commute OUT to work elsewhere?
- Net balance determines if district is a jobs importer or exporter

---

## Data Quality Assessment

### ✓ Strengths
1. **Long time series**: 23 years of data (2002-2024)
2. **Consistent structure**: All files use same column format
3. **Demographic breakdowns**: Gender, nationality, trainee status
4. **Complete NRW coverage**: All 54 districts included in summary files
5. **Reference date consistency**: All data as of June (mid-year snapshot)

### ⚠ Limitations
1. **Missing outgoing commuters**: Only half of required data
2. **Limited detailed years**: Full geographic breakdown only for 2009 and 2024
3. **Summary years incomplete**: 2010-2023 lack origin/destination details
4. **Single workplace breakdown**: Detailed files appear to focus on specific districts (Düsseldorf in 2024)

---

## Data Extraction Strategy

### Option A: Use Summary Data Only (2002-2024)
**Pros**:
- Simple extraction: Just district totals
- Long time series: 23 years
- Covers all NRW districts

**Cons**:
- No geographic origin information
- Missing outgoing commuters
- Limited analytical value (can't answer "where from/to?")

**Indicators Possible**:
- 101: Total Incoming Commuters by District
- (Cannot create outgoing without additional data)

### Option B: Use Detailed Data (2009, 2024 only)
**Pros**:
- Full geographic breakdown
- Can analyze commuter flows between regions
- Shows cross-border dependencies

**Cons**:
- Only 2 years available
- Large gap between 2009-2024 (15 years)
- Still missing outgoing commuters

**Indicators Possible**:
- 101: Incoming Commuters by Origin Region
- 102: Incoming Commuters by Demographics
- (Cannot create comprehensive flow analysis without outgoing)

### Option C: Request Additional Downloads (RECOMMENDED)
**Download outgoing commuter data** for same years:
- 2024 detailed outgoing (Auspendler)
- 2009 detailed outgoing (historical baseline)
- 2002-2023 summary outgoing (time series)

**Complete Indicators Possible**:
- 101: Incoming Commuters (Einpendler)
- 102: Outgoing Commuters (Auspendler)
- 103: Net Commuter Balance (Incoming - Outgoing)
- 104: Commuter Dependency Ratio (Incoming / Local Employment)
- 105: Commuter Flow Matrix (Origin-Destination pairs)

---

## File Naming Convention
```
statistik_pendler_YYYYMMDDHHMMSS.csv
```
- YYYY: Year (2026 = download year)
- MM: Month (01 = January)
- DD: Day (03)
- HHMMSS: Time of download

**Files are NOT named by data year** - the year is found in the "Area status" metadata row inside each file.

---

## Next Steps

### Immediate Actions Required:
1. ✗ **Download Outgoing Commuter Data (Auspendler)** for years 2002-2024
   - URL: Same as incoming, but select "Auspendler" in query interface
   - Should result in ~24 additional files (matching current download)

2. ✓ **Verify Data Completeness** once outgoing data is available
   - Confirm all 54 NRW districts present
   - Check for any missing years
   - Validate data consistency between incoming/outgoing

3. **Design Database Schema**
   - Decide on indicator structure (separate indicators vs. notes field)
   - Plan fact table: fact_labor_market or fact_demographics?
   - Consider commuter flow matrix storage approach

### Optional Enhancements:
- Download detailed breakdowns for additional years (2010-2023) if available
- Verify if data exists for earlier years (pre-2002)
- Check if quarterly data is available (currently only June snapshots)

---

## Database Integration Plan

### Recommended Indicators:

**Indicator 101: Incoming Commuters (Einpendler)**
- Unit: Persons
- Dimensions: Gender, Nationality, Trainee Status, Origin Region (in notes)
- Coverage: All NRW districts, 2002-2024

**Indicator 102: Outgoing Commuters (Auspendler)** ✗ REQUIRES NEW DOWNLOAD
- Unit: Persons
- Dimensions: Gender, Nationality, Trainee Status, Destination Region (in notes)
- Coverage: All NRW districts, 2002-2024

**Indicator 103: Net Commuter Balance** (Derived)
- Calculation: Incoming - Outgoing
- Interpretation: Positive = Jobs importer, Negative = Jobs exporter
- Example: If Düsseldorf has +150k net, it's a major employment center

### Storage Approach:
```sql
fact_demographics (
    geo_id,           -- NRW district (workplace for incoming, residence for outgoing)
    time_id,          -- Year
    indicator_id,     -- 101 (incoming) or 102 (outgoing)
    value,            -- Commuter count
    gender,           -- 'male', 'female', NULL (total)
    nationality,      -- 'german', 'foreigner', NULL (total)
    notes             -- 'origin_region:05XXX' or 'destination_region:05XXX'
)
```

---

## Verification Checklist

Before extraction begins:

- [ ] Outgoing commuter data downloaded (Auspendler)
- [ ] All 54 NRW districts present in data
- [ ] Years 2002-2024 confirmed (23 years)
- [ ] File structure matches incoming commuter format
- [ ] Demographic columns consistent
- [ ] No duplicate files or years

Once verified, extraction can proceed with confidence.

---

**Analysis Completed**: 2026-01-03
**Analyst**: Claude Sonnet 4.5
**Status**: ⚠ INCOMPLETE - Awaiting outgoing commuter data download
