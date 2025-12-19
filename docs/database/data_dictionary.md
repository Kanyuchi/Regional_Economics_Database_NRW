# Data Dictionary
## Regional Economics Database - NRW

**Version:** 1.0  
**Last Updated:** December 2024  
**Coverage:** North Rhine-Westphalia, Germany

---

## Table of Contents
1. [Demographics Indicators](#demographics-indicators)
2. [Labor Market Indicators](#labor-market-indicators)
3. [Business Economy Indicators](#business-economy-indicators)
4. [Healthcare Indicators](#healthcare-indicators)
5. [Public Finance Indicators](#public-finance-indicators)
6. [Infrastructure Indicators](#infrastructure-indicators)
7. [Mobility Indicators](#mobility-indicators)
8. [Geographic Classifications](#geographic-classifications)
9. [Time Dimensions](#time-dimensions)

---

## Demographics Indicators

### Population by Gender, Nationality, and Age Groups
**Source:** Regional Database Germany  
**Table ID:** 12411-03-03-4  
**Update Frequency:** Annual  
**Reference Date:** December 31

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| total_population | INTEGER | Total population count | >= 0 |
| gender | VARCHAR(20) | Gender classification | Male, Female, Total |
| nationality | VARCHAR(50) | Nationality status | German, Foreign, Total |
| age_group | VARCHAR(50) | Age classification | 0-5, 6-14, 15-17, 18-24, 25-29, 30-39, 40-49, 50-64, 65+, Total |

**Granularity:** District level  
**Historical Availability:** 1995-present  
**Data Quality Notes:** Complete coverage for all NRW districts

---

### Population by Migration Background
**Source:** State Database NRW  
**Table ID:** 12211-132i  
**Update Frequency:** Annual

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| migration_background | VARCHAR(100) | Migration classification | With migration background, Without migration background, Total |
| employment_status | VARCHAR(100) | Employment classification | Employed, Unemployed, Not in labor force |
| gender | VARCHAR(20) | Gender | Male, Female, Total |
| count | INTEGER | Number of persons | >= 0 |

**Granularity:** District level  
**Historical Availability:** 2005-present  
**Data Quality Notes:** Based on microcensus sample

---

### Population by Personal Net Income
**Source:** State Database NRW  
**Table ID:** 12211-114i  
**Update Frequency:** Annual  
**Latest Available:** 2019

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| income_bracket | VARCHAR(100) | Monthly net income range | Under 500€, 500-900€, 900-1300€, 1300-1500€, 1500-2000€, 2000-2600€, 2600-3600€, 3600-5000€, 5000€+ |
| gender | VARCHAR(20) | Gender | Male, Female, Total |
| count | INTEGER | Number of persons | >= 0 |

**Granularity:** District level  
**Data Quality Notes:** Self-reported income data

---

## Labor Market Indicators

### Employees Subject to Social Insurance at Workplace
**Source:** Regional Database Germany  
**Table ID:** 13111-01-03-4  
**Update Frequency:** Annual  
**Reference Date:** June 30

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| employee_count | INTEGER | Number of employees | >= 0 |
| gender | VARCHAR(20) | Gender | Male, Female, Total |
| nationality | VARCHAR(50) | Nationality | German, Foreign, Total |

**Granularity:** District level (workplace)  
**Historical Availability:** 1999-present  
**Data Quality Notes:** Excludes self-employed, civil servants, mini-jobbers

---

### Employees by Economic Sector
**Source:** Regional Database Germany  
**Table ID:** 13111-07-05-4  
**Update Frequency:** Annual  
**Reference Date:** June 30

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| employee_count | INTEGER | Number of employees | >= 0 |
| gender | VARCHAR(20) | Gender | Male, Female, Total |
| nationality | VARCHAR(50) | Nationality | German, Foreign, Total |
| economic_sector | VARCHAR(100) | WZ 2008 classification | See WZ 2008 codes |

**Economic Sectors (WZ 2008):**
- A: Agriculture, forestry and fishing
- B: Mining and quarrying
- C: Manufacturing
- D: Electricity, gas, steam supply
- E: Water supply; sewerage, waste management
- F: Construction
- G: Wholesale and retail trade
- H: Transportation and storage
- I: Accommodation and food service
- J: Information and communication
- K: Financial and insurance activities
- L: Real estate activities
- M: Professional, scientific and technical activities
- N: Administrative and support service activities
- O: Public administration and defense
- P: Education
- Q: Human health and social work
- R: Arts, entertainment and recreation
- S: Other service activities
- T: Household activities
- U: Extraterritorial organizations

**Granularity:** District level  
**Historical Availability:** 1999-present

---

### Employment by Type (Full-time/Part-time)
**Source:** Regional Database Germany  
**Table ID:** 13111-03-02-4 (workplace), 13111-04-02-4 (residence)  
**Update Frequency:** Annual  
**Reference Date:** June 30

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| employee_count | INTEGER | Number of employees | >= 0 |
| employment_type | VARCHAR(50) | Type of employment | Full-time, Part-time, Total |
| location_type | VARCHAR(50) | Location reference | Workplace, Residence |

**Definitions:**
- Full-time: 30+ hours per week typically
- Part-time: <30 hours per week typically

**Granularity:** District level  
**Historical Availability:** 1999-present

---

### Employees by Educational Qualification
**Source:** Regional Database Germany  
**Table ID:** 13111-11-04-4 (workplace), 13111-12-03-4 (residence)  
**Update Frequency:** Annual  
**Reference Date:** June 30

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| employee_count | INTEGER | Number of employees | >= 0 |
| education_level | VARCHAR(100) | Qualification type | No vocational qualification, Recognized vocational qualification, Academic degree, Unknown |

**Granularity:** District level  
**Historical Availability:** 1999-present  
**Data Quality Notes:** "Unknown" category may be significant in some years

---

### Unemployment Statistics
**Source:** Regional Database Germany  
**Table ID:** 13211-02-05-4  
**Update Frequency:** Annual  
**Reference Period:** Annual average

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| unemployed_count | INTEGER | Number unemployed | >= 0 |
| unemployment_rate | NUMERIC(5,2) | Unemployment rate | 0.00-100.00 (percentage) |
| person_group | VARCHAR(100) | Classification | Total, Under 25 years, Women, Foreign nationals, Long-term unemployed |

**Granularity:** District level  
**Historical Availability:** 1995-present

---

### Employed Persons by Economic Sector
**Source:** Regional Database Germany  
**Table ID:** 13312-01-05-4  
**Update Frequency:** Annual  
**Reference Period:** Annual average

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| employed_count | INTEGER | Number of employed | >= 0 |
| economic_sector | VARCHAR(100) | Broad sector | Agriculture/forestry/fishing, Manufacturing, Construction, Services, Total |

**Note:** This includes all employed persons (broader than just social insurance employees)

**Granularity:** District level  
**Historical Availability:** 1991-present

---

### Employment and Wage Data (BA)
**Source:** Federal Employment Agency  
**Table:** Table 8 - Employment/Wages  
**Update Frequency:** Annual

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| median_wage | NUMERIC(10,2) | Median monthly wage | In Euros |
| employee_count | INTEGER | Number of employees | >= 0 |
| sector | VARCHAR(100) | Economic sector | Various classifications |

**Granularity:** District level  
**Historical Availability:** 1999-present

---

## Business Economy Indicators

### Construction Industry Statistics
**Source:** Regional Database Germany  
**Table ID:** 44231-01-03-4  
**Update Frequency:** Annual  
**Reference Date:** June 30

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| establishments | INTEGER | Number of establishments | >= 0 |
| active_persons | INTEGER | Persons working | >= 0 |
| construction_turnover | NUMERIC(15,2) | Turnover in € thousands | >= 0 |

**Scope:** Construction companies with 20+ employees

**Granularity:** District level  
**Historical Availability:** 1995-present

---

### Total Establishment Statistics
**Source:** Regional Database Germany  
**Table ID:** 44231-01-02-4  
**Update Frequency:** Annual  
**Reference Date:** June 30

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| establishments | INTEGER | Number of establishments | >= 0 |
| active_persons | INTEGER | Persons working | >= 0 |
| total_turnover | NUMERIC(15,2) | Turnover in € thousands | >= 0 |

**Granularity:** District level  
**Historical Availability:** 1995-present

---

### Establishments by Size Class
**Source:** Regional Database Germany  
**Table ID:** 52111-01-02-4  
**Update Frequency:** Annual

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| establishment_count | INTEGER | Number of establishments | >= 0 |
| size_class | VARCHAR(50) | Employee size range | 0 employees, 1-9, 10-49, 50-249, 250+, Total |

**Granularity:** District level  
**Historical Availability:** 2000-present

---

### Establishments by Economic Section
**Source:** Regional Database Germany  
**Table ID:** 52111-02-01-4  
**Update Frequency:** Annual

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| establishment_count | INTEGER | Number of establishments | >= 0 |
| economic_section | VARCHAR(100) | WZ 2008 section | A through U (see above) |

**Granularity:** District level  
**Historical Availability:** 2008-present

---

### Business Registrations and Closures
**Source:** Regional Database Germany  
**Table ID:** 52311-01-04-4  
**Update Frequency:** Annual  
**Reference Period:** Annual total

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| registrations | INTEGER | New business registrations | >= 0 |
| deregistrations | INTEGER | Business closures | >= 0 |
| net_change | INTEGER | Net change | Can be negative |

**Granularity:** District level  
**Historical Availability:** 2003-present

---

### Corporate Insolvencies
**Source:** Regional Database Germany  
**Table ID:** 52411-02-01-4  
**Update Frequency:** Annual  
**Reference Period:** Annual total

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| insolvency_applications | INTEGER | Filed applications | >= 0 |

**Granularity:** District level  
**Historical Availability:** 1999-present

---

### Gross Domestic Product
**Source:** State Database NRW  
**Table ID:** 82711-01i  
**Update Frequency:** Annual  
**Latest Available:** 2022

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| gdp | NUMERIC(15,2) | GDP in € millions | >= 0 |
| gross_value_added | NUMERIC(15,2) | GVA in € millions | >= 0 |
| economic_sector | VARCHAR(100) | Sector classification | 7 broad sectors |

**Sectors:**
1. Agriculture, forestry, fishing
2. Manufacturing
3. Construction
4. Trade, transport, hospitality
5. Information and communication
6. Finance and insurance
7. Public services, education, health

**Granularity:** District level  
**Data Quality Notes:** Revised values, subject to updates

---

### Employee Compensation
**Source:** State Database NRW  
**Table ID:** 82711-06i  
**Update Frequency:** Annual  
**Latest Available:** 2022

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| total_compensation | NUMERIC(15,2) | Total in € millions | >= 0 |
| compensation_per_employee | NUMERIC(10,2) | Per employee in € | >= 0 |
| economic_sector | VARCHAR(100) | Sector | 7 broad sectors |

**Granularity:** District level

---

## Healthcare Indicators

### Hospitals and Bed Capacity
**Source:** State Database NRW  
**Table ID:** 23111-01i  
**Update Frequency:** Annual

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| hospital_count | INTEGER | Number of hospitals | >= 0 |
| beds_available | INTEGER | Number of beds | >= 0 |
| provider_type | VARCHAR(100) | Ownership | Public, Private non-profit, Private for-profit |

**Granularity:** District level  
**Historical Availability:** 1991-present

---

### Doctors
**Source:** State Database NRW  
**Table ID:** 23111-12i  
**Update Frequency:** Annual

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| doctor_count | INTEGER | Number of full-time doctors | >= 0 |
| gender | VARCHAR(20) | Gender | Male, Female, Total |

**Granularity:** District level  
**Historical Availability:** 1995-present

---

### Care Facilities - Outpatient Services
**Source:** State Database NRW  
**Table ID:** 22411-01i  
**Update Frequency:** Biennial (every 2 years)

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| facility_count | INTEGER | Number of facilities | >= 0 |
| personnel_count | INTEGER | Number of staff | >= 0 |
| facility_type | VARCHAR(100) | Type of facility | Outpatient care services |

**Granularity:** District level

---

### Care Facilities - Residential
**Source:** State Database NRW  
**Table ID:** 22412-01i  
**Update Frequency:** Biennial

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| facility_count | INTEGER | Number of nursing homes | >= 0 |
| places_available | INTEGER | Available capacity | >= 0 |
| personnel_count | INTEGER | Number of staff | >= 0 |

**Granularity:** District level

---

### Persons Requiring Care - Outpatient
**Source:** State Database NRW  
**Table ID:** 22411-02i  
**Update Frequency:** Biennial

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| persons_in_care | INTEGER | Number of persons | >= 0 |
| care_level | VARCHAR(50) | Level classification | Level 1, 2, 3, 4, 5 |

**Granularity:** District level

---

### Persons Requiring Care - Residential
**Source:** State Database NRW  
**Table ID:** 22412-02i  
**Update Frequency:** Biennial

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| persons_in_care | INTEGER | Number of persons | >= 0 |
| care_level | VARCHAR(50) | Level classification | Level 1, 2, 3, 4, 5 |
| care_type | VARCHAR(100) | Type of care | Various classifications |

**Granularity:** District level

---

### Total Care Benefit Recipients
**Source:** State Database NRW  
**Table ID:** 22421-02i  
**Update Frequency:** Biennial

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| recipient_count | INTEGER | Number of recipients | >= 0 |
| care_level | VARCHAR(50) | Level | Level 1-5, Total |
| benefit_type | VARCHAR(100) | Type of benefit | Various types |

**Granularity:** District level

---

## Public Finance Indicators

### Municipal Finances - Receipts
**Source:** State Database NRW  
**Table ID:** 71517-01i  
**Update Frequency:** Annual

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| receipt_amount | NUMERIC(15,2) | Amount in € thousands | >= 0 |
| receipt_type | VARCHAR(100) | Type of receipt | Tax revenues, Transfers, Fees, etc. |

**Granularity:** Municipality level (can be aggregated to district)  
**Historical Availability:** 1991-present

---

### Income Tax Statistics
**Source:** State Database NRW  
**Table ID:** 73111-07iz (2021) or 73111-030i (2020+)  
**Update Frequency:** Annual with lag

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| taxpayer_count | INTEGER | Number of taxpayers | >= 0 |
| total_income | NUMERIC(15,2) | Total income in € | >= 0 |
| tax_amount | NUMERIC(15,2) | Tax collected in € | >= 0 |
| income_bracket | VARCHAR(100) | Income size class | Various brackets |

**Granularity:** Municipality level  
**Data Quality Notes:** Based on tax returns, 2-3 year lag

---

## Infrastructure Indicators

### Road Network
**Source:** State Database NRW  
**Table ID:** 46271-01i  
**Update Frequency:** Annual

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| road_length | NUMERIC(10,2) | Length in km | >= 0 |
| road_class | VARCHAR(100) | Classification | Federal highways (BAB), Federal roads (B), State roads (L), District roads (K) |

**Granularity:** District level  
**Historical Availability:** 1990-present

---

## Mobility Indicators

### Commuter Statistics - Incoming
**Source:** Federal Employment Agency  
**Update Frequency:** Annual  
**Reference Date:** June 30

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| commuter_count | INTEGER | Number of incoming commuters | >= 0 |
| origin_district | VARCHAR(50) | Origin district code | German district codes |
| destination_district | VARCHAR(50) | Work district code | German district codes |

**Granularity:** District-to-district flows  
**Historical Availability:** 1999-present

---

### Commuter Statistics - Outgoing
**Source:** Federal Employment Agency  
**Update Frequency:** Annual  
**Reference Date:** June 30

| Field | Type | Description | Values/Range |
|-------|------|-------------|--------------|
| commuter_count | INTEGER | Number of outgoing commuters | >= 0 |
| origin_district | VARCHAR(50) | Residence district code | German district codes |
| destination_district | VARCHAR(50) | Work district code | German district codes |

**Granularity:** District-to-district flows  
**Historical Availability:** 1999-present

---

## Geographic Classifications

### District Types
- **Kreisfreie Stadt** (Urban district): Independent city
- **Kreis** (District): Rural district containing multiple municipalities

### NRW Districts (53 total)
**Urban Districts (22):**
Aachen, Bielefeld, Bochum, Bonn, Bottrop, Dortmund, Duisburg, Düsseldorf, Essen, Gelsenkirchen, Hagen, Hamm, Herne, Köln (Cologne), Krefeld, Leverkusen, Mönchengladbach, Mülheim an der Ruhr, Münster, Oberhausen, Remscheid, Solingen, Wuppertal

**Districts (31):**
Aachen, Borken, Coesfeld, Düren, Ennepe-Ruhr-Kreis, Euskirchen, Gütersloh, Heinsberg, Herford, Hochsauerlandkreis, Höxter, Kleve, Lippe, Märkischer Kreis, Mettmann, Minden-Lübbecke, Oberbergischer Kreis, Olpe, Paderborn, Recklinghausen, Rhein-Erft-Kreis, Rheinisch-Bergischer Kreis, Rhein-Kreis Neuss, Rhein-Sieg-Kreis, Siegen-Wittgenstein, Soest, Steinfurt, Unna, Viersen, Warendorf, Wesel

### Regional Aggregations

**Ruhr Area (Ruhrgebiet):**
- Bochum
- Bottrop
- Dortmund
- Duisburg
- Essen
- Gelsenkirchen
- Hagen
- Hamm
- Herne
- Mülheim an der Ruhr
- Oberhausen
- Ennepe-Ruhr-Kreis
- Recklinghausen
- Unna
- Wesel

**Lower Rhine (Niederrhein):**
- Krefeld
- Mönchengladbach
- Kleve
- Rhein-Kreis Neuss
- Viersen
- Wesel (also in Ruhr area)

---

## Time Dimensions

### Reference Dates
- **December 31:** Population statistics
- **June 30:** Employment, establishment statistics
- **Annual average:** Unemployment, employed persons
- **Annual total:** Business registrations, insolvencies

### Update Lags
- Most indicators: Available year after reference year
- GDP data: 1-2 year lag
- Income tax: 2-3 year lag
- Care statistics: Biennial with 1-year lag

---

## Data Quality Indicators

### Completeness Score
- **100%:** All expected values present
- **95-99%:** Minor gaps (acceptable)
- **<95%:** Significant gaps (investigate)

### Consistency Flags
- **C:** Consistent across related tables
- **I:** Inconsistent (review needed)
- **U:** Unable to verify

### Validation Status
- **V:** Validated against source
- **E:** Estimated/Interpolated
- **P:** Provisional (subject to revision)
- **F:** Final

---

## Glossary

**Employees subject to social insurance:** Employees required to pay into statutory social insurance (excludes self-employed, civil servants, marginal part-time workers)

**Establishments:** Economic units at a single location

**WZ 2008:** German classification of economic activities (Wirtschaftszweigerklassifikation)

**Care level (Pflegegrad):** Classification from 1 (lowest care needs) to 5 (highest care needs)

**District (Kreis):** Administrative unit below state level

**Migration background:** Person or at least one parent born without German citizenship

---

## Revision History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | Dec 2024 | Initial version | Kanyuchi |

---

## Contact

For questions about data definitions or methodology:
**Data Steward:** Kanyuchi  
**Organization:** DBI  
**Email:** [Your email]

