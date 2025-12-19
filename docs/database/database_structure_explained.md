Database Structure Explained
Star Schema Design
Instead of having separate tables like tbl_population and tbl_employment, this database uses a star schema with:
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐│ dim_geography   │    │    dim_time     │    │  dim_indicator  │├─────────────────┤    ├─────────────────┤    ├─────────────────┤│ geo_id (PK)     │    │ time_id (PK)    │    │ indicator_id    ││ region_code     │    │ year            │    │ indicator_code  ││ region_name     │    │ reference_date  │    │ source_table_id ││ region_type     │    └────────┬────────┘    └────────┬────────┘│ ruhr_area       │             │                      │└────────┬────────┘             │                      │         │                      │                      │         └──────────────────────┼──────────────────────┘                                │                                ▼                    ┌───────────────────────┐                    │   fact_demographics   │  ← ALL DATA HERE                    ├───────────────────────┤                    │ geo_id (FK)           │ → Which region?                    │ time_id (FK)          │ → Which year?                    │ indicator_id (FK)     │ → What metric?                    │ value                 │ → THE NUMBER                    └───────────────────────┘
How It Works
indicator_id	indicator_code	Source Table	What's Stored
1	pop_total	12411-03-03-4	17,556 population records
9	employment_total	13111-01-03-4	798 employment records
Example Query for Duisburg 2024
Indicator ID	Code	Value
1	pop_total	502,270 (population)
9	employment_total	178,093 (employees)
Why This Design?
Flexibility - Easy to add new indicators without creating new tables
Consistency - All metrics share the same geography and time dimensions
Query simplicity - One JOIN pattern works for all data types
Scalability - 36 different source tables → same fact table structure
The indicator_id tells you what type of data each row contains, while geo_id and time_id tell you where and when.