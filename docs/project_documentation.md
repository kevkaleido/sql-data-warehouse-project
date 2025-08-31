# Data Warehouse Project Documentation

This project demonstrates my ability to build a comprehensive data warehouse using the medallion architecture pattern.

## Project Planning and Setup

### Initial Planning
- Created a comprehensive Notion todo list breaking down all tasks into small, manageable steps
- This approach provided a sense of accomplishment and dopamine boost when checking off completed tasks

### Architecture Design
- Created a flowchart diagram using Draw.io to visualize the data architecture
- Defined the medallion architecture with clear Bronze, Silver, and Gold layer responsibilities

### Naming Conventions
- **Bronze and Silver layers**: Used source file names to maintain traceability
- **Gold layer**: Implemented snake_case convention (lowercase with underscore delimiters)

## Bronze Layer Implementation

### Database Setup
- Created the main database: `DataWarehouse`
- Established three schemas: `bronze`, `silver`, and `gold`

### Table Creation and Data Loading
- Created tables in the bronze schema following established naming conventions
- Used bulk insert to load data from source destinations to bronze tables
- Truncated each table before loading to prevent unnecessary duplicates

### Automation and Monitoring
- Created stored procedure `bronze.load_bronze` for streamlined execution
- Implemented comprehensive logging with print statements for:
  - Loading CRM tables
  - Loading ERP tables
  - Truncating tables
  - Inserting data
  - Error tracking
  - Execution timing (start/end times for individual tables and entire batch)
- Used declared variables for time tracking:
  - Start time and end time variables for individual table load duration
  - Start batch time and end batch time variables for overall batch duration

### Version Control
- Committed all bronze layer work to Git before proceeding to silver layer

## Silver Layer Implementation

### Data Relationship Mapping
- Used Draw.io to visually map table relationships
- Connected tables via common columns and primary keys for clear understanding

### Table Structure Setup
- Copied DDL scripts from bronze tables
- Used find-and-replace to change `bronze.` to `silver.`
- Added audit column: `dwh_create_date DATETIME2` with `GETDATE()` default function
- This timestamp column records load timestamps for data lineage tracking

### Data Quality and Cleaning

#### CRM Customer Information (`bronze.crm_cust_info`)

**Quality Check Queries:**
- Checked for NULLs and duplicates in primary key (expectation: no results)
- Checked for unwanted spaces (expectation: no results)
- Verified data standardization and consistency

**Issues Found and Resolutions:**
- **Duplicates and NULLs in primary key**: Discovered duplicates from incomplete entries
- **Solution**: Used `ROW_NUMBER()` function:
  - Partitioned by primary key
  - Ordered by latest entry date
  - Filtered to select only rank #1 (most complete, recent entries)
- **Unwanted spaces**: Found in string columns (first name, last name)
- **Solution**: Applied `TRIM()` function to remove spaces
- **Data standardization**: Made gender and marital status values explicit and consistent

**Final Step**: Inserted cleaned data into `silver.crm_cust_info` and re-ran quality checks for validation

#### CRM Product Information (`bronze.crm_prd_info`)

**Quality Checks:**
- No NULLs or duplicates found in primary key (`prd_id`)
- Identified issue with `prd_key` field containing concatenated information

**Data Transformation:**
- **Issue**: `prd_key` characters were excessively long, containing multiple pieces of information
- **Solution**: Extracted first 5 characters using `SUBSTRING()` function
- **Character standardization**: 
  - Product category table used underscore `_` delimiters
  - Extracted keys used hyphen `-` delimiters
  - Used `REPLACE()` function to convert hyphens to underscores
- **Validation**: Verified 390 out of 397 extracted category IDs matched the product category table (`bronze.erp_px_cat_g1v2`)
- **Remaining characters**: Extracted actual `prd_key` using `SUBSTRING()` function
- **Cross-reference**: Found 177 out of 397 product keys matched `sls_prd_keys` in sales details table

**Additional Quality Improvements:**
- Checked and cleaned trailing/leading spaces in `prd_name`
- Handled NULLs in `prd_cost` (converted to 0) and checked for negative values
- Made product line names explicit from abbreviations
- **Date logic fix**: Corrected end dates that were earlier than start dates
  - Used `LEAD()` function partitioned by `prd_key` and ordered by start date
  - Set end date as start date of next record minus 1 day

**Schema Updates:**
- Modified DDL script to change `start_date` and `end_date` data types from `DATETIME` to `DATE`
- Added `cat_id` column
- Re-ran script (drops and recreates table)
- Inserted cleaned data and performed quality validation

#### CRM Sales Details (`bronze.crm_sales_details`)

**Data Validation:**
- Verified all product keys (`sls_prd_key`) match `silver.crm_prd_info` ✓
- Verified all customer IDs (`cst_id`) match `silver.crm_cust_info` ✓

**Date Format Issues:**
- Sales order date, ship date, and due date were in integer format (e.g., 20101229)

**Data Quality Checks and Transformations:**
- **Length validation**: Flagged dates not equal to 8 characters as NULL
- **Date logic validation**:
  - No ship dates earlier than order dates ✓
  - No due dates earlier than ship dates or order dates ✓
- **Data type conversion**: `INT` → `NVARCHAR` → `DATE`

**Business Logic Validation:**
- Checked if `quantity × price = sales`
- Identified negative numbers, zeros, and NULLs in sales, quantity, and price fields

**Data Transformation Rules:**
- If sales is negative, zero, NULL, or doesn't equal `quantity × price`: derive using `quantity × price`
- If price is zero or NULL: calculate using `sales ÷ quantity`
- Convert negative values to positive using `ABS()` function

**Final Steps:**
- Updated DDL script: changed date columns from `INT` to `DATE`
- Re-ran script and inserted cleaned data
- Performed quality validation checks

#### ERP Customer Additional Information (`bronze.erp_cust_az12`)

**Data Matching:**
- Identified that some `cid` values had three extra characters prepended
- Used `SUBSTRING()` function to remove extra characters and match `cst_key` in `silver.crm_cust_info`

**Data Quality Issues:**
- **Birthdate validation**:
  - Found dates over 100 years old
  - Found future dates
  - Flagged problematic dates as NULL for data quality

**Final Step**: Inserted cleaned data into `silver.erp_cust_az12`

#### ERP Customer Location Information (`bronze.erp_loc_a101`)

**Data Matching:**
- `cid` values matched `cst_key` in `silver.crm_cust_info` except for hyphen after first two characters
- Used `REPLACE()` function to remove hyphens

**Data Consistency Issues:**
- **Country field problems**:
  - Inconsistent formatting
  - Abbreviated country names
  - Empty strings
- **Solution**: Used `CASE WHEN` function to standardize country values

**Final Step**: Inserted cleaned data into `silver.erp_loc_a101`

#### ERP Product Category (`bronze.erp_px_cat_g1v2`)
- Performed data quality checks
- No cleaning required
- Inserted data directly into `silver.erp_px_cat_g1v2`

### Silver Layer Automation
- Combined all silver layer insertion scripts into a single stored procedure
- Added comprehensive print statements for execution tracking, debugging, and flow understanding
- Mirrored bronze layer monitoring approach

## Gold Layer Implementation

### Business Object Identification
- Returned to Draw.io to identify and explore business objects
- Updated visual representation of table relationships

### Dimension Table: Customers (`gold.dim_customers`)

**Table Joining Strategy:**
- Joined main customer table (`silver.crm_cust_info`) with supplementary tables:
  - `silver.erp_cust_az12` (birthdate information)
  - `silver.erp_loc_a101` (country information)

**Data Integration Logic:**
- **Gender field consolidation**: Both main and location tables contained gender information
  - Primary source: main customer info table (`silver.crm_cust_info`)
  - Fallback logic: When main table shows 'n/a', use data from location table (`silver.erp_loc_a101`)
  - Implemented using `CASE WHEN` function

**Dimension Design:**
- Introduced surrogate key using `ROW_NUMBER()` function
- Applied friendly naming conventions using snake_case
- Sorted columns logically for improved readability
- Created as VIEW: `gold.dim_customers`

### Dimension Table: Products (`gold.dim_products`)

**Table Joining:**
- Joined product tables: `silver.crm_prd_info` and `silver.erp_px_cat_g1v2`

**Current Record Logic:**
- Filtered for active products where `prd_end_dt` IS NULL
- This represents current product information

**Dimension Design:**
- Grouped and sorted columns for improved readability
- Renamed columns to friendly, meaningful names
- Introduced surrogate key using `ROW_NUMBER()` function
- Created as VIEW: `gold.dim_products`

### Fact Table: Sales (`gold.fact_sales`)

**Definition**: A fact table connects multiple dimensions and contains measurable business metrics.

**Implementation Strategy:**
- Joined sales details table (`silver.crm_sales_details`) with dimension tables:
  - `gold.dim_customers`
  - `gold.dim_products`
- **Purpose**: Include dimension surrogate keys in fact table
- **Cleanup**: Removed original ID columns used for joining (replaced with surrogate keys)

**Benefits of Surrogate Keys:**
- Easier connectivity between facts and dimensions
- **Historical preservation**: Surrogate keys maintain data history, ensuring historical reports remain accurate forever

**Structure Organization:**
- Renamed columns to friendly, meaningful names
- Organized columns into logical groups:
  - Dimension keys
  - Dates
  - Measures
- Created as VIEW: `gold.fact_sales`

### Data Integrity Validation
- **Dimension connectivity**: Verified all dimension tables successfully connect to fact table using surrogate keys ✓
- **Orphaned records**: Confirmed no sales records exist without corresponding customers ✓

## Documentation and Visualization

### Star Schema Design
- Created comprehensive data model diagram in Draw.io showing star schema structure

### Data Catalog
- Developed detailed data catalog containing:
  - Column listings for Gold layer 
  - Data types for all columns
  - Descriptions with examples for better understanding

### Data Flow Documentation
- Extended Draw.io diagram to show complete data flow through all layers

## Project Completion
- Committed final codebase and documentation to GitHub
- All layers successfully implemented with proper data quality controls
- Comprehensive monitoring and logging in place
- Full medallion architecture achieved with Bronze (raw), Silver (cleaned), and Gold (business-ready) layers

## Key Achievements
- ✅ Medallion architecture implementation
- ✅ Comprehensive data quality framework
- ✅ Automated ETL processes with stored procedures
- ✅ Star schema design with proper dimension and fact tables
- ✅ Surrogate key implementation for historical data preservation
- ✅ Complete documentation and visual representations
- ✅ Version control with Git integration
