 /*                                    Bronze Layer Data Cleaning
	Script Purpose:
		This script performs  data cleaning and transformation operations
		across multiple bronze layer tables. It includes cleaning processes for:
		- Duplicate record removal and primary key validation
		- String field trimming and standardization  
		- Data type conversions and format corrections
		- Null value handling and default assignments
		- Key extraction and transformation for table relationships
		- Date validation and logical consistency checks
		- Categorical data expansion from abbreviations to full names
		- Cross-table referential integrity validation

	Tables Processed:
		- bronze.crm_cust_info (Customer information)
		- bronze.crm_prd_info (Product information)
		- bronze.crm_sales_details (Sales transaction details)
		- bronze.erp_cust_az12 (ERP customer data)
		- bronze.erp_loc_a101 (ERP location data)

	Usage Notes:
		- Run these cleaning operations before promoting data to silver layer
		- Each section can be executed independently for specific table cleaning
		- Review transformation logic before applying to production data
		- Validate results against business rules and data quality standards
*/
 
 ---------------------------------------------------------------------------------------------

  --Data Cleaning Process: bronze.crm_cust_info

 ---------------------------------------------------------------------------------------------

--Check for duplicates and NULL in the primary key
SELECT
    cst_id,
    COUNT(*) COUNT
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL



-- rank and filter

SELECT *
    FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) Rank
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL) sub
WHERE Rank = 1


--check for unwanted spaces in string values(first name and last name)

SELECT cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)



-- fix these unwanted spaces using the TRIM function

SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) cst_firstname,
    TRIM(cst_lastname) cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM (SELECT *
    FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) Rank
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL) sub
WHERE Rank = 1) sub



--make gender and marital status name explicit and handle nulls

SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) cst_firstname,
    TRIM(cst_lastname) cst_lastname,
    CASE WHEN cst_marital_status = 'M' THEN 'Married'
         WHEN cst_marital_status = 'S' THEN 'Single'
         ELSE 'N/a'
         END cst_marital_status,
    CASE WHEN cst_gndr = 'M' THEN 'Male'
         WHEN cst_gndr = 'F' THEN 'Female'
         ELSE 'N/a'
    END cst_gndr,
    cst_create_date
FROM (SELECT *
    FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) Rank
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL) sub
WHERE Rank = 1) sub



---------------------------------------------------------------------------------------------

  --Data Cleaning Process: bronze.crm_prd_info

---------------------------------------------------------------------------------------------


--extract category key from prd_key so it matches id in category table


SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') cat_id, --extract category key
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info



--extract the remaining characters after cat_id extraction


SELECT 
    prd_id,
    prd_key ,
    REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') cat_id, --extract category key
    SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key, --extract product key
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info




--check for trailing and leading spaces in the prd_name

SELECT 
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)



--check for negative number in the prd_cost

SELECT 
prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL



--change prd_line names from abbr to explicit and handle prd_cost null


SELECT DISTINCT prd_line
FROM bronze.crm_prd_info


SELECT 
    prd_id,
    prd_key ,
    REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key,
    prd_nm,
    COALESCE(prd_cost, 0) prd_cost,
    CASE WHEN prd_line = 'R' THEN 'Road'
         WHEN prd_line = 'M' THEN 'Mountain'
         WHEN prd_line = 'S' THEN 'Other Sales'
         WHEN prd_line = 'T' THEN 'Touring'
         ELSE 'N/a'
    END prd_line,   --prd_line names changed from abbr to descriptive values
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info



--Check for invalid date orders

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt



--correct this invalid date orders by using end date = start date of the next record


SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key,
    prd_nm,
    COALESCE(prd_cost, 0) prd_cost,
    CASE WHEN prd_line = 'R' THEN 'Road'
         WHEN prd_line = 'M' THEN 'Mountain'
         WHEN prd_line = 'S' THEN 'Other Sales'
         WHEN prd_line = 'T' THEN 'Touring'
         ELSE 'N/a'
    END prd_line,
    CAST(prd_start_dt AS DATE) prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt --end date as one day before the next start date
FROM bronze.crm_prd_info



 ---------------------------------------------------------------------------------------------
  --Data Cleaning Process: bronze.crm_sales_details
 ---------------------------------------------------------------------------------------------
/*
Verified product keys (sls_prd_key) match records in silver.crm_prd_info ✓
Verified customer IDs (cst_id) match records in silver.crm_cust_info ✓

Identified and converted non-standard length dates (≠8 characters) to null
Validated date logic: sls_ship_date ≥ sls_order_date and sls_due_date ≥ sls_ship_date ✓
Cast date fields from INT → NVARCHAR → DATE

Applied the following transformation rules for sales, quantity, and price inconsistencies:

Sales issues: When sales was negative, zero, null, or ≠ (quantity × price) → derived using quantity × price
Price issues: When price was zero or null → calculated using sales ÷ quantity
Negative values: Converted all negative amounts to positive using ABS() function

Result: Clean dataset with validated relationships, proper date formats, and consistent sales calculations. */



SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN LEN(CAST(sls_order_dt AS NVARCHAR)) != 8 THEN NULL
	     ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE) END sls_order_dt,
	CASE WHEN LEN(CAST(sls_ship_dt AS NVARCHAR)) != 8 THEN NULL
	     ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE) END sls_ship_dt,
	CASE WHEN LEN(CAST(sls_due_dt AS NVARCHAR)) != 8 THEN NULL
	     ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE) END sls_due_dt,
	CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price) ELSE sls_sales END sls_sales,
	sls_quantity,
	CASE WHEN sls_price <= 0 OR sls_price IS NULL
		 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END sls_price        
FROM bronze.crm_sales_details





---------------------------------------------------------------------------------------------

  --Data Cleaning Process: bronze.erp_cust_az12

---------------------------------------------------------------------------------------------

--Check for duplicates in cid (expectation: none)

SELECT 
    cid,
    COUNT(*) COUNT
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1



--extract first three characters from cid to match cst_key in silver.crm_cust_info for table join

SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid END cid,
    bdate,
    gen
FROM bronze.erp_cust_az12




--check if cid matches cst_key in silver.crm_cust_info(expectation: none)


SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid END cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid END NOT IN (SELECT cst_key from silver.crm_cust_info)





--identify out of range dates(expectation: none)
SELECT 
    bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1925-01-01' OR
bdate > GETDATE()


--fix these out of range dates by replacing them with NULL

SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
         ELSE cid END cid,
    CASE WHEN bdate < '1925-01-01' OR bdate > GETDATE()
         THEN NULL ELSE bdate END bdate,
    gen
FROM bronze.erp_cust_az12



--check unique genders

SELECT DISTINCT
gen,
LEN(gen) length
FROM bronze.erp_cust_az12


--fix gender names

SELECT DISTINCT
 CASE WHEN gen IS NULL OR gen = '' THEN 'N/a'
      WHEN gen = 'F' THEN 'Female'
      WHEN gen = 'M' THEN 'Male'
      ELSE gen END gen,
LEN(gen) length
FROM bronze.erp_cust_az12




---------------------------------------------------------------------------------------------

  --Data Cleaning Process: bronze.erp_loc_a101

---------------------------------------------------------------------------------------------


--replace '-' to an empty string in cid to match cst_key in silver.crm_cust_info

SELECT 
REPLACE(cid, '-', '') cid
FROM bronze.erp_loc_a101



-- fix data inconsistency in country column

SELECT DISTINCT
CASE WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'N/a'
     WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     ELSE cntry END cntry
FROM bronze.erp_loc_a101
order by cntry



