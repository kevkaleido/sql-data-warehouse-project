/*                                          Quality Checks

	Script Purpose:
              	This script performs various quality checks for data consistency, accuracy,
              	and standardization across the 'silver' schemas. It includes checks for:
              	- Null or duplicate primary keys.
              	- Unwanted spaces in string fields.
              	- Data standardization and consistency.
              	- Invalid date ranges and orders.
              	- Data consistency between related fields.

	Usage Notes :
              	- Run these checks after data loading Silver Layer.
              	- Investigate and resolve any discrepancies found during the checks.                                               */


----------------------------------------------------------------------------------------------------------
--Quality checks for silver.crm_cust_info
----------------------------------------------------------------------------------------------------------
--Check for duplicates and NULL in the primary key(expectation: none)

SELECT
    cst_id,
    COUNT(*) COUNT
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL



--check for unwanted spaces in string values(first name and last name) expectation : none

SELECT cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

--

SELECT cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname)



----------------------------------------------------------------------------------------------------------
--Quality checks for silver.crm_prd_info
----------------------------------------------------------------------------------------------------------

--check for trailing and leading spaces in the prd_name(expectation: none)

SELECT 
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)




--check for negative number in the prd_cost(expectation: none)

SELECT 
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL




--Check for invalid date orders(expectation: none)

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt



----------------------------------------------------------------------------------------------------------
--Quality checks for silver.crm_sales_details
----------------------------------------------------------------------------------------------------------

--check for data consistency between sales, quantity and price (expectation: none)

SELECT *
FROM silver.crm_sales_details
WHERE sls_quantity * sls_price != sls_sales
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price



--check for invalid date orders(expectation: none)

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt




----------------------------------------------------------------------------------------------------------
--Quality checks for silver.erp_cust_az12
----------------------------------------------------------------------------------------------------------

--Check for duplicates in cid (expectation: none)

SELECT 
    cid,
    COUNT(*) COUNT
FROM silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1


--check if cid matches cst_key in silver.crm_cust_info(expectation: none)

SELECT *
FROM silver.erp_cust_az12
WHERE cid NOT IN (SELECT cst_key from silver.crm_cust_info)



--identify out of range dates(expectation: none)
SELECT 
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1925-01-01' OR
bdate > GETDATE()


--Identify data inconsistencies in gender
SELECT DISTINCT
gen
FROM silver.erp_cust_az12


----------------------------------------------------------------------------------------------------------
--Quality checks for silver.erp_loc_a101
----------------------------------------------------------------------------------------------------------

--Check for duplicates in cid (expectation: none)
SELECT cid, COUNT(*) COUNT
FROM silver.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1

--check data standardization and consistency in country

SELECT DISTINCT cntry
FROM silver.erp_loc_a101



----------------------------------------------------------------------------------------------------------
--Quality checks for silver.erp_px_cat_g1v2
----------------------------------------------------------------------------------------------------------
 --data was already clean 
