/*                             =====================================================
                               Stored Prcedure: Load Silver Layer (Bronze -> Silver)
                              =======================================================
Script Purpose: This Stored Procedure performs the ETL(Extract, Transform, Load) process to populate the 'silver' schema 
                tables from the 'bronze' schema.
                It performs the following actions:
                 -Truncates the 'silver' table
                 -Inserts transformed and cleaned data from Bronze to the 'Silver' tables.

Parameters: None. This stored procedure does not accept any parameters or return any values 

Usage Example: EXEC silver.load_bronze                                                                                   */

CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @start_load_time DATETIME, @end_load_time DATETIME
SET @start_load_time = GETDATE()
BEGIN TRY 
    PRINT' '
    PRINT' '
    PRINT'-------------------LOADING SILVER LAYER------------------------------------'
    PRINT' '
    PRINT' '
SET @start_time = GETDATE();
        
        PRINT 'Loading CRM tables'
        PRINT ''
        PRINT ''
    PRINT '>>Truncating Table: silver.crm_cust_info'
    TRUNCATE TABLE silver.crm_cust_info
    PRINT '>>Inserting data: silver.crm_cust_info from bronze.crm_cust_info'
    INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date 
    )

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
SET @end_time = GETDATE();
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'

    PRINT'---------------------------------------------------------------------------'

SET @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.crm_prd_info'
    TRUNCATE TABLE silver.crm_prd_info
    PRINT '>>Inserting data: silver.crm_prd_info from bronze.crm_prd_info'
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
        )

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
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
    FROM bronze.crm_prd_info
SET @end_time = GETDATE();
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'

    PRINT'---------------------------------------------------------------------------'




SET @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.crm_sales_details'
    TRUNCATE TABLE silver.crm_sales_details 
    PRINT '>>Inserting data: silver.crm_sales_details from bronze.crm_sales_details'
    INSERT INTO silver.crm_sales_details (
	    sls_ord_num,
	    sls_prd_key,
	    sls_cust_id,
	    sls_order_dt,
	    sls_ship_dt,
	    sls_due_dt,
	    sls_sales,
	    sls_quantity,
	    sls_price
    )

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
SET @end_time = GETDATE()
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + ' seconds'
    PRINT'---------------------------------------------------------------------------'

     PRINT 'Loading ERP tables'
        PRINT ''
        PRINT ''
SET @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.erp_cust_az12'
    TRUNCATE TABLE silver.erp_cust_az12
    PRINT '>>Inserting data: silver.erp_cust_az12 from bronze.erp_cust_az12'
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )


    SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
             ELSE cid END cid,
        CASE WHEN bdate < '1925-01-01' OR bdate > GETDATE()
             THEN NULL ELSE bdate END bdate,
        CASE WHEN gen IS NULL OR gen = '' THEN 'N/a'
             WHEN gen = 'F' THEN 'Female'
             WHEN gen = 'M' THEN 'Male'
             ELSE gen END gen
    FROM bronze.erp_cust_az12
SET @end_time = GETDATE();
    PRINT'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + ' seconds'
    PRINT'---------------------------------------------------------------------------'


SET @start_time = GETDATE();
    PRINT '>>Truncating Table: silver.erp_loc_a101'
    TRUNCATE TABLE silver.erp_loc_a101
    PRINT '>>Inserting data: silver.erp_loc_a101 from bronze.erp_loc_a101'
    INSERT INTO silver.erp_loc_a101(
        cid,
        cntry
    )
 
    SELECT 
    REPLACE(cid, '-', '') cid,
    CASE WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'N/a'
         WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         ELSE cntry END cntry
    FROM bronze.erp_loc_a101
SET @end_time = GETDATE();
    PRINT'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT'---------------------------------------------------------------------------'


SET @end_time = GETDATE();
    PRINT '>>Truncating Table: silver.erp_px_cat_g1v2'
    TRUNCATE TABLE silver.erp_px_cat_g1v2
    PRINT '>>Inserting data: silver.erp_px_cat_g1v2 from bronze.erp_px_cat_g1v2'
    INSERT INTO silver.erp_px_cat_g1v2(
	    id,
	    cat,
	    subcat,
	    maintenance
    )


    SELECT 
	    id,
	    cat,
	    subcat,
	    maintenance
    FROM bronze.erp_px_cat_g1v2
SET @end_time = GETDATE();
    PRINT'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT'---------------------------------------------------------------------------'
END TRY
BEGIN CATCH
    PRINT '========================================='
    PRINT 'Error Occured during SIlver Layer Loading'
    PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
    PRINT 'Error Message: ' + ERROR_MESSAGE()
    PRINT 'Error Message: ' + CAST(ERROR_LINE() AS NVARCHAR)
    PRINT '========================================='
END CATCH
SET @end_load_time = GETDATE()
    PRINT' '
    PRINT' ' 
    PRINT'LOADING SILVER LAYER COMPLETE'
    PRINT'Total Load Duration: ' + CAST(DATEDIFF(second, @start_load_time, @end_load_time)AS NVARCHAR) + ' seconds'
END
