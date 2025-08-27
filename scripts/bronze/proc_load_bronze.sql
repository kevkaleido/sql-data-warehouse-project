/*                             =====================================================
                               Stored Prcedure: Load Bronze Layer (Source -> Bronze)
                              =======================================================
Script Purpose: This Stored Procedure loads data into the 'bronze' schema from external CSV files.
                It performs the following actions:
                 -Truncates the bronze tables before loading data.
                 -Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters: None. This stored procedure does not accept any parameters or return any values 

Usage Example: EXEC bronze.load_bronze                                                                                   */





CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
       DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
       SET @batch_start_time = GETDATE();
BEGIN TRY
        PRINT '---------------------------------------'
        PRINT '           Loading Bronze Layer'


        PRINT '---------------------------------------'
        PRINT '           Loading CRM tables'
        PRINT ''
        PRINT ''

        SET @start_time = GETDATE();
        --first reset to an empty state(delete all rows)
 
        PRINT 'Truncating table: bronze.crm_cust_info'

        
        TRUNCATE TABLE bronze.crm_cust_info 

        --then bulk insert
        PRINT 'Inserting data into bronze.crm_cust_info'

        --Customer Info
        BULK INSERT bronze.crm_cust_info 
        FROM 'C:\Users\user\Desktop\analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm/cust_info.csv'
        WITH
             ( FIRSTROW = 2,
               FIELDTERMINATOR = ',',
               TABLOCK


             );
        SET @end_time = GETDATE();
        PRINT 'Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
           PRINT '---------------------------------------'




    SET @start_time = GETDATE();
    PRINT 'Truncating table: bronze.crm_prd_info'
    TRUNCATE TABLE bronze.crm_prd_info
    PRINT 'Inserting data into bronze.crm_prd_info'
    --Products
    BULK INSERT bronze.crm_prd_info 
    FROM 'C:\Users\user\Desktop\analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm/prd_info.csv'
    WITH
         ( FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
         );
    SET @end_time = GETDATE();
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '---------------------------------------'




    SET @start_time = GETDATE();
    PRINT 'Truncating table: bronze.crm_sales_details'
    TRUNCATE TABLE bronze.crm_sales_details
    PRINT 'Inserting data into bronze.crm_sales_details'
    --Sales Details 
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\user\Desktop\analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm/sales_details.csv'
    WITH
         ( FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
         );
    SET @end_time = GETDATE();
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '---------------------------------------'

        PRINT ''
        PRINT ''
        PRINT '           Loading ERP tables'
        PRINT ''
        PRINT ''

    SET @start_time = GETDATE();
    PRINT 'Truncating table: bronze.erp_cust_az12'
    TRUNCATE TABLE bronze.erp_cust_az12
    
    PRINT 'Inserting data into bronze.erp_cust_az12'
    --CUST_AZ12
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\user\Desktop\analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp/CUST_AZ12.csv'
    WITH
         ( FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
         );
    SET @end_time = GETDATE();
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '---------------------------------------'





    SET @start_time = GETDATE();
    PRINT 'Truncating table: bronze.erp_loc_a101'
    TRUNCATE TABLE bronze.erp_loc_a101
    
    PRINT 'Inserting data into bronze.erp_loc_a101'
    --LOC_A101
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\user\Desktop\analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp/LOC_A101.csv'
    WITH
         ( FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
         );
    SET @end_time = GETDATE();
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '---------------------------------------'





    SET @start_time = GETDATE();
    PRINT 'Truncating table: bronze.erp_px_cat_g1v2'
    TRUNCATE TABLE bronze.erp_px_cat_g1v2
    
    PRINT 'Inserting data into bronze.erp_px_cat_g1v2'
    --PX_CAT_G1V2
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\user\Desktop\analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp/PX_CAT_G1V2.csv'
    WITH
         ( FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK 

         );
    SET @end_time = GETDATE();
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '---------------------------------------'
    SET @batch_end_time = GETDATE();
    PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'
END TRY
BEGIN CATCH
    PRINT '========================================='
    PRINT 'Error Occured during Bronze Layer Loading'
    PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR)
    PRINT 'Error Message: ' + ERROR_MESSAGE()
    PRINT 'Error Message: ' + CAST(ERROR_LINE() AS NVARCHAR)
    PRINT '========================================='
END CATCH

END 
