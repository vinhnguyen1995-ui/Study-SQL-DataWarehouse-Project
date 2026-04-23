/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

/* The detailed Steps to follow
1st step: truncate the table, create the bulk insert, notice the WITH to make sure the data quality
2nd step: check the total rows with COUNT(*)
3rd step: load all the files
4th step: go into create the stored procedure command
5th step: add PRINT to control the loading progress, issues, errors
6th step: add PRINT for Header, add PRINT for each section, explain what the step is doing
7th step: add TRY CATCH to ensure error handling, data integrity and issue logging for easier debugging
-- CREATE OR ALTER PROCEDURE pcd_name AS
-- BEGIN
---- BEGIN TRY
------ coding
---- END TRY
---- BEGIN CATCH
------ print issues log
---- END CATCH
-- END
8th step: track ETL duration --> identify bottlenecks, optimize performance, monitor trends, detect issues
9th step: declare the variables to calculate the loading time, @start_time @end_time DATETIME to get the seconds data
10th step: use DATEDIFF to calculate the duration btw start and end
11th step: declare the variable for whole batch @batch_strat_time, @batch_end_time
12th step: calculate the Duration of Loading Bronze Layer 'Whole Batch'
13th step: PRINT the message of whole batch duration
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN -- the start of the stored procedure
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

	SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==========================================';

		PRINT '------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; -- clear the content of the table before loading, prevent from duplication
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info -- insert all the data from csv file
		FROM 'C:\Users\vinhn\Documents\3. Learning\4. SQL\SQL PROJECT\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- tell the SQL that the data starts from the 2nd line because 1st row is header
			FIELDTERMINATOR = ',',
			TABLOCK
		); /* SELECT COUNT(*) FROM tbl_name to check the total rows of the loaded file */
		
		SET @end_time = GETDATE();
		PRINT '>> Loading Duartion: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; -- clear the content of the table before loading, prevent from duplication
		PRINT '>> Inserting Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info -- insert all the data from csv file
		FROM 'C:\Users\vinhn\Documents\3. Learning\4. SQL\SQL PROJECT\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, -- tell the SQL that the data starts from the 2nd line because 1st row is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duartion: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details; -- clear the content of the table before loading, prevent from duplication
		PRINT '>> Inserting Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details -- insert all the data from csv file
		FROM 'C:\Users\vinhn\Documents\3. Learning\4. SQL\SQL PROJECT\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, -- tell the SQL that the data starts from the 2nd line because 1st row is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duartion: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		PRINT '------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12; -- clear the content of the table before loading, prevent from duplication
		PRINT '>> Inserting Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12 -- insert all the data from csv file
		FROM 'C:\Users\vinhn\Documents\3. Learning\4. SQL\SQL PROJECT\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2, -- tell the SQL that the data starts from the 2nd line because 1st row is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duartion: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101; -- clear the content of the table before loading, prevent from duplication
		PRINT '>> Inserting Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101 -- insert all the data from csv file
		FROM 'C:\Users\vinhn\Documents\3. Learning\4. SQL\SQL PROJECT\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2, -- tell the SQL that the data starts from the 2nd line because 1st row is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duartion: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; -- clear the content of the table before loading, prevent from duplication
		PRINT '>> Inserting Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2 -- insert all the data from csv file
		FROM 'C:\Users\vinhn\Documents\3. Learning\4. SQL\SQL PROJECT\SQL_Data_Warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2, -- tell the SQL that the data starts from the 2nd line because 1st row is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loading Duartion: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';

		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '  - Total Loading Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';;
	END TRY
	BEGIN CATCH
		PRINT '=================================================';
		PRINT 'ERROR OCCURED DUING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR); -- CAST is used to transform the numberic data into text
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================================';
	END CATCH
END -- the end of the stored procedure
;
EXEC bronze.load_bronze;
