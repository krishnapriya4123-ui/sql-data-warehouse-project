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



set @batch_start_time =now(),
@batch_end_timr=now();
select'============================================================================';
select 'Loading Bronze Layer';
select '============================================================================';

select '----------------------------------------------------------------------------';
select 'Loading CRM Tables';
select '-----------------------------------------------------------------------------';

set @start_time=now();
select '>> Truncating Table:bronze.crm_cust_info';

truncate table bronze.crm_cust_info;

select '>> Inserting Data into:bronze.crm_cust_info';

 LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@cst_id, @cst_key, @cst_firstname, @cst_lastname,
 @cst_marital_status, @cst_gndr, @cst_create_date)
SET
cst_id = NULLIF(@cst_id,''),
cst_key = NULLIF(@cst_key,''),
cst_firstname = NULLIF(@cst_firstname,''),
cst_lastname = NULLIF(@cst_lastname,''),
cst_marital_status = NULLIF(@cst_marital_status,''),
cst_gndr = NULLIF(@cst_gndr,''),
cst_create_date = NULLIF(@cst_create_date,'');

select *
from bronze.crm_cust_info;
select count(*) from bronze.crm_cust_info;

set @end_time=now();
SELECT CONCAT(
    '>> Load Duration ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;
select '-----------------------------------';

set @start_time=now();

select '>> Truncating Table:bronze.crm_prd_info';
truncate table bronze.crm_prd_info;

select '>> Inserting Data into:bronze.crm_prd_info';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@prd_id, @prd_key, @prd_nm, @prd_cose, @prd_line, @prd_start_dt, @prd_end_dt)
SET
prd_id = NULLIF(@prd_id,''),
prd_key = NULLIF(@prd_key,''),
prd_nm = NULLIF(@prd_nm,''),
prd_cose = NULLIF(@prd_cose,''),
prd_line = NULLIF(@prd_line,''),
prd_start_dt = NULLIF(@prd_start_dt,''),
prd_end_dt = NULLIF(@prd_end_dt,'');

SELECT * FROM  bronze.crm_prd_info;
SELECT COUNT(*) FROM  bronze.crm_prd_info;

set @end_time=now();
SELECT CONCAT(
    '>> Load Duration ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;
select '-----------------------------------';

set @start_time=now();
select '>> Truncating Table:bronze.crm_sales_details';
truncate table bronze.crm_sales_details;

select '>> Inserting Data into:bronze.crm_sales_details';


LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@sls_ord_num, @sls_prd_key, @sls_cust_id,
 @sls_order_dt, @sls_ship_dt, @sls_due_dt,
 @sls_sales, @sls_quantity, @sls_price)
SET
sls_ord_num   = NULLIF(@sls_ord_num,''),
sls_prd_key   = NULLIF(@sls_prd_key,''),
sls_cust_id   = NULLIF(@sls_cust_id,''),
sls_order_dt  = NULLIF(@sls_order_dt,''),
sls_ship_dt   = NULLIF(@sls_ship_dt,''),
sls_due_dt    = NULLIF(@sls_due_dt,''),
sls_sales     = NULLIF(@sls_sales,''),
sls_quantity  = NULLIF(@sls_quantity,''),
sls_price     = NULLIF(@sls_price,'');

SELECT * FROM bronze.crm_sales_details;
SELECT COUNT(*) FROM bronze.crm_sales_details;


set @end_time=now();
SELECT CONCAT(
    '>> Load Duration ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;
select '-----------------------------------';

select '-----------------------------------------------------------------------------';
select 'Loading ERP Tables';
select '-----------------------------------------------------------------------------';
set @start_time=now();

select '>> Truncating Table:bronze.erp_cust_az12';
truncate table bronze.erp_cust_az12;

select '>> Inserting Data into:bronze.erp_cust_az12';


#Loading table cust_as12
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@cid, @bdate, @gen)
SET
cid   = NULLIF(@cid,''),
bdate = NULLIF(@bdate,''),
gen   = NULLIF(@gen,'');

SELECT * FROM  bronze.erp_cust_az12;
SELECT COUNT(*) FROM  bronze.erp_cust_az12;

set @end_time=now();
SELECT CONCAT(
    '>> Load Duration ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;
select '-----------------------------------';

set @start_time=now();
select '>> Truncating Table:bronze.erp_loc_a101';
truncate table bronze.erp_loc_a101;

select '>> Inserting Data into:bronze.loc_a101';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@cid, @cntry)
SET
cid   = NULLIF(@cid,''),
cntry = NULLIF(@cntry,'');

SELECT * FROM  bronze.erp_loc_a101;
SELECT COUNT(*) FROM bronze.erp_loc_a101;

set @end_time=now();
SELECT CONCAT(
    '>> Load Duration ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;
select '-----------------------------------';

set @start_time=now();

select '>> Truncating Table:bronze.erp_px_cat_g1v2';
truncate table bronze.erp_px_cat_g1v2;
select '>> Inserting Data into:bronze.erp_px_cat_g1v2';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@id, @cat, @subcat, @maintenance)
SET
id          = NULLIF(@id,''),
cat         = NULLIF(@cat,''),
subcat      = NULLIF(@subcat,''),
maintenance = NULLIF(@maintenance,'');

SELECT * FROM bronze.erp_px_cat_g1v2;
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

set @end_time=now();
SELECT CONCAT(
    '>> Load Duration ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS message;
select '-----------------------------------';

set @batch_end_time=now();
select'==============================================';
select'Loading Bronze Laye is Completed';
SELECT CONCAT(
    '>> Total Load Duration ',
    TIMESTAMPDIFF(SECOND, @start_time, @end_time),
    ' seconds'
) AS Tot_Duration;
