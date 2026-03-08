/*
===============================================================================
Bulk Load: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `LOAD DATA` command to load data from csv Files to bronze tables.

===============================================================================
*/



TRUNCATE TABLE bronze_crm_cust_info;

LOAD DATA LOCAL INFILE '/Users/ananyasingh/Documents/DataEngineering/DataWithBaraa/DataWarehouse/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


TRUNCATE TABLE bronze_crm_prod_info;

LOAD DATA LOCAL INFILE '/Users/ananyasingh/Documents/DataEngineering/DataWithBaraa/DataWarehouse/sql-data-warehouse-project-main/datasets/source_crm/prod_info.csv'
INTO TABLE bronze_crm_prod_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


TRUNCATE TABLE bronze_crm_sales_details;

LOAD DATA LOCAL INFILE '/Users/ananyasingh/Documents/DataEngineering/DataWithBaraa/DataWarehouse/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


TRUNCATE TABLE bronze_erp_cust_az12;

LOAD DATA LOCAL INFILE '/Users/ananyasingh/Documents/DataEngineering/DataWithBaraa/DataWarehouse/sql-data-warehouse-project-main/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


TRUNCATE TABLE bronze_erp_loc_a101;

LOAD DATA LOCAL INFILE '/Users/ananyasingh/Documents/DataEngineering/DataWithBaraa/DataWarehouse/sql-data-warehouse-project-main/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


TRUNCATE TABLE bronze_erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE '/Users/ananyasingh/Documents/DataEngineering/DataWithBaraa/DataWarehouse/sql-data-warehouse-project-main/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
