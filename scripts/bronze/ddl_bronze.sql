/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

DROP TABLE IF EXISTS bronze_crm_cust_info;

CREATE TABLE bronze_crm_cust_info(
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

DROP TABLE IF EXISTS bronze_crm_cust_info;

CREATE TABLE bronze_crm_cust_info(
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost VARCHAR(50),
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

DROP TABLE IF EXISTS bronze_crm_cust_info;

CREATE TABLE bronze_crm_cust_info(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id VARCHAR(50),
    sls_order_dt  VARCHAR(50),
    sls_ship_dt  VARCHAR(50),
    sls_due_dt VARCHAR(50),
    sls_sales VARCHAR(50),
    sls_quantity VARCHAR(50),
    sls_price  VARCHAR(50)
);

DROP TABLE IF EXISTS bronze_erp_cust_az12;

CREATE TABLE bronze_erp_cust_az12(
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50)
);

DROP TABLE IF EXISTS bronze_erp_loc_a101;

CREATE TABLE bronze_erp_loc_a101(
    CID VARCHAR(50),
    CNTRY VARCHAR(50)
);

DROP TABLE IF EXISTS bronze_erp_px_cat_g1v2;

CREATE TABLE bronze_erp_px_cat_g1v2(
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);
