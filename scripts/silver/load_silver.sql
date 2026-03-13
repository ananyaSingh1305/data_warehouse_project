/*
===============================================================================
Stored Procedure:Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
===============================================================================
*/

TRUNCATE TABLE silver_crm_cust_info;

INSERT INTO silver_crm_cust_info(
cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)

SELECT 
t.cst_id, 
t.cst_key, 
TRIM(t.cst_firstname), 
TRIM(t.cst_lastname), 
CASE 
WHEN TRIM(cst_marital_status)='M' THEN 'Married'
WHEN Tsilver_crm_cust_infoRIM(cst_marital_status)='S' THEN 'Single'
ELSE 'N/A'
END AS cst_marital_status,
CASE 
WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
ELSE 'N/A'
END AS cst_gndr,
t.cst_create_date
FROM
(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_row
FROM DataWarehouse.bronze_crm_cust_info) t
WHERE t.flag_row=1 AND t.cst_id!=0;



TRUNCATE TABLE silver_crm_prod_info
  
INSERT INTO silver_crm_prod_info(prd_id,prd_cat_id,prd_sales_id,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_sales_id,
prd_nm,
IF (prd_cost='', '0', prd_cost) AS prd_cost,
CASE TRIM(prd_line)
WHEN 'M' THEN 'Mountain'
WHEN 'R' THEN 'Road'
WHEN 'S' THEN 'Other Sales'
WHEN 'T' THEN 'Touring'
ELSE 'N/A'
END AS prd_line,
prd_start_dt,
DATE_SUB(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_key, prd_start_dt), INTERVAL 1 DAY) AS prd_end_dt
FROM DataWarehouse.bronze_crm_prod_info
ORDER BY prd_cat_id, prd_sales_id;



TRUNCATE TABLE silver_crm_sales_details
  
INSERT INTO DataWarehouse.silver_crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)

SELECT 
sls_ord_num, 
sls_prd_key,
sls_cust_id,
CASE
WHEN sls_order_dt = 0 OR length(sls_order_dt)!=8 THEN NULL 
ELSE CAST(sls_order_dt AS DATE)
END AS sls_order_dt,
CASE
WHEN sls_ship_dt = 0 OR length(sls_ship_dt)!=8 THEN NULL 
ELSE CAST(sls_ship_dt AS DATE)
END AS sls_ship_dt,
CASE
WHEN sls_due_dt = 0 OR length(sls_due_dt)!=8 THEN NULL 
ELSE CAST(sls_due_dt AS DATE)
END AS sls_due_dt,
CASE 
WHEN sls_sales IS NULL 
     OR sls_sales <= 0 
     OR sls_sales != (
        (CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
            THEN ABS(sls_sales)/sls_quantity 
            ELSE sls_price 
         END) * sls_quantity
     )
THEN 
     (CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN ABS(sls_sales)/sls_quantity 
        ELSE sls_price 
      END) * sls_quantity
ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN ((sls_price IS NULL) OR (sls_price<=0)) THEN (ABS(sls_sales)/sls_quantity) ELSE sls_price END AS sls_price
FROM DataWarehouse.bronze_crm_sales_details;



TRUNCATE TABLE silver_erp_cust_az12
  
INSERT INTO silver_erp_cust_az12(CID,BDATE,GEN)
  
SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LENGTH(CID))
ELSE CID
END AS CID,
BDATE,
CASE 
WHEN GEN LIKE 'M%' THEN 'Male'
WHEN GEN LIKE 'F%' THEN 'Female'
ELSE 'N/A'
END AS GEN
FROM DataWarehouse.bronze_erp_cust_az12;



TRUNCATE TABLE silver_erp_loc_a101
  
INSERT INTO silver_erp_loc_a101(CID, CNTRY)
SELECT 
REPLACE(CID,'-','') AS CID,
CASE 
WHEN TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) LIKE 'DE%' THEN 'Germany'
WHEN TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) LIKE 'US%' OR TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) LIKE 'USA%' THEN 'United States'
WHEN ((TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) ='') OR (TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', '')) IS NULL)) THEN 'N/A'
ELSE TRIM(REGEXP_REPLACE(CNTRY, '[^A-Za-z ]', ''))
END AS CNTRY
FROM DataWarehouse.bronze_erp_loc_a101;



TRUNCATE TABLE silver_erp_px_cat_g1v2
  
INSERT INTO silver_erp_px_cat_g1v2(ID,CAT,SUBCAT,MAINTENANCE)
SELECT ID,
CAT,
SUBCAT,
TRIM(REGEXP_REPLACE(maintenance, '[^A-Za-z ]', '')) AS MAINTENANCE
FROM DataWarehouse.bronze_erp_px_cat_g1v2;

