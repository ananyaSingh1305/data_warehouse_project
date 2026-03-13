SELECT * FROM DataWarehouse.bronze_crm_sales_details;

DESCRIBE DataWarehouse.bronze_crm_sales_details;

-- QUALITY CHECK 1
-- UNWANTED SPACES
SELECT * FROM DataWarehouse.bronze_crm_sales_details WHERE sls_order_num != TRIM(sls_order_num);
-- NO ISSUES

-- QUALITY CHECK 2
-- Check if all sls_prd_key are present in silver_crm_prod_info
-- and sls_cust_id are present in silver_crm_cust_info

SELECT * FROM DataWarehouse.bronze_crm_sales_details WHERE sls_prd_key NOT IN (SELECT prd_sales_id FROM silver_crm_prod_info);

SELECT * FROM DataWarehouse.bronze_crm_sales_details WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver_crm_cust_info);
-- ALL GOOD

-- QUALITY CHECK 3
-- For sls_order_dt, check if any is <=0. If yes, convert it to NULL
-- Also check if length of the column is > or < 8
-- Cast it to date.
-- Do the same for all 3 date fields.
-- Also sls_order_dt < sls_ship_dt < sls_due_dt

SELECT * FROM DataWarehouse.bronze_crm_sales_details WHERE sls_order_dt<1;

SELECT
sls_ord_num, 
sls_prd_key,
sls_cust_id,
CASE
WHEN sls_order_dt = 0 OR length(sls_order_dt)!=8 THEN NULL 
ELSE CAST(sls_order_dt AS DATE)
END AS sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM DataWarehouse.bronze_crm_sales_details WHERE sls_ship_dt<1 OR length(sls_ship_dt)!=8 OR sls_ship_dt<sls_order_dt;


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
sls_sales,
sls_quantity,
sls_price
FROM DataWarehouse.bronze_crm_sales_details;

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
sls_sales,
sls_quantity,
sls_price
FROM DataWarehouse.bronze_crm_sales_details 
WHERE (sls_order_dt > sls_ship_dt) OR (sls_ship_dt > sls_due_dt) OR (sls_order_dt > sls_due_dt);

-- NO ISSUES.

-- QUALITY CHECK 4
-- LOGIC 1: sls_sales = sls_quantity * sls_price
-- LOGIC 2: Negative, NULLS, 0s are not allowed.

SELECT * FROM DataWarehouse.bronze_crm_sales_details 
WHERE (sls_quantity * sls_price) != sls_sales
OR sls_sales IS NULL OR sls_price IS NULL OR sls_quantity IS NULL
OR sls_sales < 0 OR sls_price <0 OR sls_quantity <0;

-- Discuss scenario with source team.
-- SOLUTION 1: They respond that data issues will be fixed directly in the source system, so issues will be there in source system till the source is fixed.
-- SOLUTION 2: If team says that data is really old and not in a budget to fix and that data issues has to be fixed in data warehouse or let the issue remain.
-- Let say, on discussion with team, following rules are finalized.
-- RULE 1: If price is negative, convert it to positive.
-- RULE 2: If price is 0 or NULL, calculate it using sales and quantity.
-- RULE 3: If sales is negative, 0, or NULL, derive it using quantity and price.

-- CASE WHEN t.new_sls_sales=0 THEN 12345 ELSE 54321 END

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

-- DDL for silver_crm_sales_details:

DROP TABLE IF EXISTS silver_crm_sales_details;

CREATE TABLE silver_crm_sales_details(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price  INT,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- INSERT IN TABLE

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


-- DO QUALITY CHECK AGAIN!!

select * from DataWarehouse.silver_crm_sales_details where sls_order_dt>sls_ship_dt or sls_ship_dt>sls_due_dt or sls_order_dt>sls_due_dt;




/*
 DATE: Handling invalid data (!=8) + Casting
 SALES: Handling missing data + Handling invalid data by deriving the column from already existing one.
*/





