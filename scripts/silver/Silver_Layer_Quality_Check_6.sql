SELECT * FROM DataWarehouse.bronze_erp_px_cat_g1v2;

-- QUALITY CHECK 1
-- For ID, we have a column in silver_crm_prod_info

select * from DataWarehouse.bronze_erp_px_cat_g1v2
where ID NOT IN (SELECT DISTINCT prd_cat_id FROM silver_crm_prod_info);
-- 1 item, but it's ok.

-- QUALITY CHECK 2
-- CAT: For unwanted spaces
select distinct cat, length(cat) from DataWarehouse.bronze_erp_px_cat_g1v2;

select * from DataWarehouse.bronze_erp_px_cat_g1v2
where cat!=trim(cat) OR subcat!=trim(subcat) or maintenance!=TRIM(maintenance);
-- All good

SELECT DISTINCT SUBCAT FROM DataWarehouse.bronze_erp_px_cat_g1v2;

SELECT DISTINCT subcat,length(subcat) FROM DataWarehouse.bronze_erp_px_cat_g1v2;

SELECT DISTINCT maintenance,length(maintenance) FROM DataWarehouse.bronze_erp_px_cat_g1v2;
-- ISSUE

SELECT DISTINCT TRIM(REGEXP_REPLACE(maintenance, '[^A-Za-z ]', '')),
LENGTH(TRIM(REGEXP_REPLACE(maintenance, '[^A-Za-z ]', '')))
FROM DataWarehouse.bronze_erp_px_cat_g1v2;

-- FINAL

SELECT ID,CAT,SUBCAT,TRIM(REGEXP_REPLACE(maintenance, '[^A-Za-z ]', '')) AS MAINTENANCE
FROM DataWarehouse.bronze_erp_px_cat_g1v2;

-- DDL

DROP TABLE IF EXISTS silver_erp_px_cat_g1v2;

CREATE TABLE silver_erp_px_cat_g1v2(
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver_erp_px_cat_g1v2(ID,CAT,SUBCAT,MAINTENANCE)
SELECT ID,CAT,SUBCAT,TRIM(REGEXP_REPLACE(maintenance, '[^A-Za-z ]', '')) AS MAINTENANCE
FROM DataWarehouse.bronze_erp_px_cat_g1v2;

-- QUALITY CHECK AGAIN
SELECT * FROM silver_erp_px_cat_g1v2;





