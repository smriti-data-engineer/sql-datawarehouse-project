-- =================================================
-- CHECKS FOR silver.crm_cust_info
-- =================================================

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No result
SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted spaces
-- Expectation: No result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- Check for unwanted spaces
-- Expectation: No result
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- Data standardization and consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info


-- =================================================
-- CHECKS FOR silver.crm_prd_info
-- =================================================
-- Check for NULLS and Duplicates in Primary Key
-- Expectation: No Result

SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for numbers: negative or NULL
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL


-- Data standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


-- =================================================
-- CHECKS FOR silver.crm_sales_details
-- =================================================
-- check for invalid dates
SELECT
	sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 
OR LENGTH(sls_due_dt::TEXT) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

-- check for invalid data orders

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt

-- check for invalid sales
SELECT
	sls_sales old_sales,
	sls_quantity,
	sls_price old_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= NULL
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL

-- =================================================
-- CHECKS FOR silver.erp_cust_az12
-- =================================================

-- Check out of range bdate
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE

-- Check gan values
SELECT DISTINCT gen
FROM silver.erp_cust_az12

-- =================================================
-- CHECKS FOR silver.erp_loc_a101
-- =================================================

-- Check missing cids in the foreign table and fix it
SELECT cid
FROM silver.erp_loc_a101
WHERE cid  NOT IN
(SELECT cst_key FROM silver.crm_cust_info)

-- Data standardization & consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;

SELECT *
FROM silver.erp_loc_a101