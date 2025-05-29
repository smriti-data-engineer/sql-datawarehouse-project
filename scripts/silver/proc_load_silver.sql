CREATE OR REPLACE PROCEDURE silver.load_silver_tables()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
	start_time := clock_timestamp();
	BEGIN
        RAISE NOTICE '==========================================';
		RAISE NOTICE 'LOADING SILVER LAYER';
		RAISE NOTICE '==========================================';
		
		RAISE NOTICE '------------------------------------------';
		RAISE NOTICE 'LOADING CRM TABLES';
		RAISE NOTICE '------------------------------------------';

	    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
	    TRUNCATE TABLE silver.crm_cust_info;
	    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
	    INSERT INTO silver.crm_cust_info (
	    	cst_id,
	    	cst_key,
	    	cst_firstname,
	    	cst_lastname,
	    	cst_marital_status,
	    	cst_gndr,
	    	cst_create_date)
    
	    WITH rank_cst AS
	    (SELECT
	    	*,
	    	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	    FROM bronze.crm_cust_info
	    WHERE cst_id IS NOT NULL)
    
	    SELECT
	    	cst_id,
	    	cst_key,
	    	TRIM(cst_firstname) AS cst_firstname,
	    	TRIM(cst_lastname) AS cst_lastname,
	    	CASE
	    		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
	    		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	    		ELSE 'n/a'
	    	END cst_marital_status,
	    	CASE
	    		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
	    		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	    		ELSE 'n/a'
	    	END cst_gndr,
	    	cst_create_date
	    FROM rank_cst
	    WHERE flag_last = 1;
    
    
	    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
	    TRUNCATE TABLE silver.crm_prd_info;
	    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
	    INSERT INTO silver.crm_prd_info (
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
	    	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	    	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
	    	prd_nm,
	    	COALESCE(prd_cost, 0) prd_cost,
	    	CASE UPPER(TRIM(prd_line))
	    		WHEN 'M' THEN 'Mountain'
	    		WHEN 'R' THEN 'Road'
	    		WHEN 'S' THEN 'Other Sales'
	    		WHEN 'M' THEN 'Touring'
	    		ELSE 'n/a'
	    	END prd_line,
	    	prd_start_dt,
	    	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 prd_end_dt
	    FROM bronze.crm_prd_info;
    
    
	    RAISE NOTICE '>> Truncating Table: ssilver.crm_sales_details';
	    TRUNCATE TABLE silver.crm_sales_details;
	    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
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
	    	CASE 
	    		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL 
	    		ELSE sls_order_dt::VARCHAR::DATE 
	    	END AS sls_order_dt,
	    	CASE 
	    		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL 
	    		ELSE sls_ship_dt::VARCHAR::DATE 
	    	END AS  sls_ship_dt,
	    	CASE 
	    		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL 
	    		ELSE sls_due_dt::VARCHAR::DATE 
	    	END AS  sls_due_dt,
	    	CASE 
	    		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	    		THEN sls_quantity * ABS(sls_price)
	    		ELSE sls_sales
	    	END AS sls_sales,
	    	sls_quantity,
	    	CASE 
	    		WHEN sls_price IS NULL OR sls_price <= 0
	    		THEN sls_sales/NULLIF(sls_quantity, 0)
	    		ELSE sls_price
	    	END AS sls_price
	    FROM bronze.crm_sales_details;
    
        RAISE NOTICE '------------------------------------------';
		RAISE NOTICE 'LOADING ERP TABLES';
		RAISE NOTICE '------------------------------------------';

	    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
	    TRUNCATE TABLE silver.erp_cust_az12;
	    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
	    INSERT INTO silver.erp_cust_az12 (
	    	cid,
	    	bdate,
	    	gen
	    )
	    SELECT
	    	CASE 
	    		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	    		ELSE cid
	    	END AS cid,
	    	CASE 
	    		WHEN bdate > CURRENT_DATE THEN NULL
	    		ELSE bdate
	    	END AS bdate,
	    	CASE 
	    		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	    		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	    		ELSE 'n/a'
	    	END gen
	    FROM bronze.erp_cust_az12;
    
    
	    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
	    TRUNCATE TABLE silver.erp_loc_a101;
	    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
	    INSERT INTO silver.erp_loc_a101 (
	    	cid,
	    	cntry
	    )
	    SELECT
	    	REPLACE(cid, '-', '') cid,
	    	CASE 
	    		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	    		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	    		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	    		ELSE TRIM(cntry)
	    	END AS cntry
	    FROM bronze.erp_loc_a101;
    
    
	    RAISE NOTICE '>> Truncating Table: ssilver.erp_px_cat_g1v2';
	    TRUNCATE TABLE silver.erp_px_cat_g1v2;
	    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	    INSERT INTO silver.erp_px_cat_g1v2 (
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
	    FROM bronze.erp_px_cat_g1v2;
    EXCEPTION WHEN OTHERS THEN
		DECLARE
	        v_message text;
	        v_state text;
	        v_detail text;
	        v_hint text;
	        v_context text;
	    BEGIN
	        GET STACKED DIAGNOSTICS
	            v_message = MESSAGE_TEXT,
	            v_state = RETURNED_SQLSTATE,
	            v_detail = PG_EXCEPTION_DETAIL,
	            v_hint = PG_EXCEPTION_HINT,
	            v_context = PG_EXCEPTION_CONTEXT;
			RAISE WARNING '====================================================';
			RAISE WARNING 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			RAISE WARNING '====================================================';
			
	        RAISE WARNING 'Error Message: %', v_message;
	        RAISE WARNING 'SQLSTATE: %', v_state;
	        RAISE WARNING 'Hint: %', v_hint;
	        RAISE WARNING 'Context: %', v_context;
	    END;
    END;
    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE 'Execution Started At: %', start_time;
    RAISE NOTICE 'Execution Ended At: %', end_time;
    RAISE NOTICE 'Total Time Taken: %', duration;
END;
$$;