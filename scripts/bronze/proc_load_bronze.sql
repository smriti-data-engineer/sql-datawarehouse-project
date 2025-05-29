CREATE OR REPLACE PROCEDURE bronze.load_bronze_tables()
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
		RAISE NOTICE 'LOADING BRONZE LAYER';
		RAISE NOTICE '==========================================';
		
		RAISE NOTICE '------------------------------------------';
		RAISE NOTICE 'LOADING CRM TABLES';
		RAISE NOTICE '------------------------------------------';
		-- Load crm_cust_info
		
		RAISE NOTICE '>> Truncating Table: crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		
		RAISE NOTICE '>> Inserting Data Into Table: crm_cust_info';
		EXECUTE format(
		    'COPY bronze.crm_cust_info FROM %L WITH (FORMAT csv, HEADER true)',
		    'D:/Portfolio Projects/sql-datwarehouse-project/datasets/source_crm/cust_info.csv'
		);
		
		
		-- Load crm_prd_info
		RAISE NOTICE '>> Truncating Table: crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		
		RAISE NOTICE '>> Inserting Data Into Table: crm_prd_info';
		EXECUTE format(
		    'COPY bronze.crm_prd_info FROM %L WITH (FORMAT csv, HEADER true)',
		    'D:/Portfolio Projects/sql-datwarehouse-project/datasets/source_crm/prd_info.csv'
		);
		
		-- Load crm_sales_details
		RAISE NOTICE '>> Truncating Table: crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		
		RAISE NOTICE '>> Inserting Data Into Table: crm_sales_details';
		EXECUTE format(
		    'COPY bronze.crm_sales_details FROM %L WITH (FORMAT csv, HEADER true)',
		    'D:/Portfolio Projects/sql-datwarehouse-project/datasets/source_crm/sales_details.csv'
		);
		
		RAISE NOTICE '------------------------------------------';
		RAISE NOTICE 'LOADING ERP TABLES';
		RAISE NOTICE '------------------------------------------';
			
		-- Load erp_cust_az12
		RAISE NOTICE '>> Truncating Table: erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		RAISE NOTICE '>> Inserting Data Into Table: erp_cust_az12';
		EXECUTE format(
		    'COPY bronze.erp_cust_az12 FROM %L WITH (FORMAT csv, HEADER true)',
		    'D:/Portfolio Projects/sql-datwarehouse-project/datasets/source_erp/CUST_AZ12.csv'
		);
		
		-- Load erp_loc_A101
		RAISE NOTICE '>> Truncating Table: erp_loc_A101';
		TRUNCATE TABLE bronze.erp_loc_A101;
		
		RAISE NOTICE '>> Inserting Data Into Table: erp_loc_A101';
		EXECUTE format(
		    'COPY bronze.erp_loc_A101 FROM %L WITH (FORMAT csv, HEADER true)',
		    'D:/Portfolio Projects/sql-datwarehouse-project/datasets/source_erp/LOC_A101.csv'
		);
		
		-- Load erp_px_cat_g1v2
		RAISE NOTICE '>> Truncating Table: erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
		RAISE NOTICE '>> Inserting Data Into Table: erp_px_cat_g1v2';
		EXECUTE format(
		    'COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT csv, HEADER true)',
		    'D:/Portfolio Projects/sql-datwarehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
		);
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
