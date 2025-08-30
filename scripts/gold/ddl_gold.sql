/*                               =========================================
                                     DDL Script: Create Gold Views
                                  =========================================
Script Purpose:
               This script creates views for the Gold layer in the data warehouse. 
               The Gold layer represents the final dimension and fact tables (Star Schema)

               Each view performs transformations and combines data from the Silver layer 
               to produce a clean, enriched, and business-ready dataset.

 Usage:
                These views can be queried directly for analytics and reporting.                                          */


--Create Dimension: gold.dim_customers

DROP VIEW IF EXISTS gold.dim_customers;

GO

CREATE VIEW gold.dim_customers AS

SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) customer_key, --surrogate key
	ci.cst_id				AS customer_id,
	ci.cst_key				AS customer_number,
	ci.cst_firstname		AS first_name,
	ci.cst_lastname			AS last_name,
	cnt.cntry				AS country,
	cie.bdate				AS birth_date,
	ci.cst_marital_status	AS marital_status,
CASE WHEN ci.cst_gndr != 'N/a' THEN ci.cst_gndr      --primary source for gender is CRM
		 ELSE COALESCE(cie.gen, 'N/a') END gender,   --if primary source not available, fallback to erp
ci.cst_create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 cie
ON ci.cst_key = cie.cid
LEFT JOIN silver.erp_loc_a101 cnt
ON ci.cst_key = cnt.cid

GO



--Create Dimension: gold.dim_products

DROP VIEW IF EXISTS gold.dim_products;

GO

CREATE VIEW gold.dim_products AS

SELECT 
	ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key, --surrogate key
	pi.prd_id				AS product_id,
	pi.prd_key				AS product_number,
	pi.prd_nm				AS product_name,
	pi.cat_id				AS category_id,
	pe.cat					AS category,
	pe.subcat				AS subcategory,
	pe.maintenance,
	pi.prd_cost				AS cost,
	pi.prd_line				AS product_line,
	pi.prd_start_dt			AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pe
ON pi.cat_id = pe.id
WHERE pi.prd_end_dt IS NULL --filters out historical data

GO



--Create Fact table: gold.fact_sales

DROP VIEW IF EXISTS gold.dim_products;

GO

CREATE VIEW gold.fact_sales AS

SELECT 
	sd.sls_ord_num			AS order_number,
	dp.product_key,                               --surrogate key from gold.dim_products
	dc.customer_key,                              --surrogate key from gold.dim_customers
	sd.sls_order_dt			AS order_date,
	sd.sls_ship_dt			AS shipping_date,
	sd.sls_due_dt			AS due_date,
	sd.sls_sales			AS sales_amount,
	sd.sls_quantity quantity, 
	sd.sls_price price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp
ON sd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id

