
USE DATABASE A4_DB;
-- -- 1) Product Performance
-- 	CREATE VIEW Product_Performance AS
-- 	SELECT 
-- 		BRAND_NAME, 
-- 		PRODUCT_NAME, 
-- 		SUM(ESTIMATED_PURCHASES) as Total_Purchases, 
-- 		SUM(ESTIMATED_VIEWS) as Total_Views,
-- 		YEAR, 
-- 		MONTH
-- 	FROM a4_db.product_views_and_purchases_dim_model.FACT_SALES
-- 	GROUP BY 
-- 		BRAND_NAME, 
-- 		PRODUCT_NAME, 
-- 		YEAR, 
-- 		MONTH;

-- 2) Customer reviews and ratings
 	-- CREATE VIEW Reviews_Ratings_Insights AS
	-- SELECT  
	-- 	PRODUCT_NAME, 
	-- 	AVG(RATINGS) as Average_Rating, 
	-- 	SUM(REVIEW_COUNT) as Total_Reviews
	-- FROM 
	-- 	a4_db.product_views_and_purchases_dim_model.WEEKLY_SALES
	-- GROUP BY  
	-- 	PRODUCT_NAME;
	
-- --3) Inventory and availability tracking
-- 	CREATE VIEW Inventory_Availability AS
-- 	SELECT 
-- 		PRODUCT_NAME, 
-- 		IS_AVAILABLE, 
-- 	FROM a4_db.product_views_and_purchases_dim_model.WEEKLY_SALES;

-- -- 4) Marketplace analysis
-- 	CREATE VIEW Marketplace_Analysis AS
-- 	SELECT 
-- 		MARKETPLACE, 
-- 		COUNTRY, 
-- 		COUNT(DISTINCT SELLER_IDS) as Seller_Count
-- 	FROM 
-- 		Your_Sales_Table
-- 	GROUP BY 
-- 		MARKETPLACE, 
-- 		COUNTRY;

-- -- 5) Temporal Sale analysis
-- 	CREATE VIEW Temporal_Sales_Analysis AS
-- 	SELECT 
-- 		p.YEAR, 
-- 		p.MONTH, 
-- 		SUM(s.SALES) as Monthly_Sales
-- 	FROM 
-- 		Your_Product_Table p
-- 	JOIN 
-- 		Your_Date_Table d ON p.DATE = d.DATE
-- 	GROUP BY 
-- 		p.YEAR, 
-- 		p.MONTH;
		
-- 6) Product categorization
	CREATE VIEW Product_Categorization AS
	SELECT 
		Title, 
		SUB_CATEGORY, 
		COUNT(Title) as Product_Count
	FROM 
		a4_db.product_views_and_purchases_dim_model.PRODUCT_DIMENSION
	GROUP BY  
		TITLE, 
        SUB_CATEGORY;

-- -- 7) Historical Trend Analysis

-- 	CREATE VIEW Historical_Trends AS
-- 	SELECT 
--         PRODUCT,
-- 		YEAR, 
-- 		MONTH, 
-- 		SUM(SALES) as Total_Sales
-- 	FROM 
-- 		FACT_SALES f
--     JOIN Weekly_Sales w
--     ON f.PRODUCT_NAME = w.Product_name
-- 	GROUP BY 
-- 		YEAR, 
-- 		MONTH, 
--         Total_Sales;