"""
This query gathers total revenue and profit for the last monthly by week.
"""

SELECT
    TO_CHAR(DATE_TRUNC('week', TO_DATE(f.date, 'DD/MM/YYYY')), 'YYYY/MM/DD') AS week,
    ROUND(SUM(f.revenue)::numeric , 2) AS total_revenue,
    ROUND(SUM(f.profit)::numeric, 2) AS total_profit
FROM 
    fact f
GROUP BY 
    TO_CHAR(DATE_TRUNC('week', TO_DATE(f.date, 'DD/MM/YYYY')), 'YYYY/MM/DD')
ORDER BY 
    week DESC
LIMIT 4;

"""
This query gathers total revenue and profit for each month per year
"""

SELECT
    TO_CHAR(DATE_TRUNC('year', TO_DATE(f.date, 'DD/MM/YYYY')), 'YYYY') AS year,
    TO_CHAR(DATE_TRUNC('month', TO_DATE(f.date, 'DD/MM/YYYY')), 'Month') AS month,
    ROUND(SUM(f.revenue)::numeric, 2) AS total_revenue,
    ROUND(SUM(f.profit)::numeric, 2) AS total_profit
FROM 
    fact f
GROUP BY 
    TO_CHAR(DATE_TRUNC('year', TO_DATE(f.date, 'DD/MM/YYYY')), 'YYYY'),
    TO_CHAR(DATE_TRUNC('month', TO_DATE(f.date, 'DD/MM/YYYY')), 'Month')
ORDER BY 
    year DESC;


"""
This query gathers total revenue and profit for manager per month by year.
"""

SELECT
    TO_CHAR(DATE_TRUNC('year', TO_DATE(f.date, 'DD/MM/YYYY')), 'YYYY') AS year,
    TO_CHAR(DATE_TRUNC('month', TO_DATE(f.date, 'DD/MM/YYYY')), 'Month') AS month,
    ROUND(SUM(f.revenue)::numeric , 2) AS total_revenue,
    ROUND(SUM(f.profit)::numeric, 2) AS total_profit,
    m.manager
FROM 
    fact f
INNER JOIN manager m ON m.manager_id = f.manager_id
GROUP BY 
    TO_CHAR(DATE_TRUNC('year', TO_DATE(f.date, 'DD/MM/YYYY')), 'YYYY'),
    m.manager, 
    TO_CHAR(DATE_TRUNC('month', TO_DATE(f.date, 'DD/MM/YYYY')), 'Month')
ORDER BY 
     month, m.manager DESC;

"""
AVG PROFIT PER UNIT
"""

SELECT AVG(profit/quantity)
AS  Avg_Profit_Per_Unit
FROM fact;

"""
SALES BY PAYMENT METHOD
"""

SELECT
    payment_method_id, COUNT(order_id)
AS Total_Sales,
    SUM(revenue)
AS Total_Revenue,
FROM fact
GROUP BY payment_method_id 
ORDER BY Total_Sales 
DESC;

"""
PRODUCT PERFORMANCE ANALYSIS
"""

SELECT  
    product_id,
    SUM(revenue)
AS Total_Revenue,
    SUM(profit)
AS Total_Profit
FROM fact
GROUP BY product_id
ORDER BY Total_Profit;

"""
REVENUE AND PROFIT BY PURCHASE TYPES
"""

SELECT
    purchase_type_id,
    SUM(revenue)
AS Total_Revenue,
    SUM(profit)
AS Total_Profit
FROM fact
GROUP By purchase_type_id
ORDER BY Total_Revenue
DESC;
