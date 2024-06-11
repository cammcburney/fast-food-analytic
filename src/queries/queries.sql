"""
This query gathers total revenue and profit for the last monthly by week.
"""

SELECT
    TO_CHAR(DATE_TRUNC('week', TO_DATE(f."Date", 'DD/MM/YYYY')), 'YYYY/MM/DD') AS week,
    ROUND(SUM(f."Revenue")::numeric , 2) AS total_revenue,
    ROUND(SUM(f."Profit")::numeric, 2) AS total_profit
FROM 
    fact f
GROUP BY 
    TO_CHAR(DATE_TRUNC('week', TO_DATE(f."Date", 'DD/MM/YYYY')), 'YYYY/MM/DD')
ORDER BY 
    week DESC
LIMIT 4;


SELECT
    TO_CHAR(DATE_TRUNC('year', TO_DATE(f."Date", 'DD/MM/YYYY')), 'YYYY') AS year,
    TO_CHAR(DATE_TRUNC('month', TO_DATE(f."Date", 'DD/MM/YYYY')), 'Month') AS month,
    ROUND(SUM(f."Revenue")::numeric, 2) AS total_revenue,
    ROUND(SUM(f."Profit")::numeric, 2) AS total_profit
FROM 
    fact f
GROUP BY 
    TO_CHAR(DATE_TRUNC('year', TO_DATE(f."Date", 'DD/MM/YYYY')), 'YYYY'),
    TO_CHAR(DATE_TRUNC('month', TO_DATE(f."Date", 'DD/MM/YYYY')), 'Month')
ORDER BY 
    year DESC;


SELECT
    TO_CHAR(DATE_TRUNC('month', TO_DATE(f."Date", 'DD/MM/YYYY')), 'Month') AS month,
    ROUND(SUM(f."Revenue")::numeric , 2) AS total_revenue,
    ROUND(SUM(f."Profit")::numeric, 2) AS total_profit,
    m."Manager"
FROM 
    fact f
INNER JOIN manager m ON m."Manager_id" = f."manager_id"
GROUP BY 
    m."Manager", TO_CHAR(DATE_TRUNC('month', TO_DATE(f."Date", 'DD/MM/YYYY')), 'Month')
ORDER BY 
     month, m."Manager" DESC;
