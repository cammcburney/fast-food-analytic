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
