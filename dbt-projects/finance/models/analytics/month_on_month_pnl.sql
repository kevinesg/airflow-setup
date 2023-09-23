{{ 
    config(
        materialized='view'
    ) 
}}


SELECT
    y22.month
    
    , ROUND(y23.expenses - y22.expenses, 2) AS expenses_2023_vs_2022
    , ROUND(y23.income - y22.income, 2) AS income_2023_vs_2022
    , ROUND(y23.savings - y22.savings, 2) AS savings_2023_vs_2022
    
    , ROUND(
        SUM(y23.savings - y22.savings) OVER(
            ORDER BY y22.month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
      ) AS running_savings_2023_vs_2022
    
FROM {{ ref('monthly_pnl') }} AS y22
JOIN {{ ref('monthly_pnl') }} AS y23
	ON y22.month = y23.month
    
WHERE y22.year = '2022'
	AND y23.year = '2023'

ORDER BY month