{{ 
    config(
        materialized='table'
    ) 
}}


SELECT DISTINCT
	type
    , CASE
    	WHEN TYPE IN ('EXPENSES', 'GIFT', 'BILLS', 'FAMILY SUPPORT', 'SALARY DEDUCTION', 'DONATION', 'TAX', 'CASH IN', 'LOSS') THEN 'EXPENSES'
        WHEN TYPE IN ('SALARY', 'BONUS', 'INTEREST', 'CASHBACK', 'CASH OUT') THEN 'INCOME'
        ELSE 'UNASSIGNED'
      END AS binary_type

FROM {{ source('finance', 'raw_data') }}

ORDER BY binary_type