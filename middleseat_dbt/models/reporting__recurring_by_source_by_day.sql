{# Donor and donation analytics by day and source channel. Provides unique donor counts, average gift sizes, and recurring program metrics for use in client-facing fundraising dashboards. #}

{# Redshift performance configs, optimizes joins and range queries on wdl_client_code #}
{# See https://docs.getdbt.com/reference/resource-configs/redshift-configs for Redshift config docs #}
{{
    config(
        dist='wdl_client_code',
        sort=['wdl_client_code', 'likely_source_type']
    )
}}

WITH donation_data AS (
    SELECT *,
        MIN(CAST(et_created_at AS DATE)) OVER (PARTITION BY email) AS first_donation_date
    FROM {{ ref('core__donations') }}
    WHERE is_finance_exclusion = FALSE
        -- Only non-refunded donations count as a first donation
        AND post_refund_amount > 0
        AND likely_source_type IS NOT NULL
)

SELECT
    wdl_client_code,
    CAST(et_created_at AS DATE) AS et_created_date,
    likely_source_type,
    committee_name,

    COUNT(DISTINCT wdl_transaction_id) AS number_of_donations,
    SUM(post_refund_amount) AS dollars_raised,
    ROUND(SUM(post_refund_amount) / NULLIF(COUNT(DISTINCT wdl_transaction_id), 0), 2) AS avg_donation_size,

    -- Using email as a stand in for a unique donor id
    -- This may double count occasionally if a donor uses a different email between Shopify and ActBlue
    COUNT(DISTINCT email) AS unique_donors,
    COUNT(DISTINCT 
            CASE 
                WHEN first_donation_date = CAST(et_created_at AS DATE) THEN email 
                ELSE NULL
            END
    ) AS new_donors,
    -- new_recurring_donors can include totally new donors whose first donation is recurring
    -- It can also include existing one-time donors who have made their first recurring donation
    COUNT(DISTINCT 
            CASE 
                WHEN recurring_type = 'New' THEN email 
                ELSE NULL 
            END
    ) AS new_recurring_donors,
    

    ROUND(
        SUM(CASE WHEN recurring_type != 'One-time' THEN post_refund_amount ELSE 0 END) /
        NULLIF(COUNT(DISTINCT CASE WHEN recurring_type != 'One-time' THEN wdl_transaction_id END), 0)
    , 2) AS avg_recurring_donation_size,
    
    ROUND(AVG(CASE WHEN recurring_type != 'One-time' THEN recurring_gift_seq ELSE NULL END), 1) AS avg_recurring_sequence_length,
    -- Only for recurring donors who cancelled their recurring plans
    -- How many recurring donations on average before cancellation
    ROUND(AVG(CASE WHEN is_recurring_cancelled = TRUE THEN recurring_gift_seq ELSE NULL END), 1) AS avg_sequence_at_cancellation

FROM donation_data
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3