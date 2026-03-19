{{
    config(
        dist='wdl_client_code',
        sort=['wdl_client_code', 'likely_source_type']
    )
}}

SELECT
    wdl_client_code,
    CAST(et_created_at AS DATE) AS et_created_date,
    COALESCE(likely_source_type, 'None') AS likely_source_type,
    form_managing_entity_committee_name,
    committee_name,
    COALESCE(recurring_type, 'None') AS recurring,
    SUM(post_refund_amount) AS dollars_raised,
    COUNT(DISTINCT wdl_transaction_id) AS number_of_donations
FROM {{ ref('core__donations')}}
GROUP BY wdl_client_code, et_created_date, likely_source_type, form_managing_entity_committee_name, committee_name, recurring
ORDER BY wdl_client_code, et_created_date DESC, likely_source_type
