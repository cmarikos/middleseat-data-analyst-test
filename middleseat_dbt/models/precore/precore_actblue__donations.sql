{# Enriches raw ActBlue donation records with client codes, source classifications, and finance exclusions. Produces one row per donation with standardized fields ready for the core layer. #}

{# Redshift performance configs, optimizes joins and range queries on wdl_transaction_id #}
{# See https://docs.getdbt.com/reference/resource-configs/redshift-configs for Redshift config docs #}
{{
    config(
        dist='wdl_transaction_id',
        sort=['wdl_transaction_id'],

    )
}}


WITH 
    {# int_ prefix indicates this is an intermediate model containing partially transformed data not yet ready for the core layer #}
    donations AS (
        SELECT * FROM {{ ref('int_actblue__02__donations') }}
    ),

    {# Translates ActBlue Entity ID to client code #}
    codes AS (
        SELECT * FROM {{ source('auxiliary', 'actblue_entities_to_client_code') }}
    ),

    {# Grab known ref codes and their source type if they are in this table #}
    {# If the source type is not known it will be inferred by our likely_source_type.sql macro #}
    sources AS (
        SELECT
            wdl_client_code,
            refcode,
            form_name,
            type AS source_type
        FROM {{ source('auxiliary', 'auxiliary__source_categories_merged') }}
    ),

    {# Excludes anything that finance added to ActBlue that shouldn't be counted as digital fundraising #}
    finance_exclusions AS (
        SELECT
            wdl_client_code,
            exclude_from_digital,
            order_number
        FROM {{ source('auxiliary', 'auxiliary__finance_exclusions') }}
        WHERE exclude_from_digital IS TRUE
    )

SELECT
    {#- Use 'ZZZ' as a null placeholder -#}
    {#- Creates a standard unique transaction ID indicating ActBlue, client code, and line item ID -#}
    COALESCE(codes.wdl_client_code, 'ZZZ') AS wdl_client_code,
    'actblue-'||LOWER(COALESCE(codes.wdl_client_code, 'ZZZ'))||'-'||donations.lineitem_id as wdl_transaction_id,

    donations.lineitem_id,
    donations.committee_name,
    donations.order_number,
    donations.utc_created_at,
    {#- The normalize_timestamp macro handles timestamp format variation and converts to US Eastern time -#}
    {{ normalize_timestamp('donations.utc_created_at', 'UTC', 'US/Eastern') }} AS et_created_at,
    EXTRACT(YEAR from donations.et_created_at) AS et_created_year,
    EXTRACT(MONTH from donations.et_created_at) AS et_created_month,
    CAST(
        DATE_TRUNC('month', donations.et_created_at) AS DATE
    ) AS et_created_month_trunc,
    donations.utc_modified_at,
    {{ normalize_timestamp('donations.utc_modified_at', 'UTC', 'US/Eastern') }} AS et_modified_at,
    donations.entity_id,
    donations.amount,
    donations.post_refund_amount,

    donations.first_name,
    donations.last_name,
    donations.email,
    donations.phone,
    donations.address,
    donations.city,
    donations.state,
    donations.zip,
    donations.country,

    CAST(donations.is_recurring AS BOOLEAN) AS is_recurring,
    donations.utc_recurring_started_at,
    donations.recurring_gift_seq,
    donations.recurring_period,
    donations.is_recurring_cancelled,
    donations.utc_recurring_cancelled_at,
    {{ normalize_timestamp('donations.utc_recurring_cancelled_at', 'UTC', 'US/Eastern') }} AS et_recurring_cancelled_at,
    {#- Classifies recurring donations as new, existing or one-time -#}
    CASE
        WHEN donations.is_recurring = 1 AND donations.recurring_gift_seq = 0 THEN 'New'
        WHEN donations.is_recurring = 1 THEN 'Existing'
        WHEN donations.is_recurring = 0 THEN 'One-time'
        ELSE NULL
    END AS recurring_type,

    donations.is_refunded,
    donations.utc_refunded_at,
    {{ normalize_timestamp('donations.utc_refunded_at', 'UTC', 'US/Eastern') }} AS et_refunded_at,

    {#- Flagged Finance donations get the source type 'Finance' -#}
    COALESCE(finance_exclusions.exclude_from_digital, FALSE) AS is_finance_exclusion,
    CASE WHEN finance_exclusions.order_number IS NOT NULL THEN 'Finance'
            ELSE sources.source_type
        END AS source_type,
    CASE WHEN finance_exclusions.order_number IS NOT NULL THEN 'Finance'
            {#- Sends any other source types through the likely_source_type macro for source type to be inferred -#}
            ELSE {{ likely_source_type('sources.source_type', 'donations.refcode', 'donations.form_name') }}
        END AS likely_source_type,
    donations.refcode,
    donations.refcode2,
    donations.form_name,
    donations.form_managing_entity_committee_name,
    donations.form_managing_entity_name,
    donations.ab_test_name,
    donations.ab_test_variation
FROM donations

{# Left joining prevents donations from being lost if a match isn't found #}
LEFT JOIN codes USING (entity_id)
LEFT JOIN finance_exclusions USING (wdl_client_code, order_number)
{# COALESCE treats null refcodes and form_names as empty strings so null to null joins work right #}
LEFT JOIN sources ON (
    codes.wdl_client_code = sources.wdl_client_code
    AND COALESCE(donations.refcode, '') = COALESCE(sources.refcode, '')
    AND COALESCE(donations.form_name, '') = COALESCE(sources.form_name, '')
)
