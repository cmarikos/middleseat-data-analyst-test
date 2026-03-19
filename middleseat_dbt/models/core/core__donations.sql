{%- set schema_pattern = 'dbt_%' -%}
{%- set precore_table_name = 'precore_shopify__orders_v2' -%}
{%- set shopify_tables = get_precore_tables(schema_pattern, precore_table_name, schema_exclude=['dbt_precore'], model_exclude=['precore_shopify__orders_v1']) -%}

{{
    config(
        dist='wdl_transaction_id',
        sort=['wdl_transaction_id'],
    )
}}


WITH

    actblue AS (
        SELECT
            wdl_client_code,
            wdl_transaction_id,
            lineitem_id AS transaction_id,
            committee_name,
            order_number,
            utc_created_at,
            et_created_at,
            EXTRACT(YEAR from et_created_at) AS et_created_year,
            EXTRACT(MONTH from et_created_at) AS et_created_month,
            CAST(
                DATE_TRUNC('month', et_created_at) AS DATE
            ) AS et_created_month_trunc,
            utc_modified_at,
            et_modified_at,
            entity_id,
            amount,
            post_refund_amount,
            first_name,
            last_name,
            email,
            phone,
            address,
            city,
            state,
            zip,
            country,
            is_recurring,
            recurring_type,
            utc_recurring_started_at,
            recurring_gift_seq,
            recurring_period,
            is_recurring_cancelled,
            utc_recurring_cancelled_at,
            et_recurring_cancelled_at,
            is_refunded,
            utc_refunded_at,
            et_refunded_at,
            source_type,
            likely_source_type,
            refcode,
            refcode2,
            form_name,
            form_managing_entity_committee_name,
            form_managing_entity_name,
            ab_test_name,
            ab_test_variation,
            is_finance_exclusion
        FROM {{ ref('precore_actblue__donations') }}
    ),

    shopify_tables_unioned AS (
        {{ dbt_utils.union_relations(
                shopify_tables,
                source_column_name=None
            )
        }}
    ),

    shopify AS (
        SELECT
            wdl_client_code,
            wdl_transaction_id,
            order_id AS transaction_id,
            NULL AS committee_name,
            order_number,
            utc_created_at,
            et_created_at,
            et_created_year,
            et_created_month,
            et_created_month_trunc,
            utc_updated_at AS utc_modified_at,
            et_updated_at AS et_modified_at,
            NULL AS entity_id,
            total_amount AS amount,
            post_refund_total_amount AS post_refund_amount,
            first_name,
            last_name,
            email_address AS email,
            phone_number AS phone,
            address_line_1 AS address,
            city AS city,
            state_code AS state,
            zip_code AS zip,
            country_code AS country,
            FALSE AS is_recurring,
            'One-time' AS recurring_type,
            NULL AS utc_recurring_started_at,
            0 AS recurring_gift_seq,
            NULL AS recurring_period,
            NULL AS is_recurring_cancelled,
            NULL AS utc_recurring_cancelled_at,
            NULL AS et_recurring_cancelled_at,
            CAST(is_refunded AS INTEGER) AS is_refunded,
            NULL AS utc_refunded_at,
            NULL AS et_refunded_at,
            source_type AS source_type,
            likely_source_type,
            refcode,
            NULL AS refcode2,
            form_name,
            NULL AS form_managing_entity_committee_name,
            NULL AS form_managing_entity_name,
            NULL AS ab_test_name,
            NULL AS ab_test_variation,
            FALSE AS is_finance_exclusion
        FROM shopify_tables_unioned
    ),

    unioned AS (
        SELECT * FROM actblue
        UNION ALL
        SELECT * FROM shopify
    )

SELECT
    *
FROM unioned
