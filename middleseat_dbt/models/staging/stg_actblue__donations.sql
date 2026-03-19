{{
    config(
        dist='lineitem_id',
        sort=['utc_modified_at', 'utc_requested_at'],
        tags=['staging', 'actblue', 'donations', 'core__donations']

    )
}}


WITH
    source AS (

        SELECT * FROM {{ source('actblue', 'webhooks') }}

    ),

    renamed AS (

        SELECT
            lineitems_lineitem_id AS lineitem_id,
            lineitems_committee_name AS committee_name,
            contribution_order_number AS order_number,
            utilities.TO_UTC(lineitems_paid_at) AS utc_created_at,
            utilities.TO_UTC(GREATEST(lineitems_paid_at,lineitems_refunded_at,contribution_cancelled_at)) AS utc_modified_at,
            lineitems_entity_id AS entity_id,
            lineitems_fec_id AS fec_id,
            contribution_status AS status,
            CAST(REPLACE(lineitems_amount, ',', '') AS DECIMAL(20,2)) AS amount,

            donor_firstname AS first_name,
            donor_lastname AS last_name,
            utilities.CLEAN_EMAIL_ADDRESS(donor_email) AS email,
            utilities.CLEAN_PHONE_NUMBER(donor_phone) AS phone,
            donor_addr1 AS address,
            donor_city AS city,
            donor_state AS state,
            donor_zip AS zip,
            donor_country AS country,
            donor_employer_data_occupation AS occupation,
            donor_employer_data_employer AS employer,
            donor_employer_data_employer_addr1 AS employer_address,
            donor_employer_data_employer_city AS employer_city,
            donor_employer_data_employer_state AS employer_state,
            donor_employer_data_employer_country AS employer_country,

            lineitems_sequence AS recurring_gift_seq,
            contribution_recurring_period AS recurring_period,
            contribution_recurring_duration AS recurring_duration,
            contribution_weekly_recurring_sunset AS weekly_recurring_end_date,
            contribution_recurring_type AS recurring_type,
            contribution_recur_pledged AS recurring_pledged_count,
            contribution_recur_completed AS recurring_completed_count,
            utilities.TO_UTC(contribution_cancelled_at) AS utc_cancelled_at,

            utilities.TO_UTC(lineitems_disbursed_at) AS utc_disbursed_at,
            utilities.TO_UTC(lineitems_recovered_at) AS utc_recovered_at,
            utilities.TO_UTC(lineitems_refunded_at) AS utc_refunded_at,

            LOWER(TRIM(contribution_refcodes_refcode)) AS refcode,
            LOWER(TRIM(contribution_refcodes_refcode2)) AS refcode2,

            contribution_refcodes_refcode_email_referrer AS email_referral_refcode,
            contribution_contribution_form AS contribution_form,
            form_kind,
            LOWER(TRIM(form_name)) AS form_name,
            form_managing_entity_committee_name,
            form_managing_entity_name,
            file_name,
            form_owner_email,
            key,
            contribution_ab_test_name AS ab_test_name,
            contribution_ab_test_variation AS ab_test_variation,

            utilities.TO_UTC(contribution_created_at) AS utc_initial_contribution_created_at,
            contribution_custom_fields,
            contribution_credit_card_expiration AS credit_card_expiration,
            CAST(REPLACE(contribution_smart_boost_amount, ',', '') AS DECIMAL(20,2)) AS smart_boost_upsell_amount,
            contribution_is_paypal AS is_paypal,
            contribution_is_mobile AS is_mobile,
            contribution_is_express AS is_express_donor,
            donor_is_eligible_for_express_lane AS is_express_eligible_donor,
            contribution_with_express_lane AS is_express_lane_donation,
            contribution_express_signup AS is_express_signup,
            contribution_unique_identifier AS unique_identifier,
            contribution_thanks_url AS thanks_url,
            contribution_text_message_option AS text_message_opt_in,
            contribution_gift_declined AS gift_declined,
            contribution_gift_identifier AS gift_identifier,
            contribution_shipping_name AS shipping_name,
            contribution_shipping_addr1 AS shipping_address,
            contribution_shipping_city AS shipping_city,
            contribution_shipping_state AS shipping_state,
            contribution_shipping_zip AS shipping_zip,
            contribution_shipping_country AS shipping_country,

            payload_type,
            payload_hour,
            payload_day,
            payload_month,
            payload_year,
            payload_entity_id,
            batch_file_name,
            bucket,
            utilities.TO_UTC(utc_requested_at) AS utc_requested_at

        FROM source
    )

SELECT * FROM renamed
