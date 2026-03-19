WITH cancellations AS (
  SELECT
      order_number
      , utc_cancelled_at
      , recurring_type
      , recurring_pledged_count
      , recurring_completed_count
    FROM {{ ref('stg_actblue__donations') }}
    WHERE payload_type = 'cancellations'
),
refunds AS (
  SELECT
      lineitem_id
      , utc_disbursed_at
      , utc_recovered_at
      , utc_refunded_at
    FROM {{ ref('stg_actblue__donations') }}
    WHERE payload_type = 'refunds'
)

  SELECT
    lineitem_id
    , committee_name
    , order_number
    , utc_created_at
    , utc_modified_at
    , entity_id
    , fec_id
    , status
    , amount

    , first_name
    , last_name
    , email
    , phone
    , address
    , city
    , state
    , zip
    , country
    , occupation
    , employer
    , employer_address
    , employer_city
    , employer_state
    , employer_country

    , CASE WHEN recurring_period <> 'once' THEN 1 ELSE 0 END AS is_recurring
    , CASE WHEN is_recurring THEN utc_initial_contribution_created_at END AS utc_recurring_started_at
    , recurring_gift_seq
    , recurring_period
    , recurring_duration
    , weekly_recurring_end_date
    , COALESCE(original.recurring_type,cancellations.recurring_type) AS recurring_type
    , COALESCE(original.recurring_pledged_count,cancellations.recurring_pledged_count) AS recurring_pledged_count
    , COALESCE(original.recurring_completed_count,cancellations.recurring_completed_count) AS recurring_completed_count
    , CASE WHEN cancellations.order_number IS NOT NULL THEN 1 ELSE 0 END AS is_recurring_cancelled
    , COALESCE(original.utc_cancelled_at,cancellations.utc_cancelled_at) AS utc_recurring_cancelled_at

    , CASE WHEN refunds.lineitem_id IS NOT NULL THEN 1 ELSE 0 END AS is_refunded
    , COALESCE(original.utc_disbursed_at,refunds.utc_disbursed_at) AS utc_disbursed_at
    , COALESCE(original.utc_recovered_at,refunds.utc_recovered_at) AS utc_recovered_at
    , COALESCE(original.utc_refunded_at,refunds.utc_refunded_at) AS utc_refunded_at

    , refcode
    , CASE WHEN refcode IS NULL THEN 'none' ELSE lower(trim(refcode)) END AS refcode_join_key
    , refcode2
    , email_referral_refcode
    , contribution_form
    , form_kind
    , form_name
    , form_managing_entity_committee_name
    , form_managing_entity_name
    , file_name
    , form_owner_email
    , key
    , ab_test_name
    , ab_test_variation

    , contribution_custom_fields
    , credit_card_expiration
    , smart_boost_upsell_amount
    , is_paypal
    , is_mobile
    , is_express_donor
    , is_express_eligible_donor
    , is_express_lane_donation
    , is_express_signup
    , unique_identifier
    , thanks_url
    , text_message_opt_in
    , gift_declined
    , gift_identifier
    , shipping_name
    , shipping_address
    , shipping_city
    , shipping_state
    , shipping_zip
    , shipping_country

    , payload_type
    , payload_hour
    , payload_day
    , payload_month
    , payload_year
    , payload_entity_id
    , batch_file_name
    , bucket
    , utc_requested_at
    , ROW_NUMBER () OVER (PARTITION BY lineitem_id ORDER BY utc_modified_at DESC, utc_requested_at DESC) AS most_recent

  FROM {{ ref('stg_actblue__donations') }} AS original
  LEFT JOIN cancellations USING (order_number)
  LEFT JOIN refunds USING (lineitem_id)
