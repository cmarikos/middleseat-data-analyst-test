{# Classifies a donation source channel from available fields. Checks source_type first, then pattern matches refcode and form_name. Creates a fallback for ActBlue Express Donor Dashboard from form_name #}

{% macro likely_source_type(source_type, refcode=none, form_name=none) -%}
{% set search_fields = [refcode, form_name] %}

    CASE 
        {#- If source_type is populated, use it directly -#} 
        WHEN {{ source_type }} IS NOT NULL THEN {{ source_type }}

        {#- Source_type is null, infer source by pattern matching refcode and form_name while standardizing labels -#}
        {% for field in search_fields %}
            WHEN LEFT(lower(replace( {{ field }},'_','-')), 2) = 'em' THEN 'Email'
            WHEN LEFT(lower(replace( {{ field }},'_','-')), 3) = 'ads' THEN 'Ads'
            WHEN lower(replace( {{ field }},'_','-')) ilike '%p2p%' AND lower(replace( {{ field }},'_','-')) ilike '%-rental-%' THEN 'Texting - P2P Rental'
            WHEN lower(replace( {{ field }},'_','-')) ilike '%p2p%' THEN 'Texting - Owned P2P'
            WHEN lower(replace( {{ field }},'_','-')) ilike '%sms%' AND NOT lower(replace( {{ field }},'_','-')) ilike '%p2p%' THEN 'Texting - Broadcast'
            WHEN lower(replace( {{ field }},'_','-')) ilike 'social' THEN 'Social'
            WHEN lower(replace( {{ field }},'_','-')) ilike '%web%' THEN 'Website'
        {% endfor %}

        {#- Named fallback for ActBlue Express from form_name, which doesn't match the pattern rules above -#} 
        WHEN lower({{ form_name }}) = 'actblue express donor dashboard contribution' THEN 'ActBlue Donor Dashboard'
        ELSE NULL
        END

{%- endmacro %}