{%- macro normalize_timestamp(time_stamp, convert_from_timezone='UTC', convert_to_timezone='UTC', round_to_second=false) -%}

{#- This looks a little wacky because we want to accommodate just about any input we might come across - e.g. if a timestamp has a timezone but is of VARCHAR type, and we cast it straight to a TIMESTAMP we lose the timezone (Actblue for example: 2023-05-09T11:49:03-04:00) -#}

    CONVERT_TIMEZONE('{{ convert_from_timezone }}', '{{ convert_to_timezone }}',
        {%- if round_to_second -%} DATE_TRUNC('second', {%- endif -%}
            CAST(
                CAST(
                    {{ time_stamp }} AS TIMESTAMP with time zone
                ) AS TIMESTAMP
            )
        {%- if round_to_second -%} ) {%- endif -%}
    )

{%- endmacro -%}