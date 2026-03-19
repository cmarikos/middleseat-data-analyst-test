{%- macro get_precore_tables(schema_pattern, model_name, schema_exclude=[], model_include=[], model_exclude=[]) -%}

{%- if execute -%}

    {%- call statement('get_tables', fetch_result=True) -%}
        SELECT distinct
            table_schema,
            table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema ilike '{{ schema_pattern }}'

        {% if target.name != 'dev' -%}
            AND table_schema NOT LIKE '%_dev%'
        {%- endif %}

        {% if schema_exclude -%}
            AND table_schema NOT IN ( 
                {%- for schema in schema_exclude -%}
                    '{{ schema }}'{%- if not loop.last -%},{%- endif -%}
                {%- endfor -%} )
        {%- endif %}

        {% if model_name is sequence and model_name is not string -%}
            AND table_name IN (
                {%- for tbl in model_name -%}
                    '{{ tbl }}'{%- if not loop.last -%},{%- endif -%}
                {%- endfor -%} )
        {% else -%}
            AND (table_name ILIKE '{{ model_name }}'

            {% if model_include -%}
                OR table_name IN (
                {%- for tbl in model_include -%}
                    '{{ tbl }}'{%- if not loop.last -%},{%- endif -%}
                {%- endfor -%} )
            {%- endif %}

            )
        {%- endif %}

        {% if model_exclude -%}
            AND table_name NOT IN ( 
                {%- for tbl in model_exclude -%}
                    '{{ tbl }}'{%- if not loop.last -%},{%- endif -%}
                {%- endfor -%} )
        {%- endif %}

        ORDER BY 1,2
    {%- endcall -%}

    {%- set table_list = load_result('get_tables') -%}

    {%- if table_list and table_list['table'] -%}
        {%- set tbl_relations = [] -%}
        {%- for row in table_list['table'] -%}
            {%- set tbl = api.Relation.create(
                database=database,
                schema=row.table_schema,
                identifier=row.table_name
            ) -%}

            {%- do tbl_relations.append(tbl) -%}
        {%- endfor -%}

        {# {{ log("tables: " ~ tbl_relations, info=True) }} #}
        {{ return(tbl_relations) }}

    {%- else -%}
        {{ log("no tables found.", info=True) }}
        {{ return([]) }}
    {%- endif -%}

{% endif %}

{%- endmacro -%}