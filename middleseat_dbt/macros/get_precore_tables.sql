{# Dynamically discovers and returns a list of database tables matching the given schema and model patterns. Finds client source tables without hardcoding them. #}

{%- macro get_precore_tables(schema_pattern, model_name, schema_exclude=[], model_include=[], model_exclude=[]) -%}

{#- Execute is only true during the second pass when a database connection exists -#}
{#- without this guard the INFORMATION_SCHEMA query would fail during dependency graph compilation -#}
{%- if execute -%}

    {%- call statement('get_tables', fetch_result=True) -%}
        SELECT distinct
            table_schema,
            table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema ilike '{{ schema_pattern }}'

        {#- Prevents dev schemas from ending up in production runs -#}
        {% if target.name != 'dev' -%}
            AND table_schema NOT LIKE '%_dev%'
        {%- endif %}
        
        {#- Can pass schemas to explicitly exclude using schema_exclude -#}
        {% if schema_exclude -%}
            AND table_schema NOT IN ( 
                {%- for schema in schema_exclude -%}
                    '{{ schema }}'{%- if not loop.last -%},{%- endif -%}
                {%- endfor -%} )
        {%- endif %}
        
        {#- model_name can be a list of exact names or a string -#}
        {#- model_include allows additional tables to be added by exact name -#}
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

        {#- Can pass specific tables to exclude in model_exclude -#}
        {% if model_exclude -%}
            AND table_name NOT IN ( 
                {%- for tbl in model_exclude -%}
                    '{{ tbl }}'{%- if not loop.last -%},{%- endif -%}
                {%- endfor -%} )
        {%- endif %}

        ORDER BY 1,2
    {%- endcall -%}

    {#- Store results from get_tables in a Jinja accessible variable -#}
    {#- See https://docs.getdbt.com/reference/dbt-jinja-functions/statement-blocks -#}
    {%- set table_list = load_result('get_tables') -%}

    {#- Converts each row in table_list into a relation, appends each item in a list stored as tbl_relations -#}
    {#- See https://docs.getdbt.com/reference/dbt-classes for api.Relation.create docs -#}
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
        
        {#- Uncomment next line to log discovered tables -#}
        {#- {{ log("tables: " ~ tbl_relations, info=True) }} -#}
        {{ return(tbl_relations) }}
    
    {#- If there are no results, instead of failing return a 'no tables found' warning -#}
    {%- else -%}
        {{ log("no tables found.", info=True) }}
        {{ return([]) }}
    {%- endif -%}

{% endif %}

{%- endmacro -%}