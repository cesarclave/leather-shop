-- dbt run-operation generate_source --args '{"database_name":"LEATHER_SHOP_DEV", "schema_name":"RAW", "generate_columns":"True", "include_descriptions":"True"}'
{% macro default__generate_source(schema_name, database_name, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, table_names, include_database, include_schema, case_sensitive_databases, case_sensitive_schemas, case_sensitive_tables, case_sensitive_cols) %}

{% set sources_yaml=[] %}
{% do sources_yaml.append('version: 2') %}
{% do sources_yaml.append('') %}
{% do sources_yaml.append('sources:') %}
{% do sources_yaml.append('  - name: ' ~ name | lower) %}

{% if include_descriptions %}
    {% do sources_yaml.append('    description: ""' ) %}
{% endif %}

{% if database_name != target.database or include_database %}
{% do sources_yaml.append('    database: ' ~ (database_name if case_sensitive_databases else database_name | lower)) %}
{% endif %}

{% if schema_name != name or include_schema %}
{% do sources_yaml.append('    schema: ' ~ (schema_name if case_sensitive_schemas else schema_name | lower)) %}
{% endif %}

{% do sources_yaml.append('    tables:') %}

{% if table_names is none %}
{% set tables=codegen.get_tables_in_schema(schema_name, database_name, table_pattern, exclude) %}
{% else %}
{% set tables = table_names %}
{% endif %}

{% for table in tables %}
    {# Récupération du commentaire de la table #}
    {% set table_comment_query %}
        select comment from {{ database_name }}.information_schema.tables 
        where table_schema = '{{ schema_name | upper }}' 
        and table_name = '{{ table | upper }}'
    {% endset %}
    {% set table_comment_results = run_query(table_comment_query) %}
    {% set table_comment = table_comment_results.columns[0].values()[0] if execute and table_comment_results else "" %}

    {% do sources_yaml.append('      - name: ' ~ (table if case_sensitive_tables else table | lower) ) %}
    {% if include_descriptions %}
        {% do sources_yaml.append('        description: "' ~ (table_comment | replace('"', '\\"') if table_comment else "") ~ '"' ) %}
    {% endif %}

    {% if generate_columns %}
    {% do sources_yaml.append('        columns:') %}

        {# Requête SQL pour obtenir les colonnes ET leurs commentaires #}
        {% set column_query %}
            select column_name, data_type, comment
            from {{ database_name }}.information_schema.columns
            where table_schema = '{{ schema_name | upper }}'
              and table_name = '{{ table | upper }}'
            order by ordinal_position
        {% endset %}
        
        {% set results = run_query(column_query) %}
        
        {% if execute %}
            {% for row in results %}
                {% set col_name = row['COLUMN_NAME'] %}
                {% set col_type = row['DATA_TYPE'] %}
                {% set col_comment = row['COMMENT'] %}

                {% do sources_yaml.append('          - name: ' ~ (col_name if case_sensitive_cols else col_name | lower)) %}
                {% if include_data_types %}
                    {% do sources_yaml.append('            data_type: ' ~ col_type | lower) %}
                {% endif %}
                {% if include_descriptions %}
                    {% do sources_yaml.append('            description: "' ~ (col_comment | replace('"', '\\"') if col_comment else "") ~ '"' ) %}
                {% endif %}
            {% endfor %}
        {% endif %}
        {% do sources_yaml.append('') %}
    {% endif %}
{% endfor %}

{% if execute %}
    {% set joined = sources_yaml | join ('\n') %}
    {{ print(joined) }}
    {% do return(joined) %}
{% endif %}

{% endmacro %}