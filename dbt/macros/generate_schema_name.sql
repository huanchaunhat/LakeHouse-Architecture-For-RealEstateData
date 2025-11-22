{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
    Custom schema generation for 3-layer Medallion Architecture:
    - staging models → silver database (cleaned, validated)
    - marts models → gold database (business-ready)
    #}
    
    {%- set default_schema = target.schema -%}
    
    {#- If model is in staging folder, use silver -#}
    {%- if 'staging' in node.fqn -%}
        silver
    
    {#- Otherwise use the target schema from profiles.yml (gold) -#}
    {%- else -%}
        {{ default_schema }}
    
    {%- endif -%}

{%- endmacro %}