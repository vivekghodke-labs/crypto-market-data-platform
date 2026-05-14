-- ###############################################################################
-- # Overrides dbts default schema naming behaviour.
-- #
-- # dbt default: appends target schema prefix to custom schema config.
-- #   e.g., schema config = "silver_curated" → actual schema = "dev_silver_curated"
-- #
-- # This override: uses the custom schema name exactly as declared in
-- # dbt_project.yml — no prefix appended.
-- #   e.g., schema config = "silver_curated" → actual schema = "silver_curated"
-- #
-- # This ensures BigQuery dataset names are consistent across dev and prod
-- # targets, which is required for Looker Studio to resolve tables correctly
-- # without environment-specific dashboard reconfiguration.
-- #
-- # Reference: https://docs.getdbt.com/docs/build/custom-schemas
-- ###############################################################################

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}