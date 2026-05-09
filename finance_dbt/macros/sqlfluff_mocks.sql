{% macro config(**kwargs) %}{% endmacro %}
{% macro ref(model_name) %}{{ model_name }}{% endmacro %}
{% macro source(source_name, table_name) %}{{ source_name }}_{{ table_name }}{% endmacro %}
{% macro is_incremental() %}false{% endmacro %}