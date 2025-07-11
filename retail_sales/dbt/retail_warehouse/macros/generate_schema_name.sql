{% macro generate_schema_name(custom_schema_name, node) %}
    {#-- If the model/yaml supplies +schema:, use it directly --#}
    {% if custom_schema_name is not none %}
        {{ return(custom_schema_name) }}
    {% else %}
        {{ return(target.schema) }}
    {% endif %}
{% endmacro %}
