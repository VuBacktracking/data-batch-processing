{#
    this macro returns rate_code description
#}

{% macro get_rate_code_description(rate_code_id) %}
    CASE {{ rate_code_id }}
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negoriated fare'
        WHEN 6 THEN 'Group ride'
    END
{% endmacro %}