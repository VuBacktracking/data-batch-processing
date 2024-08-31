{#
    this macro returns vendor description
#}

{% macro get_vendor_description(vendor_id) %}
    CASE {{ vendor_id }}
        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN 2 THEN 'VeriFone Inc'
    END
{% endmacro %}