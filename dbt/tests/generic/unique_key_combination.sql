{# Fails for every combination of `combination_of_columns` that appears more than once. #}
{% test unique_key_combination(model, combination_of_columns) %}

select
    {{ combination_of_columns | join(', ') }},
    count(*) as n_rows
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1

{% endtest %}
