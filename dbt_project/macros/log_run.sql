{% macro log_run_results(results) %}

{% for res in results %}
    {% if res.node.resource_type == 'model' %}

        {% set started_at = res.timing[0].started_at.strftime("%Y-%m-%d %H:%M:%S") if res.timing and res.timing[0].started_at else None %}
        {% set finished_at = res.timing[-1].completed_at.strftime("%Y-%m-%d %H:%M:%S") if res.timing and res.timing[-1].completed_at else None %}

        {% set row_count = res.adapter_response.get('rows_affected', 0) %}

        {% set sql %}
            INSERT INTO capstone.public.dbt_logs (
                run_id,
                model_name,
                environment,
                status,
                started_at,
                finished_at,
                duration_seconds,
                row_count
            )
            SELECT
                '{{ invocation_id }}' AS run_id,
                '{{ res.node.name }}' AS model_name,
                '{{ target.name }}' AS environment,
                '{{ res.status }}' AS status,
                '{{ started_at }}'::timestamp_ntz AS started_at,
                '{{ finished_at }}'::timestamp_ntz AS finished_at,
                DATEDIFF(
                    'second',
                    '{{ started_at }}'::timestamp_ntz,
                    '{{ finished_at }}'::timestamp_ntz
                ) AS duration_seconds,
                {{ row_count }} AS row_count
        {% endset %}

        {% do run_query(sql) %}

    {% endif %}
{% endfor %}

{% endmacro %}