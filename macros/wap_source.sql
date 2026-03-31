{% macro wap_source(source_name, table_name) %}
    {#-
        Branch-aware source resolver for WAP (Write-Audit-Publish) quality gates.

        When ICEBERG_WAP_BRANCH is set, resolves the branch's current snapshot ID
        from the Iceberg $refs metadata table and injects a
        `FOR VERSION AS OF <snapshot_id>` clause so that dbt models and tests
        read from the run branch rather than the main branch tip.

        When ICEBERG_WAP_BRANCH is unset (normal serving mode), falls back to
        the standard {{ source() }} relation so models query main as usual.

        The `execute` guard is required because run_query() returns None during
        dbt's parse phase. During parse, we fall back to the plain source
        relation so compilation succeeds; the snapshot lookup only runs during
        the execution phase when a real DB connection is available.

        Trino does not support the $branch=name table suffix or the
        AT (BRANCH =>) syntax in this version, so snapshot-ID-based time travel
        via FOR VERSION AS OF is the only supported mechanism.
    -#}
    {%- set branch = env_var('ICEBERG_WAP_BRANCH', '') -%}
    {%- if branch != '' and execute -%}
        {%- set src = source(source_name, table_name) -%}
        {%- set refs_query -%}
            SELECT snapshot_id
            FROM {{ src.database }}.{{ src.schema }}."{{ table_name }}$refs"
            WHERE name = '{{ branch }}'
        {%- endset -%}
        {%- set results = run_query(refs_query) -%}
        {%- if results.rows | length == 0 -%}
            {{ exceptions.raise_compiler_error(
                "wap_source: branch '" ~ branch ~ "' not found in "
                ~ src.database ~ "." ~ src.schema ~ "." ~ table_name
                ~ " — ensure create_branch ran before quality gates."
            ) }}
        {%- endif -%}
        {%- set snapshot_id = results.columns[0].values()[0] -%}
        (SELECT * FROM {{ src }} FOR VERSION AS OF {{ snapshot_id }})
    {%- else -%}
        {{ source(source_name, table_name) }}
    {%- endif -%}
{% endmacro %}
