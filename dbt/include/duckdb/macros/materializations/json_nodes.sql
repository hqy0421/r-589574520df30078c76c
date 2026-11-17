{#-
  Reversible node relationalization entries for local SQL models.

  json_nodes:  the model outputs (key_column, json_column) document rows and
               is landed as a plain nodes relation (see macros/json_nodes.sql
               for the schema). The nodes table can be queried via ref() and
               modified with plain SQL.

  json_rebuild: the model outputs node rows in the json_nodes shape (usually
               `select * from {{ ref('...') }}` plus optional cleaning SQL)
               and is landed as (key_column, json_column) with one JSON
               document per document key, rebuilt from the *current* node
               rows. Invalid node structures abort the build with a
               descriptive error.

  Both reuse the existing SQL table materialization machinery
  (create_table_as, intermediate/backup relation swap, hooks); existing
  materializations are untouched.
-#}

{% materialization json_nodes, adapter="duckdb", supported_languages=['sql'] %}
  {%- set key_column = config.get('key_column', 'doc_key') -%}
  {%- set json_column = config.get('json_column', 'doc_json') -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set flatten_sql = duckdb__json_nodes_flatten_sql(compiled_code, key_column, json_column) -%}

  -- build model: the model's (key, json) rows land as node rows
  {% call statement('main') -%}
    {{- create_table_as(False, intermediate_relation, flatten_sql, 'sql') }}
  {%- endcall %}

  -- cleanup
  {% if existing_relation is not none %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

{% materialization json_rebuild, adapter="duckdb", supported_languages=['sql'] %}
  {%- set key_column = config.get('key_column', 'doc_key') -%}
  {%- set json_column = config.get('json_column', 'doc_json') -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- validate the current node rows before rebuilding anything
  {%- set validate_sql = duckdb__json_nodes_validate_sql(compiled_code, key_column) -%}
  {% call statement('validate', fetch_result=True) -%}
    {{ validate_sql }}
  {%- endcall %}
  {%- set problems = load_result('validate').table -%}
  {%- if problems | length > 0 -%}
    {% do exceptions.raise_compiler_error(
      "Cannot rebuild JSON documents in model '" ~ model.name
      ~ "': the node rows are invalid (" ~ problems | length ~ " problem(s)):\n  - "
      ~ duckdb__json_nodes_format_problems(problems)) %}
  {%- endif -%}

  {%- set rebuild_sql = duckdb__json_nodes_rebuild_sql(compiled_code, key_column, json_column) -%}

  -- build model: documents are reassembled from the current node rows
  {% call statement('main') -%}
    {{- create_table_as(False, intermediate_relation, rebuild_sql, 'sql') }}
  {%- endcall %}

  -- cleanup
  {% if existing_relation is not none %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
