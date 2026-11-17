{#-
  Reversible JSON <-> node relationalization for local SQL models.

  `json_nodes` materialization: a model that outputs document keys and JSON
  documents (columns named by `key_column`/`json_column`, defaults `doc_key`
  and `doc_json`) is landed as a plain nodes relation with one row per JSON
  node:

    <key_column>  - document key, passed through unchanged
    node_id       - bigint, node identity within the document (root is 1)
    parent_id     - bigint, references node_id of the parent (null for the root)
    member_name   - varchar, object member name (set iff the parent is an object)
    array_index   - bigint, 0-based array position (set iff the parent is an array)
    kind          - varchar: 'object' | 'array' | 'string' | 'number' | 'boolean' | 'null'
    value_string  - varchar, set iff kind = 'string'
    value_number  - double,  set iff kind = 'number'
    value_boolean - boolean, set iff kind = 'boolean'

  The nodes relation is a regular table: it can be queried via ref() and
  modified with plain SQL (change values, re-attach subtrees inside their
  document by updating parent_id/member_name/array_index, insert or delete
  node rows). Only node identity, parent references, member names, array
  positions and typed scalar values are stored - never the original JSON
  text or materialized paths - so the `json_rebuild` materialization always
  reconstructs documents from the *current* node rows.

  `json_rebuild` materialization: a model that outputs node rows in the
  shape above is validated and landed as (<key_column>, <json_column>) with
  one rebuilt JSON document per document key. String/number/boolean/null,
  missing members and empty containers are kept distinct, and arrays follow
  the current array_index order. Invalid node rows (orphans, cycles,
  duplicate members or array positions, scalars with children, ...) abort
  the build with a descriptive error.

  Note: numbers are stored as DOUBLE, so rebuilding normalizes the lexical
  form of integers (1 comes back as 1.0); the JSON type of every value is
  preserved.
-#}

{% macro duckdb__json_nodes_flatten_sql(model_sql, key_column='doc_key', json_column='doc_json') %}
with recursive __src as (
  select {{ key_column }} as __doc_key, {{ json_column }}::json as __doc_json
  from ({{ model_sql }}) as __model
),
__walk as (
  select
    __doc_key,
    cast(null as varchar[]) as __parent_path,
    cast([] as varchar[]) as __path,
    cast(null as varchar) as member_name,
    cast(null as bigint) as array_index,
    __doc_json as __j
  from __src
  union all
  select
    w.__doc_key,
    w.__path,
    list_append(w.__path, c.__tag),
    c.member_name,
    c.array_index,
    c.__child_j
  from __walk w
  cross join lateral (
    select ('o' || k.key) as __tag, k.key as member_name, cast(null as bigint) as array_index, (w.__j -> k.key) as __child_j
    from unnest(json_keys(w.__j)) as k(key)
    where json_type(w.__j) = 'OBJECT'
    union all
    select ('a' || i.idx::varchar), null, i.idx, (w.__j -> i.idx)
    from unnest(generate_series(0, json_array_length(w.__j)::bigint - 1)) as i(idx)
    where json_type(w.__j) = 'ARRAY'
  ) as c
),
__nodes as (
  select
    __doc_key,
    row_number() over (partition by __doc_key order by len(__path), to_json(__path)) as node_id,
    __path,
    __parent_path,
    member_name,
    array_index,
    __j
  from __walk
)
select
  n.__doc_key as {{ key_column }},
  n.node_id,
  p.node_id as parent_id,
  n.member_name,
  n.array_index,
  case
    when json_type(n.__j) is null then 'null'
    when json_type(n.__j) = 'OBJECT' then 'object'
    when json_type(n.__j) = 'ARRAY' then 'array'
    when json_type(n.__j) = 'VARCHAR' then 'string'
    when json_type(n.__j) = 'BOOLEAN' then 'boolean'
    when json_type(n.__j) = 'NULL' then 'null'
    else 'number'
  end as kind,
  case when json_type(n.__j) = 'VARCHAR' then n.__j ->> '$' end as value_string,
  case when json_type(n.__j) in ('BIGINT', 'UBIGINT', 'DOUBLE') then (n.__j ->> '$')::double end as value_number,
  case when json_type(n.__j) = 'BOOLEAN' then (n.__j ->> '$')::boolean end as value_boolean
from __nodes n
left join __nodes p on p.__doc_key = n.__doc_key and p.__path = n.__parent_path
{% endmacro %}

{% macro duckdb__json_nodes_rebuild_sql(nodes_sql, key_column='doc_key', json_column='doc_json') %}
with recursive __src as (
  select
    {{ key_column }} as __doc_key,
    node_id,
    parent_id,
    member_name,
    array_index,
    kind,
    value_string,
    value_number,
    value_boolean
  from ({{ nodes_sql }}) as __model
),
__leafj as (
  -- nodes whose JSON is known without looking at any other row:
  -- scalars and empty containers
  select
    __doc_key,
    node_id,
    case kind
      when 'string' then to_json(value_string)
      when 'number' then to_json(value_number)
      when 'boolean' then to_json(value_boolean)
      when 'null' then 'null'::json
      when 'object' then '{}'::json
      when 'array' then '[]'::json
    end as __j
  from __src
  where kind not in ('object', 'array')
     or not exists (
       select 1 from __src c
       where c.__doc_key = __src.__doc_key and c.parent_id = __src.node_id
     )
),
__forest(__iter, __m) as (
  -- fixpoint assembly: __m maps 'doc_key:node_id' -> assembled JSON subtree.
  -- each iteration adds every container whose children are all in the map
  select 0, map(list(__ref), list(__j))
  from (select __doc_key::varchar || ':' || node_id::varchar as __ref, __j from __leafj)
  union all
  select
    f.__iter + 1,
    map_concat(f.__m, (
      select map(list(__ref), list(__j))
      from (
        select
          p.__doc_key::varchar || ':' || p.node_id::varchar as __ref,
          case p.kind
            when 'object' then to_json(map(
              list(c.member_name order by c.node_id),
              list(f.__m[c.__doc_key::varchar || ':' || c.node_id::varchar] order by c.node_id)))
            when 'array' then to_json(list(
              f.__m[c.__doc_key::varchar || ':' || c.node_id::varchar] order by c.array_index))
          end as __j
        from __src p
        join __src c on c.__doc_key = p.__doc_key and c.parent_id = p.node_id
        where p.kind in ('object', 'array')
          and not map_contains(f.__m, p.__doc_key::varchar || ':' || p.node_id::varchar)
        group by p.__doc_key, p.node_id, p.kind
        having count(*) filter (map_contains(f.__m, c.__doc_key::varchar || ':' || c.node_id::varchar)) = count(*)
      ) as __new
    ))
  from __forest f
  where exists (
    select 1
    from __src p
    join __src c on c.__doc_key = p.__doc_key and c.parent_id = p.node_id
    where p.kind in ('object', 'array')
      and not map_contains(f.__m, p.__doc_key::varchar || ':' || p.node_id::varchar)
    group by p.__doc_key, p.node_id
    having count(*) filter (map_contains(f.__m, c.__doc_key::varchar || ':' || c.node_id::varchar)) = count(*)
  )
)
select
  r.__doc_key as {{ key_column }},
  f.__m[r.__ref] as {{ json_column }}
from (
  select distinct __doc_key, __doc_key::varchar || ':' || node_id::varchar as __ref
  from __src
  where parent_id is null
) as r
cross join (select __m from __forest order by __iter desc limit 1) as f
{% endmacro %}

{% macro duckdb__json_nodes_validate_sql(nodes_sql, key_column='doc_key') %}
with recursive __src as (
  select
    {{ key_column }} as __doc_key,
    node_id,
    parent_id,
    member_name,
    array_index,
    kind,
    value_string,
    value_number,
    value_boolean
  from ({{ nodes_sql }}) as __model
),
__reach as (
  -- nodes reachable from a document root; 'union' dedups so cycles terminate
  select __doc_key, node_id
  from __src
  where parent_id is null
  union
  select s.__doc_key, s.node_id
  from __src s
  join __reach r on r.__doc_key = s.__doc_key and r.node_id = s.parent_id
),
__problems as (
  select 'document key must not be null' as check_name, cast(null as varchar) as __doc_key, cast(null as bigint) as node_id,
         'rows=' || count(*)::varchar as detail
  from __src
  where __doc_key is null
  having count(*) > 0
  union all
  select 'unknown node kind', __doc_key::varchar, node_id, 'kind=' || coalesce(kind, 'NULL')
  from __src
  where kind not in ('object', 'array', 'string', 'number', 'boolean', 'null') or kind is null
  union all
  select 'duplicate node id', __doc_key::varchar, node_id, 'count=' || count(*)::varchar
  from __src
  group by __doc_key, node_id
  having count(*) > 1
  union all
  select 'document must have exactly one root', __doc_key::varchar, null,
         'roots=' || count(*) filter (parent_id is null)::varchar
  from __src
  group by __doc_key
  having count(*) filter (parent_id is null) <> 1
  union all
  select 'orphan node', s.__doc_key::varchar, s.node_id, 'parent_id=' || s.parent_id::varchar || ' not found'
  from __src s
  where s.parent_id is not null
    and not exists (select 1 from __src p where p.__doc_key = s.__doc_key and p.node_id = s.parent_id)
  union all
  select 'cycle detected', s.__doc_key::varchar, s.node_id, 'node is not reachable from the document root'
  from __src s
  where not exists (select 1 from __reach r where r.__doc_key = s.__doc_key and r.node_id = s.node_id)
  union all
  select 'duplicate object member', __doc_key::varchar, parent_id, 'member=' || member_name
  from __src
  where member_name is not null
  group by __doc_key, parent_id, member_name
  having count(*) > 1
  union all
  select 'duplicate array position', __doc_key::varchar, parent_id, 'position=' || array_index::varchar
  from __src
  where array_index is not null
  group by __doc_key, parent_id, array_index
  having count(*) > 1
  union all
  select 'scalar node has children', p.__doc_key::varchar, p.node_id, 'kind=' || p.kind
  from __src p
  where p.kind in ('string', 'number', 'boolean', 'null')
    and exists (select 1 from __src c where c.__doc_key = p.__doc_key and c.parent_id = p.node_id)
  union all
  select 'object child must have member_name and no array_index', c.__doc_key::varchar, c.node_id, 'parent kind=object'
  from __src c
  join __src p on p.__doc_key = c.__doc_key and p.node_id = c.parent_id
  where p.kind = 'object' and (c.member_name is null or c.array_index is not null)
  union all
  select 'array child must have array_index and no member_name', c.__doc_key::varchar, c.node_id, 'parent kind=array'
  from __src c
  join __src p on p.__doc_key = c.__doc_key and p.node_id = c.parent_id
  where p.kind = 'array' and (c.array_index is null or c.member_name is not null)
  union all
  select 'root node must not have member_name or array_index', __doc_key::varchar, node_id, ''
  from __src
  where parent_id is null and (member_name is not null or array_index is not null)
  union all
  select 'array_index must be non-negative', __doc_key::varchar, node_id, 'array_index=' || array_index::varchar
  from __src
  where array_index < 0
  union all
  select 'scalar node missing value', __doc_key::varchar, node_id, 'kind=' || kind || ' requires a non-null value'
  from __src
  where (kind = 'string' and value_string is null)
     or (kind = 'number' and value_number is null)
     or (kind = 'boolean' and value_boolean is null)
)
select check_name, __doc_key as doc_key, node_id, detail
from __problems
order by check_name, doc_key, node_id
{% endmacro %}

{% macro duckdb__json_nodes_format_problems(problems) %}
  {%- set grouped = {} -%}
  {%- for row in problems -%}
    {%- set check = row[0] -%}
    {%- if check not in grouped -%}{%- do grouped.update({check: []}) -%}{%- endif -%}
    {%- do grouped[check].append(row) -%}
  {%- endfor -%}
  {%- set lines = [] -%}
  {%- for check, rows in grouped.items() -%}
    {%- set examples = [] -%}
    {%- for r in rows[:3] -%}
      {%- do examples.append('(doc_key=' ~ r[1] ~ ', node_id=' ~ r[2] ~ ')' ~ (' ' ~ r[3] if r[3] else '')) -%}
    {%- endfor -%}
    {%- do lines.append(check ~ ' (' ~ rows | length ~ ' row(s)): ' ~ examples | join('; ') ~ ('; ...' if rows | length > 3 else '')) -%}
  {%- endfor -%}
  {{ return(lines | join('\n  - ')) }}
{% endmacro %}
