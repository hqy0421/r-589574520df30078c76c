"""
Tests for the reversible JSON <-> node relationalization entries:
- `json_nodes` materialization lands (doc_key, doc_json) model output as a
  plain nodes relation (node identity, parent reference, member name, array
  position, typed scalar values) that can be queried via ref() and modified
  with plain SQL.
- `json_rebuild` materialization validates the *current* node rows and
  rebuilds queryable JSON documents from them, keeping string/number/
  boolean/null/missing/empty-container distinctions and array order.
"""
import json

import pytest

from dbt.tests.util import run_dbt

# A document exercising every JSON kind, empty containers, a JSON null,
# unicode text and a nested array.
DOC1 = '{"name": "订单A", "tags": ["a", "b", "c"], "meta": {"vip": true, "note": null}, "empty_obj": {}, "empty_arr": [], "price": 9.99, "count": 3}'
DOC2 = '[1, "two", false, null, {"x": []}]'
DOC3 = '"plain"'
DOC4 = 'null'
DOC5 = '{}'

models__docs = """
{{ config(materialized='json_nodes') }}
select * from (values
    (1, '%s'::json),
    (2, '%s'::json),
    (3, '%s'::json),
    (4, '%s'::json),
    (5, '%s'::json)
) as t(doc_key, doc_json)
""" % (DOC1, DOC2, DOC3, DOC4, DOC5)

models__rebuilt = """
{{ config(materialized='json_rebuild') }}
select * from {{ ref('docs') }}
"""

# A plain downstream model: the nodes relation must be queryable via ref()
models__node_strings = """
{{ config(materialized='table') }}
select doc_key, node_id, value_string
from {{ ref('docs') }}
where kind = 'string'
"""

# The cleaning step: SQL over the current node rows - change a value,
# re-attach a subtree inside its document, add a node, delete a node.
models__rebuilt_cleaned = """
{{ config(materialized='json_rebuild') }}
with nodes as (
    select * from {{ ref('docs') }}
),
cleaned as (
    select
        doc_key,
        node_id,
        -- re-attach: move 'meta' under 'empty_obj'
        case when doc_key = 1 and member_name = 'meta'
             then (select node_id from nodes where doc_key = 1 and member_name = 'empty_obj')
             else parent_id end as parent_id,
        case when doc_key = 1 and member_name = 'meta' then 'moved_meta' else member_name end as member_name,
        array_index,
        kind,
        value_string,
        -- change a value
        case when doc_key = 1 and member_name = 'price' then 19.99 else value_number end as value_number,
        value_boolean
    from nodes
    -- delete a node: drop tags[1] (identified by node_id to avoid null traps)
    where not (doc_key = 1 and node_id = (
        select c.node_id from nodes c
        where c.doc_key = 1 and c.array_index = 1
          and c.parent_id = (select node_id from nodes where doc_key = 1 and member_name = 'tags')))
    union all
    -- add a node: new member 'extra' under the root of document 1
    select 1, 1000, 1, 'extra', null, 'string', '新增', null, null
)
select * from cleaned
"""

models__empty_docs = """
{{ config(materialized='json_nodes') }}
select * from (values (1, '{}'::json)) as t(doc_key, doc_json)
where false
"""

models__empty_rebuilt = """
{{ config(materialized='json_rebuild') }}
select * from {{ ref('empty_docs') }}
"""

# custom column names; the payload is a plain VARCHAR (the flatten casts it)
models__custom_keys = """
{{ config(materialized='json_nodes', key_column='order_id', json_column='payload') }}
select * from (values ('o1', '{"a": [true]}')) as t(order_id, payload)
"""

models__custom_keys_rebuilt = """
{{ config(materialized='json_rebuild', key_column='order_id', json_column='payload') }}
select * from {{ ref('custom_keys') }}
"""

NODE_COLUMNS = [
    "doc_key", "node_id", "parent_id", "member_name", "array_index",
    "kind", "value_string", "value_number", "value_boolean",
]


@pytest.mark.skip_profile("buenavista")
class TestJsonNodes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "docs.sql": models__docs,
            "rebuilt.sql": models__rebuilt,
            "node_strings.sql": models__node_strings,
            "rebuilt_cleaned.sql": models__rebuilt_cleaned,
            "empty_docs.sql": models__empty_docs,
            "empty_rebuilt.sql": models__empty_rebuilt,
            "custom_keys.sql": models__custom_keys,
            "custom_keys_rebuilt.sql": models__custom_keys_rebuilt,
        }

    def test_json_nodes(self, project):
        # run twice to exercise the intermediate/backup swap on existing relations
        for _ in range(2):
            results = run_dbt(["run"])
            assert len(results) == 8
        schema = project.test_schema

        # --- the nodes relation landed with the node schema ---
        cols = project.run_sql(f"describe {schema}.docs", fetch="all")
        assert [c[0] for c in cols] == NODE_COLUMNS
        assert [c[1] for c in cols] == [
            "INTEGER", "BIGINT", "BIGINT", "VARCHAR", "BIGINT",
            "VARCHAR", "VARCHAR", "DOUBLE", "BOOLEAN",
        ]

        # --- node rows carry identity / parent / member / position / typed values ---
        rows = project.run_sql(
            f"""
            select node_id, parent_id, member_name, array_index, kind,
                   value_string, value_number, value_boolean
            from {schema}.docs
            where doc_key = 1
            order by node_id
            """,
            fetch="all",
        )
        by_member = {r[2]: r for r in rows if r[2] is not None}
        # root
        assert rows[0] == (1, None, None, None, "object", None, None, None)
        # typed scalars
        assert by_member["name"][4:] == ("string", "订单A", None, None)
        assert by_member["price"][4:] == ("number", None, 9.99, None)
        assert by_member["count"][4:] == ("number", None, 3.0, None)
        # empty containers keep their kind
        assert by_member["empty_obj"][4] == "object"
        assert by_member["empty_arr"][4] == "array"
        # array elements: parent reference + 0-based position
        tags_id = by_member["tags"][0]
        elements = [r for r in rows if r[1] == tags_id]
        assert [(r[3], r[5]) for r in elements] == [(0, "a"), (1, "b"), (2, "c")]
        # JSON null is a node of kind 'null', distinct from a missing member
        meta_id = by_member["meta"][0]
        note = [r for r in rows if r[1] == meta_id and r[2] == "note"][0]
        assert note[4] == "null"
        vip = [r for r in rows if r[1] == meta_id and r[2] == "vip"][0]
        assert vip[4:] == ("boolean", None, None, True)

        # --- the nodes relation is queryable via ref() ---
        strings = project.run_sql(
            f"select value_string from {schema}.node_strings order by 1", fetch="all"
        )
        assert [r[0] for r in strings] == ["a", "b", "c", "plain", "two", "订单A"]

        # --- rebuild from the current node rows round-trips the documents ---
        rebuilt = project.run_sql(
            f"select doc_key, doc_json from {schema}.rebuilt order by doc_key",
            fetch="all",
        )
        assert len(rebuilt) == 5
        for (doc_key, doc_json), expected in zip(rebuilt, [DOC1, DOC2, DOC3, DOC4, DOC5]):
            assert json.loads(doc_json) == json.loads(expected)

        # --- SQL cleaning over the node rows is reflected in the rebuild ---
        cleaned = project.run_sql(
            f"select doc_json from {schema}.rebuilt_cleaned where doc_key = 1",
            fetch="one",
        )[0]
        expected = json.loads(DOC1)
        expected["price"] = 19.99                                   # changed value
        expected["empty_obj"] = {"moved_meta": expected.pop("meta")}  # re-attached subtree
        expected["tags"] = ["a", "c"]                               # deleted node
        expected["extra"] = "新增"                                   # added node
        assert json.loads(cleaned) == expected

        # --- empty input lands with correct metadata on both entries ---
        empty_nodes_cols = project.run_sql(f"describe {schema}.empty_docs", fetch="all")
        assert [c[0] for c in empty_nodes_cols] == NODE_COLUMNS
        empty_rebuilt_cols = project.run_sql(f"describe {schema}.empty_rebuilt", fetch="all")
        assert [c[0] for c in empty_rebuilt_cols] == ["doc_key", "doc_json"]
        assert empty_rebuilt_cols[1][1] == "JSON"
        assert project.run_sql(
            f"select count(*) from {schema}.empty_rebuilt", fetch="one"
        )[0] == 0

        # --- custom column names round-trip too ---
        custom = project.run_sql(f"select * from {schema}.custom_keys_rebuilt", fetch="all")
        assert len(custom) == 1
        assert json.loads(custom[0][1]) == {"a": [True]}


# Each of these models lands invalid node rows; the rebuild must fail clearly.
bad_models = {
    "orphan": """
        select * from {{ ref('base_docs') }}
        union all
        select 1, 999, 888, 'lost', null, 'string', 'x', null, null
    """,
    "cycle": """
        select doc_key, node_id,
               case when node_id = 1 then 2 else parent_id end as parent_id,
               member_name, array_index, kind, value_string, value_number, value_boolean
        from {{ ref('base_docs') }}
    """,
    "duplicate_member": """
        select * from {{ ref('base_docs') }}
        union all
        select 1, 999, 1, 'a', null, 'number', null, 2.0, null
    """,
    "duplicate_array_position": """
        select * from {{ ref('base_docs') }}
        union all
        select 1, 999, 2, null, 0, 'number', null, 2.0, null
    """,
    "scalar_with_children": """
        select * from {{ ref('base_docs') }}
        union all
        select 1, 999, 3, null, null, 'number', null, 1.0, null
    """,
}

expected_errors = {
    "orphan": "orphan node",
    "cycle": "cycle detected",
    "duplicate_member": "duplicate object member",
    "duplicate_array_position": "duplicate array position",
    "scalar_with_children": "scalar node has children",
}

base_docs_model = """
{{ config(materialized='json_nodes') }}
select * from (values (1, '{"a": [10, 20], "b": "text"}'::json)) as t(doc_key, doc_json)
"""


@pytest.mark.skip_profile("buenavista")
class TestJsonRebuildInvalid:
    @pytest.fixture(scope="class")
    def models(self):
        m = {"base_docs.sql": base_docs_model}
        for name, sql in bad_models.items():
            m[f"bad_{name}.sql"] = "{{ config(materialized='json_rebuild') }}\n" + sql
        return m

    def test_invalid_node_rows_fail_clearly(self, project):
        run_dbt(["run", "--select", "base_docs"])
        for name in bad_models:
            results = run_dbt(["run", "--select", f"bad_{name}"], expect_pass=False)
            assert len(results) == 1
            assert results[0].status == "error"
            message = results[0].message
            assert "Cannot rebuild JSON documents" in message
            assert expected_errors[name] in message
        # the failed runs must not have landed anything
        assert project.run_sql(
            "select count(*) from system.information_schema.tables "
            "where table_schema = '%s' and table_name = 'bad_orphan'" % project.test_schema,
            fetch="one",
        )[0] == 0
