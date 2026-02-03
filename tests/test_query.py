"""Tests for SquirrelDB Python SDK - Query Builder"""

import pytest
from squirreldb.query import (
    table,
    field_expr as field,
    and_,
    or_,
    not_,
    FieldExpr,
    QueryBuilder,
)


class TestFieldExpr:
    """Test FieldExpr operators"""

    def test_eq_creates_equal_condition(self):
        cond = field("age").eq(25)
        assert cond.field == "age"
        assert cond.operator == "$eq"
        assert cond.value == 25

    def test_ne_creates_not_equal_condition(self):
        cond = field("status").ne("inactive")
        assert cond.operator == "$ne"
        assert cond.value == "inactive"

    def test_gt_creates_greater_than_condition(self):
        cond = field("price").gt(100)
        assert cond.operator == "$gt"
        assert cond.value == 100

    def test_gte_creates_greater_than_or_equal_condition(self):
        cond = field("count").gte(10)
        assert cond.operator == "$gte"
        assert cond.value == 10

    def test_lt_creates_less_than_condition(self):
        cond = field("age").lt(18)
        assert cond.operator == "$lt"
        assert cond.value == 18

    def test_lte_creates_less_than_or_equal_condition(self):
        cond = field("rating").lte(5)
        assert cond.operator == "$lte"
        assert cond.value == 5

    def test_in_creates_array_inclusion_condition(self):
        cond = field("role").is_in(["admin", "mod"])
        assert cond.operator == "$in"
        assert cond.value == ["admin", "mod"]

    def test_not_in_creates_array_exclusion_condition(self):
        cond = field("status").not_in(["banned", "deleted"])
        assert cond.operator == "$nin"
        assert cond.value == ["banned", "deleted"]

    def test_contains_creates_substring_condition(self):
        cond = field("name").contains("test")
        assert cond.operator == "$contains"
        assert cond.value == "test"

    def test_starts_with_creates_prefix_condition(self):
        cond = field("email").starts_with("admin")
        assert cond.operator == "$startsWith"
        assert cond.value == "admin"

    def test_ends_with_creates_suffix_condition(self):
        cond = field("email").ends_with(".com")
        assert cond.operator == "$endsWith"
        assert cond.value == ".com"

    def test_exists_creates_existence_condition(self):
        cond = field("avatar").exists()
        assert cond.operator == "$exists"
        assert cond.value == True

    def test_exists_false_creates_non_existence_condition(self):
        cond = field("deleted_at").exists(False)
        assert cond.operator == "$exists"
        assert cond.value == False


class TestQueryBuilder:
    """Test QueryBuilder"""

    def test_table_creates_new_query_builder(self):
        query = table("users")
        assert isinstance(query, QueryBuilder)

    def test_compiles_minimal_query(self):
        result = table("users").compile_structured()
        assert result == {"table": "users"}

    def test_find_with_condition(self):
        result = table("users").find(field("age").gt(21)).compile_structured()
        assert result["table"] == "users"
        assert result["filter"] == {"age": {"$gt": 21}}

    def test_find_with_multiple_conditions(self):
        result = (
            table("users")
            .find(field("age").gte(18))
            .find(field("age").lte(65))
            .compile_structured()
        )
        assert result["filter"] == {"age": {"$gte": 18, "$lte": 65}}

    def test_sort_adds_sort_specification(self):
        result = table("users").sort("name").compile_structured()
        assert result["sort"] == [{"field": "name", "direction": "asc"}]

    def test_sort_with_desc_direction(self):
        result = table("users").sort("created_at", "desc").compile_structured()
        assert result["sort"] == [{"field": "created_at", "direction": "desc"}]

    def test_multiple_sorts(self):
        result = (
            table("posts")
            .sort("pinned", "desc")
            .sort("created_at", "desc")
            .compile_structured()
        )
        assert result["sort"] == [
            {"field": "pinned", "direction": "desc"},
            {"field": "created_at", "direction": "desc"},
        ]

    def test_limit_sets_max_results(self):
        result = table("users").limit(10).compile_structured()
        assert result["limit"] == 10

    def test_skip_sets_offset(self):
        result = table("users").skip(20).compile_structured()
        assert result["skip"] == 20

    def test_changes_enables_subscription(self):
        result = table("messages").changes().compile_structured()
        assert result["changes"] == {"include_initial": True}

    def test_changes_with_options(self):
        result = table("messages").changes({"include_initial": False}).compile_structured()
        assert result["changes"] == {"include_initial": False}

    def test_full_query_with_all_options(self):
        result = (
            table("users")
            .find(field("age").gte(18))
            .find(field("status").eq("active"))
            .sort("name", "asc")
            .limit(50)
            .skip(100)
            .compile_structured()
        )
        assert result == {
            "table": "users",
            "filter": {"age": {"$gte": 18}, "status": {"$eq": "active"}},
            "sort": [{"field": "name", "direction": "asc"}],
            "limit": 50,
            "skip": 100,
        }

    def test_compile_returns_json_string(self):
        import json

        result = table("users").limit(10).compile()
        assert isinstance(result, str)
        assert json.loads(result) == {"table": "users", "limit": 10}


class TestLogicalOperators:
    """Test logical operators"""

    def test_and_combines_conditions(self):
        cond = and_(field("age").gte(18), field("active").eq(True))
        assert cond.field == "$and"
        assert cond.operator == "$and"
        assert isinstance(cond.value, list)
        assert len(cond.value) == 2

    def test_or_combines_conditions(self):
        cond = or_(field("role").eq("admin"), field("role").eq("moderator"))
        assert cond.field == "$or"
        assert cond.operator == "$or"

    def test_not_negates_condition(self):
        cond = not_(field("banned").eq(True))
        assert cond.field == "$not"
        assert cond.operator == "$not"
