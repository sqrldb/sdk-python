"""
Native Python query builder for SquirrelDB
Compiles to SquirrelDB's JavaScript query syntax

Uses MongoDB-like naming with Python conventions: find/sort/limit
"""

from dataclasses import dataclass, field as dataclass_field
from typing import Any, Callable, Union, TypedDict
import json


class StructuredSort(TypedDict, total=False):
    """Sort specification for structured queries"""
    field: str
    direction: str


class StructuredChanges(TypedDict, total=False):
    """Changes specification for structured queries"""
    includeInitial: bool


class StructuredQuery(TypedDict, total=False):
    """Structured query object sent over the wire"""
    table: str
    filter: dict[str, Any]
    sort: list[StructuredSort]
    limit: int
    skip: int
    changes: StructuredChanges


@dataclass
class Eq:
    """Equal to"""
    value: Any


@dataclass
class Ne:
    """Not equal to"""
    value: Any


@dataclass
class Gt:
    """Greater than"""
    value: float


@dataclass
class Gte:
    """Greater than or equal"""
    value: float


@dataclass
class Lt:
    """Less than"""
    value: float


@dataclass
class Lte:
    """Less than or equal"""
    value: float


@dataclass
class In:
    """Value in list"""
    values: list[Any]


@dataclass
class NotIn:
    """Value not in list"""
    values: list[Any]


@dataclass
class Contains:
    """String contains"""
    value: str


@dataclass
class StartsWith:
    """String starts with"""
    value: str


@dataclass
class EndsWith:
    """String ends with"""
    value: str


@dataclass
class Exists:
    """Field exists"""
    value: bool = True


FilterOp = Union[Eq, Ne, Gt, Gte, Lt, Lte, In, NotIn, Contains, StartsWith, EndsWith, Exists]
FilterCondition = dict[str, Union[Any, FilterOp]]


class Field:
    """
    Field expression for building filter conditions.
    Usage: Field("age").gt(21), Field("name").eq("Alice")
    """

    def __init__(self, path: str):
        self._path = path

    def eq(self, value: Any) -> FilterCondition:
        """Equal to"""
        return {self._path: Eq(value)}

    def ne(self, value: Any) -> FilterCondition:
        """Not equal to"""
        return {self._path: Ne(value)}

    def gt(self, value: float) -> FilterCondition:
        """Greater than"""
        return {self._path: Gt(value)}

    def gte(self, value: float) -> FilterCondition:
        """Greater than or equal"""
        return {self._path: Gte(value)}

    def lt(self, value: float) -> FilterCondition:
        """Less than"""
        return {self._path: Lt(value)}

    def lte(self, value: float) -> FilterCondition:
        """Less than or equal"""
        return {self._path: Lte(value)}

    def is_in(self, values: list[Any]) -> FilterCondition:
        """In list of values"""
        return {self._path: In(values)}

    def not_in(self, values: list[Any]) -> FilterCondition:
        """Not in list of values"""
        return {self._path: NotIn(values)}

    def contains(self, value: str) -> FilterCondition:
        """String contains"""
        return {self._path: Contains(value)}

    def starts_with(self, value: str) -> FilterCondition:
        """String starts with"""
        return {self._path: StartsWith(value)}

    def ends_with(self, value: str) -> FilterCondition:
        """String ends with"""
        return {self._path: EndsWith(value)}

    def exists(self, value: bool = True) -> FilterCondition:
        """Field exists"""
        return {self._path: Exists(value)}

    def __getattr__(self, name: str) -> "Field":
        """Allow nested field access: doc.user.profile.name"""
        return Field(f"{self._path}.{name}")


class Doc:
    """
    Proxy for document field access in filter expressions.
    Usage: find(lambda doc: doc.age.gt(21))
    """

    def __getattr__(self, name: str) -> Field:
        return Field(name)


def _compile_filter(condition: FilterCondition) -> str:
    """Compile a filter condition to SquirrelDB JS syntax"""
    parts: list[str] = []

    for field_name, value in condition.items():
        if field_name == "$and" and isinstance(value, list):
            sub_conditions = [_compile_filter(c) for c in value]
            parts.append(f"({' && '.join(sub_conditions)})")
        elif field_name == "$or" and isinstance(value, list):
            sub_conditions = [_compile_filter(c) for c in value]
            parts.append(f"({' || '.join(sub_conditions)})")
        elif field_name == "$not" and isinstance(value, dict):
            parts.append(f"!({_compile_filter(value)})")
        elif isinstance(value, Eq):
            parts.append(f"doc.{field_name} === {json.dumps(value.value)}")
        elif isinstance(value, Ne):
            parts.append(f"doc.{field_name} !== {json.dumps(value.value)}")
        elif isinstance(value, Gt):
            parts.append(f"doc.{field_name} > {value.value}")
        elif isinstance(value, Gte):
            parts.append(f"doc.{field_name} >= {value.value}")
        elif isinstance(value, Lt):
            parts.append(f"doc.{field_name} < {value.value}")
        elif isinstance(value, Lte):
            parts.append(f"doc.{field_name} <= {value.value}")
        elif isinstance(value, In):
            parts.append(f"{json.dumps(value.values)}.includes(doc.{field_name})")
        elif isinstance(value, NotIn):
            parts.append(f"!{json.dumps(value.values)}.includes(doc.{field_name})")
        elif isinstance(value, Contains):
            parts.append(f"doc.{field_name}.includes({json.dumps(value.value)})")
        elif isinstance(value, StartsWith):
            parts.append(f"doc.{field_name}.startsWith({json.dumps(value.value)})")
        elif isinstance(value, EndsWith):
            parts.append(f"doc.{field_name}.endsWith({json.dumps(value.value)})")
        elif isinstance(value, Exists):
            if value.value:
                parts.append(f"doc.{field_name} !== undefined")
            else:
                parts.append(f"doc.{field_name} === undefined")
        else:
            # Direct equality
            parts.append(f"doc.{field_name} === {json.dumps(value)}")

    return " && ".join(parts) if parts else "true"


def _filter_to_structured(condition: FilterCondition) -> dict[str, Any]:
    """Convert a filter condition to structured query format"""
    result: dict[str, Any] = {}

    for field_name, value in condition.items():
        if field_name == "$and" and isinstance(value, list):
            result["$and"] = [_filter_to_structured(c) for c in value]
        elif field_name == "$or" and isinstance(value, list):
            result["$or"] = [_filter_to_structured(c) for c in value]
        elif field_name == "$not" and isinstance(value, dict):
            result["$not"] = _filter_to_structured(value)
        elif isinstance(value, Eq):
            result[field_name] = {"$eq": value.value}
        elif isinstance(value, Ne):
            result[field_name] = {"$ne": value.value}
        elif isinstance(value, Gt):
            result[field_name] = {"$gt": value.value}
        elif isinstance(value, Gte):
            result[field_name] = {"$gte": value.value}
        elif isinstance(value, Lt):
            result[field_name] = {"$lt": value.value}
        elif isinstance(value, Lte):
            result[field_name] = {"$lte": value.value}
        elif isinstance(value, In):
            result[field_name] = {"$in": value.values}
        elif isinstance(value, NotIn):
            result[field_name] = {"$nin": value.values}
        elif isinstance(value, Contains):
            result[field_name] = {"$contains": value.value}
        elif isinstance(value, StartsWith):
            result[field_name] = {"$startsWith": value.value}
        elif isinstance(value, EndsWith):
            result[field_name] = {"$endsWith": value.value}
        elif isinstance(value, Exists):
            result[field_name] = {"$exists": value.value}
        else:
            # Direct equality
            result[field_name] = {"$eq": value}

    return result


class QueryBuilder:
    """
    Query builder for fluent, type-safe queries.
    Uses MongoDB-like naming: find/sort/limit

    Usage:
        table("users").find(lambda doc: doc.age.gt(21)).sort("name").limit(10)
    """

    def __init__(self, table_name: str):
        self._table_name = table_name
        self._filter_expr: str | None = None
        self._filter_condition: FilterCondition | None = None
        self._sort_specs: list[tuple[str, str]] = []
        self._limit_value: int | None = None
        self._skip_value: int | None = None
        self._is_changes = False

    def find(
        self,
        condition: Union[Callable[[Doc], FilterCondition], FilterCondition, None] = None,
        **kwargs: Any
    ) -> "QueryBuilder":
        """
        Find documents matching condition.

        Usage:
            .find(lambda doc: doc.age.gt(21))
            .find({"age": Gt(21)})
            .find(name="Alice", status="active")  # kwargs for equality
        """
        if callable(condition):
            cond = condition(Doc())
            self._filter_condition = cond
            self._filter_expr = _compile_filter(cond)
        elif condition is not None:
            self._filter_condition = condition
            self._filter_expr = _compile_filter(condition)
        elif kwargs:
            # Convert kwargs to equality conditions
            cond = {k: v for k, v in kwargs.items()}
            self._filter_condition = cond
            self._filter_expr = _compile_filter(cond)
        return self

    def sort(self, field: str, direction: str = "asc") -> "QueryBuilder":
        """
        Sort results by field.

        Usage: .sort("name") or .sort("age", "desc")
        """
        self._sort_specs.append((field, direction))
        return self

    def limit(self, n: int) -> "QueryBuilder":
        """Limit number of results."""
        self._limit_value = n
        return self

    def skip(self, n: int) -> "QueryBuilder":
        """Skip n results (offset)."""
        self._skip_value = n
        return self

    def changes(self) -> "QueryBuilder":
        """Subscribe to changes instead of querying."""
        self._is_changes = True
        return self

    def compile(self) -> str:
        """Compile to SquirrelDB JS query string (legacy)."""
        query = f'db.table("{self._table_name}")'

        if self._filter_expr:
            query += f".filter(doc => {self._filter_expr})"

        for field_name, direction in self._sort_specs:
            if direction == "desc":
                query += f'.orderBy("{field_name}", "desc")'
            else:
                query += f'.orderBy("{field_name}")'

        if self._limit_value is not None:
            query += f".limit({self._limit_value})"

        if self._skip_value is not None:
            query += f".skip({self._skip_value})"

        if self._is_changes:
            query += ".changes()"
        else:
            query += ".run()"

        return query

    def compile_structured(self) -> StructuredQuery:
        """Compile to structured query object (preferred, no JS evaluation on server)."""
        query: StructuredQuery = {"table": self._table_name}

        if self._filter_condition:
            query["filter"] = _filter_to_structured(self._filter_condition)

        if self._sort_specs:
            query["sort"] = [
                {"field": field_name, "direction": direction}
                for field_name, direction in self._sort_specs
            ]

        if self._limit_value is not None:
            query["limit"] = self._limit_value

        if self._skip_value is not None:
            query["skip"] = self._skip_value

        if self._is_changes:
            query["changes"] = {"includeInitial": False}

        return query

    def __str__(self) -> str:
        return self.compile()


def table(name: str) -> QueryBuilder:
    """
    Create a table query builder.

    Usage: table("users").find(lambda doc: doc.age.gt(21)).compile()
    """
    return QueryBuilder(name)


def field(name: str) -> Field:
    """
    Create a field expression.

    Usage: field("age").gt(21)
    """
    return Field(name)


def and_(*conditions: FilterCondition) -> FilterCondition:
    """
    Combine conditions with AND.

    Usage: and_(field("age").gt(21), field("status").eq("active"))
    """
    return {"$and": list(conditions)}


def or_(*conditions: FilterCondition) -> FilterCondition:
    """
    Combine conditions with OR.

    Usage: or_(field("status").eq("pending"), field("status").eq("active"))
    """
    return {"$or": list(conditions)}


def not_(condition: FilterCondition) -> FilterCondition:
    """
    Negate a condition.

    Usage: not_(field("deleted").eq(True))
    """
    return {"$not": condition}
