# ruff: noqa: B008
import argparse
import asyncio
import logging
import os
import signal
import sys
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Union

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from pydantic import Field
from pydantic import validate_call

from postgres_mcp.index.dta_calc import DatabaseTuningAdvisor

from .artifacts import ErrorResult
from .artifacts import ExplainPlanArtifact
from .database_health import DatabaseHealthTool
from .database_health import HealthType
from .explain import ExplainPlanTool
from .index.index_opt_base import MAX_NUM_INDEX_TUNING_QUERIES
from .index.llm_opt import LLMOptimizerTool
from .index.presentation import TextPresentation
from .sql import DbConnPool
from .sql import SafeSqlDriver
from .sql import SqlDriver
from .sql import check_hypopg_installation_status
from .sql import obfuscate_password
from .top_queries import TopQueriesCalc
from .database_operations import SimpleCrudOperations, SchemaOperations
from .database_operations.vector_operations import VectorOperations
from .database_operations.data_types import (
    ColumnDefinition,
    DataType,
    IndexDefinition,
    QueryCondition,
    QueryOptions,
    TableDefinition,
    VectorSearchOptions,
)

# Initialize FastMCP with default settings
mcp = FastMCP("postgres-mcp")

# Constants
PG_STAT_STATEMENTS = "pg_stat_statements"
HYPOPG_EXTENSION = "hypopg"

ResponseType = List[types.TextContent | types.ImageContent | types.EmbeddedResource]

logger = logging.getLogger(__name__)


class AccessMode(str, Enum):
    """SQL access modes for the server."""

    UNRESTRICTED = "unrestricted"  # Unrestricted access
    RESTRICTED = "restricted"  # Read-only with safety features


# Global variables
db_connection = DbConnPool()
current_access_mode = AccessMode.UNRESTRICTED
shutdown_in_progress = False


async def get_sql_driver() -> Union[SqlDriver, SafeSqlDriver]:
    """Get the appropriate SQL driver based on the current access mode."""
    base_driver = SqlDriver(conn=db_connection)

    if current_access_mode == AccessMode.RESTRICTED:
        logger.debug("Using SafeSqlDriver with restrictions (RESTRICTED mode)")
        return SafeSqlDriver(sql_driver=base_driver, timeout=30)  # 30 second timeout
    else:
        logger.debug("Using unrestricted SqlDriver (UNRESTRICTED mode)")
        return base_driver


def format_text_response(text: Any) -> ResponseType:
    """Format a text response."""
    return [types.TextContent(type="text", text=str(text))]


def format_error_response(error: str) -> ResponseType:
    """Format an error response."""
    return format_text_response(f"Error: {error}")


@mcp.tool(description="List all schemas in the database")
async def list_schemas() -> ResponseType:
    """List all schemas in the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT
                schema_name,
                schema_owner,
                CASE
                    WHEN schema_name LIKE 'pg_%' THEN 'System Schema'
                    WHEN schema_name = 'information_schema' THEN 'System Information Schema'
                    ELSE 'User Schema'
                END as schema_type
            FROM information_schema.schemata
            ORDER BY schema_type, schema_name
            """
        )
        schemas = [row.cells for row in rows] if rows else []
        return format_text_response(schemas)
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        return format_error_response(str(e))


@mcp.tool(description="List objects in a schema")
async def list_objects(
    schema_name: str = Field(description="Schema name"),
    object_type: str = Field(description="Object type: 'table', 'view', 'sequence', or 'extension'", default="table"),
) -> ResponseType:
    """List objects of a given type in a schema."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            table_type = "BASE TABLE" if object_type == "table" else "VIEW"
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = {} AND table_type = {}
                ORDER BY table_name
                """,
                [schema_name, table_type],
            )
            objects = (
                [{"schema": row.cells["table_schema"], "name": row.cells["table_name"], "type": row.cells["table_type"]} for row in rows]
                if rows
                else []
            )

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT sequence_schema, sequence_name, data_type
                FROM information_schema.sequences
                WHERE sequence_schema = {}
                ORDER BY sequence_name
                """,
                [schema_name],
            )
            objects = (
                [{"schema": row.cells["sequence_schema"], "name": row.cells["sequence_name"], "data_type": row.cells["data_type"]} for row in rows]
                if rows
                else []
            )

        elif object_type == "extension":
            # Extensions are not schema-specific
            rows = await sql_driver.execute_query(
                """
                SELECT extname, extversion, extrelocatable
                FROM pg_extension
                ORDER BY extname
                """
            )
            objects = (
                [{"name": row.cells["extname"], "version": row.cells["extversion"], "relocatable": row.cells["extrelocatable"]} for row in rows]
                if rows
                else []
            )

        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(objects)
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Show detailed information about a database object")
async def get_object_details(
    schema_name: str = Field(description="Schema name"),
    object_name: str = Field(description="Object name"),
    object_type: str = Field(description="Object type: 'table', 'view', 'sequence', or 'extension'", default="table"),
) -> ResponseType:
    """Get detailed information about a database object."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            # Get columns
            col_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = {} AND table_name = {}
                ORDER BY ordinal_position
                """,
                [schema_name, object_name],
            )
            columns = (
                [
                    {
                        "column": r.cells["column_name"],
                        "data_type": r.cells["data_type"],
                        "is_nullable": r.cells["is_nullable"],
                        "default": r.cells["column_default"],
                    }
                    for r in col_rows
                ]
                if col_rows
                else []
            )

            # Get constraints
            con_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
                FROM information_schema.table_constraints AS tc
                LEFT JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = {} AND tc.table_name = {}
                """,
                [schema_name, object_name],
            )

            constraints = {}
            if con_rows:
                for row in con_rows:
                    cname = row.cells["constraint_name"]
                    ctype = row.cells["constraint_type"]
                    col = row.cells["column_name"]

                    if cname not in constraints:
                        constraints[cname] = {"type": ctype, "columns": []}
                    if col:
                        constraints[cname]["columns"].append(col)

            constraints_list = [{"name": name, **data} for name, data in constraints.items()]

            # Get indexes
            idx_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = {} AND tablename = {}
                """,
                [schema_name, object_name],
            )

            indexes = [{"name": r.cells["indexname"], "definition": r.cells["indexdef"]} for r in idx_rows] if idx_rows else []

            result = {
                "basic": {"schema": schema_name, "name": object_name, "type": object_type},
                "columns": columns,
                "constraints": constraints_list,
                "indexes": indexes,
            }

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT sequence_schema, sequence_name, data_type, start_value, increment
                FROM information_schema.sequences
                WHERE sequence_schema = {} AND sequence_name = {}
                """,
                [schema_name, object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                result = {
                    "schema": row.cells["sequence_schema"],
                    "name": row.cells["sequence_name"],
                    "data_type": row.cells["data_type"],
                    "start_value": row.cells["start_value"],
                    "increment": row.cells["increment"],
                }
            else:
                result = {}

        elif object_type == "extension":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT extname, extversion, extrelocatable
                FROM pg_extension
                WHERE extname = {}
                """,
                [object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                result = {"name": row.cells["extname"], "version": row.cells["extversion"], "relocatable": row.cells["extrelocatable"]}
            else:
                result = {}

        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting object details: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Explains the execution plan for a SQL query, showing how the database will execute it and provides detailed cost estimates.")
async def explain_query(
    sql: str = Field(description="SQL query to explain"),
    analyze: bool = Field(
        description="When True, actually runs the query to show real execution statistics instead of estimates. "
        "Takes longer but provides more accurate information.",
        default=False,
    ),
    hypothetical_indexes: list[dict[str, Any]] = Field(
        description="""A list of hypothetical indexes to simulate. Each index must be a dictionary with these keys:
    - 'table': The table name to add the index to (e.g., 'users')
    - 'columns': List of column names to include in the index (e.g., ['email'] or ['last_name', 'first_name'])
    - 'using': Optional index method (default: 'btree', other options include 'hash', 'gist', etc.)

Examples: [
    {"table": "users", "columns": ["email"], "using": "btree"},
    {"table": "orders", "columns": ["user_id", "created_at"]}
]
If there is no hypothetical index, you can pass an empty list.""",
        default=[],
    ),
) -> ResponseType:
    """
    Explains the execution plan for a SQL query.

    Args:
        sql: The SQL query to explain
        analyze: When True, actually runs the query for real statistics
        hypothetical_indexes: Optional list of indexes to simulate
    """
    try:
        sql_driver = await get_sql_driver()
        explain_tool = ExplainPlanTool(sql_driver=sql_driver)
        result: ExplainPlanArtifact | ErrorResult | None = None

        # If hypothetical indexes are specified, check for HypoPG extension
        if hypothetical_indexes and len(hypothetical_indexes) > 0:
            if analyze:
                return format_error_response("Cannot use analyze and hypothetical indexes together")
            try:
                # Use the common utility function to check if hypopg is installed
                (
                    is_hypopg_installed,
                    hypopg_message,
                ) = await check_hypopg_installation_status(sql_driver)

                # If hypopg is not installed, return the message
                if not is_hypopg_installed:
                    return format_text_response(hypopg_message)

                # HypoPG is installed, proceed with explaining with hypothetical indexes
                result = await explain_tool.explain_with_hypothetical_indexes(sql, hypothetical_indexes)
            except Exception:
                raise  # Re-raise the original exception
        elif analyze:
            try:
                # Use EXPLAIN ANALYZE
                result = await explain_tool.explain_analyze(sql)
            except Exception:
                raise  # Re-raise the original exception
        else:
            try:
                # Use basic EXPLAIN
                result = await explain_tool.explain(sql)
            except Exception:
                raise  # Re-raise the original exception

        if result and isinstance(result, ExplainPlanArtifact):
            return format_text_response(result.to_text())
        else:
            error_message = "Error processing explain plan"
            if isinstance(result, ErrorResult):
                error_message = result.to_text()
            return format_error_response(error_message)
    except Exception as e:
        logger.error(f"Error explaining query: {e}")
        return format_error_response(str(e))


# Query function declaration without the decorator - we'll add it dynamically based on access mode
async def execute_sql(
    sql: str = Field(description="SQL to run", default="all"),
) -> ResponseType:
    """Executes a SQL query against the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(sql)  # type: ignore
        if rows is None:
            return format_text_response("No results")
        return format_text_response(list([r.cells for r in rows]))
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Analyze frequently executed queries in the database and recommend optimal indexes")
@validate_call
async def analyze_workload_indexes(
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(description="Method to use for analysis", default="dta"),
) -> ResponseType:
    """Analyze frequently executed queries in the database and recommend optimal indexes."""
    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_workload(max_index_size_mb=max_index_size_mb)
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing workload: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Analyze a list of (up to 10) SQL queries and recommend optimal indexes")
@validate_call
async def analyze_query_indexes(
    queries: list[str] = Field(description="List of Query strings to analyze"),
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(description="Method to use for analysis", default="dta"),
) -> ResponseType:
    """Analyze a list of SQL queries and recommend optimal indexes."""
    if len(queries) == 0:
        return format_error_response("Please provide a non-empty list of queries to analyze.")
    if len(queries) > MAX_NUM_INDEX_TUNING_QUERIES:
        return format_error_response(f"Please provide a list of up to {MAX_NUM_INDEX_TUNING_QUERIES} queries to analyze.")

    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_queries(queries=queries, max_index_size_mb=max_index_size_mb)
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing queries: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Analyzes database health. Here are the available health checks:\n"
    "- index - checks for invalid, duplicate, and bloated indexes\n"
    "- connection - checks the number of connection and their utilization\n"
    "- vacuum - checks vacuum health for transaction id wraparound\n"
    "- sequence - checks sequences at risk of exceeding their maximum value\n"
    "- replication - checks replication health including lag and slots\n"
    "- buffer - checks for buffer cache hit rates for indexes and tables\n"
    "- constraint - checks for invalid constraints\n"
    "- all - runs all checks\n"
    "You can optionally specify a single health check or a comma-separated list of health checks. The default is 'all' checks."
)
async def analyze_db_health(
    health_type: str = Field(
        description=f"Optional. Valid values are: {', '.join(sorted([t.value for t in HealthType]))}.",
        default="all",
    ),
) -> ResponseType:
    """Analyze database health for specified components.

    Args:
        health_type: Comma-separated list of health check types to perform.
                    Valid values: index, connection, vacuum, sequence, replication, buffer, constraint, all
    """
    health_tool = DatabaseHealthTool(await get_sql_driver())
    result = await health_tool.health(health_type=health_type)
    return format_text_response(result)


@mcp.tool(
    name="get_top_queries",
    description=f"Reports the slowest or most resource-intensive queries using data from the '{PG_STAT_STATEMENTS}' extension.",
)
async def get_top_queries(
    sort_by: str = Field(
        description="Ranking criteria: 'total_time' for total execution time or 'mean_time' for mean execution time per call, or 'resources' "
        "for resource-intensive queries",
        default="resources",
    ),
    limit: int = Field(description="Number of queries to return when ranking based on mean_time or total_time", default=10),
) -> ResponseType:
    try:
        sql_driver = await get_sql_driver()
        top_queries_tool = TopQueriesCalc(sql_driver=sql_driver)

        if sort_by == "resources":
            result = await top_queries_tool.get_top_resource_queries()
            return format_text_response(result)
        elif sort_by == "mean_time" or sort_by == "total_time":
            # Map the sort_by values to what get_top_queries_by_time expects
            result = await top_queries_tool.get_top_queries_by_time(limit=limit, sort_by="mean" if sort_by == "mean_time" else "total")
        else:
            return format_error_response("Invalid sort criteria. Please use 'resources' or 'mean_time' or 'total_time'.")
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting slow queries: {e}")
        return format_error_response(str(e))


# ============================================================================
# 新增的数据库操作MCP工具
# ============================================================================

@mcp.tool(description="Create a new table in the database with specified columns and constraints")
async def create_table(
    table_name: str = Field(description="Name of the table to create"),
    columns: List[Dict[str, Any]] = Field(description="""List of column definitions. Each column must be a dictionary with:
    - 'name': Column name (required)
    - 'data_type': Data type (required) - one of: integer, bigint, serial, bigserial, text, varchar, char, boolean, timestamp, timestamptz, date, time, json, jsonb, uuid, vector, decimal, numeric, real, double_precision
    - 'nullable': Whether column can be NULL (default: true)
    - 'default': Default value (optional)
    - 'primary_key': Whether this is primary key (default: false)
    - 'unique': Whether this column is unique (default: false)
    - 'check_constraint': Check constraint expression (optional)
    - 'vector_dimensions': Required for vector type, specifies dimensions

Examples:
[
    {"name": "id", "data_type": "serial", "primary_key": true, "nullable": false},
    {"name": "name", "data_type": "varchar", "nullable": false},
    {"name": "email", "data_type": "varchar", "unique": true},
    {"name": "created_at", "data_type": "timestamp", "default": "CURRENT_TIMESTAMP"},
    {"name": "embedding", "data_type": "vector", "vector_dimensions": 1536}
]"""),
    schema: str = Field(description="Schema name", default="public"),
    constraints: List[str] = Field(description="Additional table-level constraints", default=[]),
) -> ResponseType:
    """Create a new table with specified columns and constraints."""
    try:
        sql_driver = await get_sql_driver()
        crud_ops = SimpleCrudOperations(sql_driver)
        
        # 转换列定义
        column_defs = []
        for col in columns:
            try:
                data_type = DataType(col["data_type"])
            except ValueError:
                return format_error_response(f"Invalid data type: {col['data_type']}")
            
            column_def = ColumnDefinition(
                name=col["name"],
                data_type=data_type,
                nullable=col.get("nullable", True),
                default=col.get("default"),
                primary_key=col.get("primary_key", False),
                unique=col.get("unique", False),
                check_constraint=col.get("check_constraint"),
                vector_dimensions=col.get("vector_dimensions"),
            )
            column_defs.append(column_def)
        
        table_def = TableDefinition(
            name=table_name,
            columns=column_defs,
            schema=schema,
            constraints=constraints,
        )
        
        result = await crud_ops.create_table(table_def)
        
        if result.success:
            return format_text_response({
                "success": True,
                "message": result.message,
                "table": f"{schema}.{table_name}",
                "execution_time_ms": result.execution_time_ms,
            })
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Insert one or more records into a table")
async def insert_records(
    table_name: str = Field(description="Name of the table to insert into"),
    data: List[Dict[str, Any]] = Field(description="""List of records to insert. Each record is a dictionary with column names as keys.
Examples:
[
    {"name": "John Doe", "email": "john@example.com", "age": 30},
    {"name": "Jane Smith", "email": "jane@example.com", "age": 25}
]
For vector columns, provide the vector as a list of floats:
[
    {"content": "Hello world", "embedding": [0.1, 0.2, 0.3, ...]}
]"""),
    schema: str = Field(description="Schema name", default="public"),
    returning_columns: Optional[List[str]] = Field(description="List of columns to return from inserted records", default=None),
) -> ResponseType:
    """Insert one or more records into a table."""
    try:
        if not data:
            return format_error_response("No data provided for insert")
        
        sql_driver = await get_sql_driver()
        crud_ops = SimpleCrudOperations(sql_driver)
        
        result = await crud_ops.insert_records(
            table_name=table_name,
            data=data,
            schema=schema,
            returning_columns=returning_columns,
        )
        
        if result.success:
            response = {
                "success": True,
                "message": result.message,
                "affected_rows": result.affected_rows,
                "execution_time_ms": result.execution_time_ms,
            }
            if result.returned_data:
                response["returned_data"] = result.returned_data
            return format_text_response(response)
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error inserting records into {table_name}: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Update records in a table based on conditions")
async def update_records(
    table_name: str = Field(description="Name of the table to update"),
    data: Dict[str, Any] = Field(description="""Data to update as a dictionary with column names as keys.
Example: {"name": "Updated Name", "email": "new@example.com", "updated_at": "2024-01-01 10:00:00"}"""),
    conditions: List[Dict[str, Any]] = Field(description="""List of conditions for WHERE clause. Each condition is a dictionary with:
    - 'column': Column name
    - 'operator': Operator (=, !=, >, <, >=, <=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL)
    - 'value': Value to compare (not needed for IS NULL/IS NOT NULL, list/tuple for IN/NOT IN)
    - 'logical_operator': 'AND' or 'OR' (default: 'AND')

Examples:
[
    {"column": "id", "operator": "=", "value": 1},
    {"column": "status", "operator": "IN", "value": ["active", "pending"], "logical_operator": "AND"},
    {"column": "name", "operator": "LIKE", "value": "John%"}
]"""),
    schema: str = Field(description="Schema name", default="public"),
    returning_columns: Optional[List[str]] = Field(description="List of columns to return from updated records", default=None),
) -> ResponseType:
    """Update records in a table based on conditions."""
    try:
        sql_driver = await get_sql_driver()
        crud_ops = CrudOperations(sql_driver)
        
        # 转换查询条件
        query_conditions = []
        for cond in conditions:
            query_condition = QueryCondition(
                column=cond["column"],
                operator=cond["operator"],
                value=cond.get("value"),
                logical_operator=cond.get("logical_operator", "AND"),
            )
            query_conditions.append(query_condition)
        
        result = await crud_ops.update_records(
            table_name=table_name,
            data=data,
            conditions=query_conditions,
            schema=schema,
            returning_columns=returning_columns,
        )
        
        if result.success:
            response = {
                "success": True,
                "message": result.message,
                "affected_rows": result.affected_rows,
                "execution_time_ms": result.execution_time_ms,
            }
            if result.returned_data:
                response["returned_data"] = result.returned_data
            return format_text_response(response)
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error updating records in {table_name}: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Delete records from a table based on conditions")
async def delete_records(
    table_name: str = Field(description="Name of the table to delete from"),
    conditions: List[Dict[str, Any]] = Field(description="""List of conditions for WHERE clause. Each condition is a dictionary with:
    - 'column': Column name
    - 'operator': Operator (=, !=, >, <, >=, <=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL)
    - 'value': Value to compare (not needed for IS NULL/IS NOT NULL, list/tuple for IN/NOT IN)
    - 'logical_operator': 'AND' or 'OR' (default: 'AND')

Examples:
[
    {"column": "id", "operator": "=", "value": 1},
    {"column": "status", "operator": "IN", "value": ["inactive", "deleted"]},
    {"column": "created_at", "operator": "<", "value": "2023-01-01"}
]"""),
    schema: str = Field(description="Schema name", default="public"),
    returning_columns: Optional[List[str]] = Field(description="List of columns to return from deleted records", default=None),
) -> ResponseType:
    """Delete records from a table based on conditions."""
    try:
        sql_driver = await get_sql_driver()
        crud_ops = CrudOperations(sql_driver)
        
        # 转换查询条件
        query_conditions = []
        for cond in conditions:
            query_condition = QueryCondition(
                column=cond["column"],
                operator=cond["operator"],
                value=cond.get("value"),
                logical_operator=cond.get("logical_operator", "AND"),
            )
            query_conditions.append(query_condition)
        
        result = await crud_ops.delete_records(
            table_name=table_name,
            conditions=query_conditions,
            schema=schema,
            returning_columns=returning_columns,
        )
        
        if result.success:
            response = {
                "success": True,
                "message": result.message,
                "affected_rows": result.affected_rows,
                "execution_time_ms": result.execution_time_ms,
            }
            if result.returned_data:
                response["returned_data"] = result.returned_data
            return format_text_response(response)
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error deleting records from {table_name}: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Query records from a table with advanced filtering, sorting, and pagination")
async def query_records(
    table_name: str = Field(description="Name of the table to query"),
    columns: Optional[List[str]] = Field(description="List of columns to select (default: all columns)", default=None),
    conditions: Optional[List[Dict[str, Any]]] = Field(description="""Optional list of conditions for WHERE clause. Each condition is a dictionary with:
    - 'column': Column name
    - 'operator': Operator (=, !=, >, <, >=, <=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL)
    - 'value': Value to compare (not needed for IS NULL/IS NOT NULL, list/tuple for IN/NOT IN)
    - 'logical_operator': 'AND' or 'OR' (default: 'AND')

Examples:
[
    {"column": "status", "operator": "=", "value": "active"},
    {"column": "age", "operator": ">=", "value": 18, "logical_operator": "AND"}
]""", default=None),
    order_by: Optional[List[Dict[str, str]]] = Field(description="""Optional list of ordering specifications. Each is a dictionary with:
    - 'column': Column name
    - 'direction': 'ASC' or 'DESC'

Examples: [{"column": "created_at", "direction": "DESC"}, {"column": "name", "direction": "ASC"}]""", default=None),
    limit: Optional[int] = Field(description="Maximum number of records to return", default=None),
    offset: Optional[int] = Field(description="Number of records to skip", default=None),
    schema: str = Field(description="Schema name", default="public"),
) -> ResponseType:
    """Query records from a table with advanced filtering, sorting, and pagination."""
    try:
        sql_driver = await get_sql_driver()
        crud_ops = CrudOperations(sql_driver)
        
        # 转换查询条件
        query_conditions = None
        if conditions:
            query_conditions = []
            for cond in conditions:
                query_condition = QueryCondition(
                    column=cond["column"],
                    operator=cond["operator"],
                    value=cond.get("value"),
                    logical_operator=cond.get("logical_operator", "AND"),
                )
                query_conditions.append(query_condition)
        
        # 构建查询选项
        options = QueryOptions(
            limit=limit,
            offset=offset,
            order_by=order_by or [],
        )
        
        result = await crud_ops.query_records(
            table_name=table_name,
            columns=columns,
            conditions=query_conditions,
            options=options,
            schema=schema,
        )
        
        if result.success:
            return format_text_response({
                "success": True,
                "message": result.message,
                "data": result.data,
                "columns": result.columns,
                "total_count": result.total_count,
                "execution_time_ms": result.execution_time_ms,
            })
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error querying records from {table_name}: {e}")
        return format_error_response(str(e))


# ============================================================================
# 数据库模式管理MCP工具
# ============================================================================

@mcp.tool(description="Create a new database schema")
async def create_schema(
    schema_name: str = Field(description="Name of the schema to create"),
    if_not_exists: bool = Field(description="Use IF NOT EXISTS clause", default=True),
) -> ResponseType:
    """Create a new database schema."""
    try:
        sql_driver = await get_sql_driver()
        schema_ops = SchemaOperations(sql_driver)
        
        result = await schema_ops.create_schema(schema_name, if_not_exists)
        
        if result.success:
            return format_text_response({
                "success": True,
                "message": result.message,
                "schema": schema_name,
                "execution_time_ms": result.execution_time_ms,
            })
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error creating schema {schema_name}: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Drop a database schema")
async def drop_schema(
    schema_name: str = Field(description="Name of the schema to drop"),
    cascade: bool = Field(description="Use CASCADE to drop all dependent objects", default=False),
    if_exists: bool = Field(description="Use IF EXISTS clause", default=True),
) -> ResponseType:
    """Drop a database schema."""
    try:
        sql_driver = await get_sql_driver()
        schema_ops = SchemaOperations(sql_driver)
        
        result = await schema_ops.drop_schema(schema_name, cascade, if_exists)
        
        if result.success:
            return format_text_response({
                "success": True,
                "message": result.message,
                "schema": schema_name,
                "execution_time_ms": result.execution_time_ms,
            })
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error dropping schema {schema_name}: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Drop a table from the database")
async def drop_table(
    table_name: str = Field(description="Name of the table to drop"),
    schema: str = Field(description="Schema name", default="public"),
    cascade: bool = Field(description="Use CASCADE to drop all dependent objects", default=False),
    if_exists: bool = Field(description="Use IF EXISTS clause", default=True),
) -> ResponseType:
    """Drop a table from the database."""
    try:
        sql_driver = await get_sql_driver()
        schema_ops = SchemaOperations(sql_driver)
        
        result = await schema_ops.drop_table(table_name, schema, cascade, if_exists)
        
        if result.success:
            return format_text_response({
                "success": True,
                "message": result.message,
                "table": f"{schema}.{table_name}",
                "execution_time_ms": result.execution_time_ms,
            })
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error dropping table {table_name}: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Create an index on a table")
async def create_index(
    index_name: str = Field(description="Name of the index to create"),
    table_name: str = Field(description="Name of the table to create index on"),
    columns: List[str] = Field(description="List of column names to include in the index"),
    index_type: str = Field(description="Index type: btree, hash, gin, gist, brin, spgist, hnsw (vector), ivfflat (vector)", default="btree"),
    unique: bool = Field(description="Create a unique index", default=False),
    partial_condition: Optional[str] = Field(description="WHERE clause for partial index", default=None),
    vector_index_options: Optional[Dict[str, Any]] = Field(description="""Options for vector indexes (hnsw/ivfflat). Dictionary with options like:
    - For HNSW: {"m": 16, "ef_construction": 64}
    - For IVFFlat: {"lists": 100}""", default=None),
) -> ResponseType:
    """Create an index on a table."""
    try:
        sql_driver = await get_sql_driver()
        schema_ops = SchemaOperations(sql_driver)
        
        index_def = IndexDefinition(
            name=index_name,
            table_name=table_name,
            columns=columns,
            index_type=index_type,
            unique=unique,
            partial_condition=partial_condition,
            vector_index_options=vector_index_options or {},
        )
        
        result = await schema_ops.create_index(index_def)
        
        if result.success:
            return format_text_response({
                "success": True,
                "message": result.message,
                "index": index_name,
                "table": table_name,
                "execution_time_ms": result.execution_time_ms,
            })
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error creating index {index_name}: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Drop an index from the database")
async def drop_index(
    index_name: str = Field(description="Name of the index to drop"),
    if_exists: bool = Field(description="Use IF EXISTS clause", default=True),
    cascade: bool = Field(description="Use CASCADE to drop all dependent objects", default=False),
) -> ResponseType:
    """Drop an index from the database."""
    try:
        sql_driver = await get_sql_driver()
        schema_ops = SchemaOperations(sql_driver)
        
        result = await schema_ops.drop_index(index_name, if_exists, cascade)
        
        if result.success:
            return format_text_response({
                "success": True,
                "message": result.message,
                "index": index_name,
                "execution_time_ms": result.execution_time_ms,
            })
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error dropping index {index_name}: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Create a PostgreSQL extension")
async def create_extension(
    extension_name: str = Field(description="Name of the extension to create (e.g., 'vector', 'pg_stat_statements')"),
    if_not_exists: bool = Field(description="Use IF NOT EXISTS clause", default=True),
    schema: Optional[str] = Field(description="Schema to install the extension in", default=None),
) -> ResponseType:
    """Create a PostgreSQL extension."""
    try:
        sql_driver = await get_sql_driver()
        schema_ops = SchemaOperations(sql_driver)
        
        result = await schema_ops.create_extension(extension_name, if_not_exists, schema)
        
        if result.success:
            return format_text_response({
                "success": True,
                "message": result.message,
                "extension": extension_name,
                "execution_time_ms": result.execution_time_ms,
            })
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error creating extension {extension_name}: {e}")
        return format_error_response(str(e))


# Vector-specific tools (Phase 2)

@mcp.tool(description="Create a specialized table for vector storage with pgvector extension")
async def create_vector_table(
    table_name: str = Field(description="Name of the vector table to create"),
    schema: str = Field(description="Schema name", default="public"),
    vector_column: str = Field(description="Name of the vector column", default="embedding"),
    vector_dimensions: int = Field(description="Number of dimensions for the vector", default=1536),
    additional_columns: Optional[List[Dict[str, Any]]] = Field(description="""Additional columns to include. Each column is a dictionary with:
    - 'name': Column name (required)
    - 'data_type': Data type (required)
    - 'nullable': Whether column can be NULL (default: true)
    - 'default': Default value (optional)
    - 'unique': Whether this column is unique (default: false)

Examples:
[
    {"name": "content", "data_type": "text", "nullable": false},
    {"name": "metadata", "data_type": "jsonb"},
    {"name": "source", "data_type": "varchar"}
]""", default=None),
) -> ResponseType:
    """Create a specialized table for vector storage."""
    try:
        sql_driver = await get_sql_driver()
        vector_ops = VectorOperations(sql_driver)
        
        # 转换额外列定义
        extra_columns = None
        if additional_columns:
            extra_columns = []
            for col_dict in additional_columns:
                col = ColumnDefinition(
                    name=col_dict["name"],
                    data_type=DataType(col_dict["data_type"]),
                    nullable=col_dict.get("nullable", True),
                    default=col_dict.get("default"),
                    unique=col_dict.get("unique", False)
                )
                extra_columns.append(col)
        
        result = await vector_ops.create_vector_table(
            table_name=table_name,
            schema=schema,
            vector_column=vector_column,
            vector_dimensions=vector_dimensions,
            additional_columns=extra_columns
        )
        
        if result.success:
            return format_text_response(result.message)
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error creating vector table: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Insert vector data into a vector table")
async def insert_vector_data(
    table_name: str = Field(description="Name of the vector table"),
    vectors: List[List[float]] = Field(description="""List of vectors to insert. Each vector is a list of floats.
Example: [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]"""),
    schema: str = Field(description="Schema name", default="public"),
    vector_column: str = Field(description="Name of the vector column", default="embedding"),
    additional_data: Optional[List[Dict[str, Any]]] = Field(description="""Additional data for each vector. Must have same length as vectors list.
Each item is a dictionary with column names as keys.
Example: [{"content": "First text", "metadata": {"type": "doc"}}, {"content": "Second text"}]""", default=None),
    returning_columns: Optional[List[str]] = Field(description="List of columns to return from inserted records", default=None),
) -> ResponseType:
    """Insert vector data into a vector table."""
    try:
        sql_driver = await get_sql_driver()
        vector_ops = VectorOperations(sql_driver)
        
        result = await vector_ops.insert_vector_data(
            table_name=table_name,
            schema=schema,
            vector_column=vector_column,
            vectors=vectors,
            additional_data=additional_data,
            returning_columns=returning_columns
        )
        
        if result.success:
            response_data = {"message": result.message, "affected_rows": result.affected_rows}
            if result.returned_data:
                response_data["returned_data"] = result.returned_data
            return format_text_response(response_data)
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error inserting vector data: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Perform vector similarity search to find similar vectors")
async def vector_similarity_search(
    table_name: str = Field(description="Name of the vector table to search"),
    search_vector: List[float] = Field(description="Query vector to search for similar vectors"),
    schema: str = Field(description="Schema name", default="public"),
    vector_column: str = Field(description="Name of the vector column", default="embedding"),
    distance_function: str = Field(description="Distance function: 'cosine', 'euclidean', or 'inner_product'", default="cosine"),
    limit: int = Field(description="Maximum number of results to return", default=10),
    threshold: Optional[float] = Field(description="Distance threshold to filter results", default=None),
    additional_columns: Optional[List[str]] = Field(description="Additional columns to include in results", default=None),
    include_distance: bool = Field(description="Include distance in results", default=True),
) -> ResponseType:
    """Perform vector similarity search."""
    try:
        sql_driver = await get_sql_driver()
        vector_ops = VectorOperations(sql_driver)
        
        search_options = VectorSearchOptions(
            vector=search_vector,
            distance_function=distance_function,
            limit=limit,
            threshold=threshold,
            include_distance=include_distance
        )
        
        result = await vector_ops.vector_similarity_search(
            table_name=table_name,
            schema=schema,
            vector_column=vector_column,
            search_options=search_options,
            additional_columns=additional_columns
        )
        
        if result.success:
            response_data = {
                "message": result.message,
                "total_results": result.total_count,
                "results": result.data
            }
            return format_text_response(response_data)
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error performing vector similarity search: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Create a vector index for efficient similarity search")
async def create_vector_index(
    table_name: str = Field(description="Name of the table to create index on"),
    schema: str = Field(description="Schema name", default="public"),
    vector_column: str = Field(description="Name of the vector column", default="embedding"),
    index_name: Optional[str] = Field(description="Custom index name (auto-generated if not provided)", default=None),
    index_type: str = Field(description="Vector index type: 'hnsw' (recommended) or 'ivfflat'", default="hnsw"),
    index_options: Optional[Dict[str, Any]] = Field(description="""Index-specific options:
    - For HNSW: {"m": 16, "ef_construction": 64} (m: max connections per node, ef_construction: search scope during construction)
    - For IVFFlat: {"lists": 100} (lists: number of inverted lists)""", default=None),
) -> ResponseType:
    """Create a vector index for efficient similarity search."""
    try:
        sql_driver = await get_sql_driver()
        vector_ops = VectorOperations(sql_driver)
        
        result = await vector_ops.create_vector_index(
            table_name=table_name,
            schema=schema,
            vector_column=vector_column,
            index_name=index_name,
            index_type=index_type,
            index_options=index_options
        )
        
        if result.success:
            return format_text_response(result.message)
        else:
            return format_error_response(result.message)
            
    except Exception as e:
        logger.error(f"Error creating vector index: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Perform K-nearest neighbor (KNN) vector search to find the k most similar vectors")
async def vector_knn_search(
    table_name: str = Field(description="Name of the vector table to search"),
    search_vector: List[float] = Field(description="Query vector to search for similar vectors"),
    k: int = Field(description="Number of nearest neighbors to return", default=10),
    schema: str = Field(description="Schema name", default="public"),
    vector_column: str = Field(description="Name of the vector column", default="embedding"),
    distance_function: str = Field(description="Distance function: 'cosine', 'euclidean', or 'inner_product'", default="cosine"),
    additional_columns: Optional[List[str]] = Field(description="Additional columns to include in results", default=None),
    include_distance: bool = Field(description="Include distance in results", default=True),
) -> ResponseType:
    """Perform K-nearest neighbor (KNN) vector search."""
    try:
        sql_driver = await get_sql_driver()
        vector_ops = VectorOperations(sql_driver)
        
        result = await vector_ops.vector_knn_search(
            table_name=table_name,
            schema=schema,
            vector_column=vector_column,
            search_vector=search_vector,
            k=k,
            distance_function=distance_function,
            additional_columns=additional_columns,
            include_distance=include_distance
        )
        
        if result.success:
            response = {
                "message": result.message,
                "data": result.data,
                "columns": result.columns,
                "total_count": result.total_count,
                "execution_time_ms": result.execution_time_ms
            }
            return format_text_response(response)
        else:
            return format_error_response(result.message)
        
    except Exception as e:
        logger.error(f"Error in vector KNN search: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Get statistics and monitoring information for vector indexes")
async def get_vector_index_stats(
    table_name: str = Field(description="Name of the table containing vector indexes"),
    schema: str = Field(description="Schema name", default="public"),
    vector_column: str = Field(description="Name of the vector column", default="embedding"),
) -> ResponseType:
    """Get statistics and monitoring information for vector indexes."""
    try:
        sql_driver = await get_sql_driver()
        vector_ops = VectorOperations(sql_driver)
        
        result = await vector_ops.get_vector_index_stats(
            table_name=table_name,
            schema=schema,
            vector_column=vector_column
        )
        
        if result.success:
            response = {
                "message": result.message,
                "data": result.data,
                "columns": result.columns,
                "total_count": result.total_count,
                "execution_time_ms": result.execution_time_ms
            }
            return format_text_response(response)
        else:
            return format_error_response(result.message)
        
    except Exception as e:
        logger.error(f"Error getting vector index stats: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Optimize a vector index by rebuilding it for better performance")
async def optimize_vector_index(
    index_name: str = Field(description="Name of the vector index to optimize"),
    schema: str = Field(description="Schema name", default="public"),
) -> ResponseType:
    """Optimize a vector index by rebuilding it."""
    try:
        sql_driver = await get_sql_driver()
        vector_ops = VectorOperations(sql_driver)
        
        result = await vector_ops.optimize_vector_index(
            index_name=index_name,
            schema=schema
        )
        
        if result.success:
            response = {
                "message": result.message,
                "operation_type": result.operation_type.value,
                "table_name": result.table_name,
                "execution_time_ms": result.execution_time_ms
            }
            return format_text_response(response)
        else:
            return format_error_response(result.message)
        
    except Exception as e:
        logger.error(f"Error optimizing vector index: {e}")
        return format_error_response(str(e))


async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="PostgreSQL MCP Server")
    parser.add_argument("database_url", help="Database connection URL", nargs="?")
    parser.add_argument(
        "--access-mode",
        type=str,
        choices=[mode.value for mode in AccessMode],
        default=AccessMode.UNRESTRICTED.value,
        help="Set SQL access mode: unrestricted (unrestricted) or restricted (read-only with protections)",
    )
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse"],
        default="stdio",
        help="Select MCP transport: stdio (default) or sse",
    )
    parser.add_argument(
        "--sse-host",
        type=str,
        default="localhost",
        help="Host to bind SSE server to (default: localhost)",
    )
    parser.add_argument(
        "--sse-port",
        type=int,
        default=8000,
        help="Port for SSE server (default: 8000)",
    )

    args = parser.parse_args()

    # Store the access mode in the global variable
    global current_access_mode
    current_access_mode = AccessMode(args.access_mode)

    # Add the query tool with a description appropriate to the access mode
    if current_access_mode == AccessMode.UNRESTRICTED:
        mcp.add_tool(execute_sql, description="Execute any SQL query")
    else:
        mcp.add_tool(execute_sql, description="Execute a read-only SQL query")

    logger.info(f"Starting PostgreSQL MCP Server in {current_access_mode.upper()} mode")

    # Get database URL from environment variable or command line
    database_url = os.environ.get("DATABASE_URI", args.database_url)

    if not database_url:
        raise ValueError(
            "Error: No database URL provided. Please specify via 'DATABASE_URI' environment variable or command-line argument.",
        )

    # Initialize database connection pool
    try:
        await db_connection.pool_connect(database_url)
        logger.info("Successfully connected to database and initialized connection pool")
    except Exception as e:
        logger.warning(
            f"Could not connect to database: {obfuscate_password(str(e))}",
        )
        logger.warning(
            "The MCP server will start but database operations will fail until a valid connection is established.",
        )

    # Set up proper shutdown handling
    try:
        loop = asyncio.get_running_loop()
        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s)))
    except NotImplementedError:
        # Windows doesn't support signals properly
        logger.warning("Signal handling not supported on Windows")
        pass

    # Run the server with the selected transport (always async)
    if args.transport == "stdio":
        await mcp.run_stdio_async()
    else:
        # Update FastMCP settings based on command line arguments
        mcp.settings.host = args.sse_host
        mcp.settings.port = args.sse_port
        await mcp.run_sse_async()


async def shutdown(sig=None):
    """Clean shutdown of the server."""
    global shutdown_in_progress

    if shutdown_in_progress:
        logger.warning("Forcing immediate exit")
        # Use sys.exit instead of os._exit to allow for proper cleanup
        sys.exit(1)

    shutdown_in_progress = True

    if sig:
        logger.info(f"Received exit signal {sig.name}")

    # Close database connections
    try:
        await db_connection.close()
        logger.info("Closed database connections")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

    # Exit with appropriate status code
    sys.exit(128 + sig if sig is not None else 0)
