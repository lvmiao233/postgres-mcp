"""
CRUD操作实现类

提供统一的数据库增删改查操作，支持参数化查询和批量操作。
"""

import logging
import time
from typing import Any, Dict, List, Optional, Union
from psycopg.sql import SQL, Identifier, Literal, Composed

from ..sql.sql_driver import SqlDriver
from .data_types import (
    ColumnDefinition, 
    DatabaseResult,
    DataType,
    IndexDefinition,
    OperationResult,
    OperationType,
    QueryCondition,
    QueryOptions,
    QueryResult,
    TableDefinition,
    VectorSearchOptions,
)

logger = logging.getLogger(__name__)


class CrudOperations:
    """统一的CRUD操作类"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def _format_data_type(self, column: ColumnDefinition) -> str:
        """格式化数据类型为PostgreSQL语法"""
        if column.data_type == DataType.VECTOR:
            if column.vector_dimensions is None:
                raise ValueError(f"Vector column {column.name} must specify vector_dimensions")
            return f"vector({column.vector_dimensions})"
        elif column.data_type in [DataType.VARCHAR, DataType.CHAR]:
            # 默认长度，可以后续扩展为参数
            return f"{column.data_type.value}(255)"
        elif column.data_type in [DataType.DECIMAL, DataType.NUMERIC]:
            # 默认精度，可以后续扩展为参数
            return f"{column.data_type.value}(10,2)"
        else:
            return column.data_type.value

    def _build_create_table_sql(self, table_def: TableDefinition) -> str:
        """构建CREATE TABLE SQL语句"""
        # 构建列定义
        column_defs = []
        for col in table_def.columns:
            col_def = f'"{col.name}" {self._format_data_type(col)}'
            
            if not col.nullable:
                col_def += " NOT NULL"
            
            if col.default is not None:
                if isinstance(col.default, str):
                    col_def += f" DEFAULT '{col.default}'"
                else:
                    col_def += f" DEFAULT {col.default}"
            
            if col.primary_key:
                col_def += " PRIMARY KEY"
            
            if col.unique and not col.primary_key:
                col_def += " UNIQUE"
            
            if col.check_constraint:
                col_def += f" CHECK ({col.check_constraint})"
            
            column_defs.append(col_def)

        # 添加表级约束
        if table_def.constraints:
            column_defs.extend(table_def.constraints)

        columns_sql = ", ".join(column_defs)
        
        return f'CREATE TABLE "{table_def.schema}"."{table_def.name}" ({columns_sql})'

    def _build_insert_sql(self, table_name: str, data: List[Dict[str, Any]], 
                         schema: str = "public", returning_columns: Optional[List[str]] = None) -> tuple[str, List[List[Any]]]:
        """构建INSERT SQL语句"""
        if not data:
            raise ValueError("No data provided for insert")

        # 获取所有列名
        columns = list(data[0].keys())
        
        # 构建参数占位符
        placeholders = ", ".join(["%s"] * len(columns))
        
        # 构建SQL
        quoted_columns = ", ".join([f'"{col}"' for col in columns])
        sql = f'INSERT INTO "{schema}"."{table_name}" ({quoted_columns}) VALUES ({placeholders})'
        
        # 添加RETURNING子句
        if returning_columns:
            quoted_returning = ", ".join([f'"{col}"' for col in returning_columns])
            sql += f" RETURNING {quoted_returning}"
        
        # 准备参数
        params = []
        for row in data:
            row_params = [row.get(col) for col in columns]
            params.append(row_params)
        
        return sql, params

    def _build_update_sql(self, table_name: str, data: Dict[str, Any], 
                         conditions: List[QueryCondition], schema: str = "public",
                         returning_columns: Optional[List[str]] = None) -> tuple[Composed, List[Any]]:
        """构建UPDATE SQL语句"""
        if not data:
            raise ValueError("No data provided for update")

        # 构建SET子句
        set_clauses = []
        params = []
        for column, value in data.items():
            set_clauses.append(SQL("{} = %s").format(Identifier(column)))
            params.append(value)

        # 构建WHERE子句
        where_clauses, where_params = self._build_where_clause(conditions)
        params.extend(where_params)

        # 构建完整SQL
        sql_parts = [
            SQL("UPDATE {}.{} SET {}").format(
                Identifier(schema),
                Identifier(table_name),
                SQL(", ").join(set_clauses)
            )
        ]
        
        if where_clauses:
            sql_parts.append(SQL(" WHERE {}").format(where_clauses))
        
        # 添加RETURNING子句
        if returning_columns:
            sql_parts.append(SQL(" RETURNING {}").format(
                SQL(", ").join(Identifier(col) for col in returning_columns)
            ))
        
        sql = SQL("").join(sql_parts)
        return sql, params

    def _build_delete_sql(self, table_name: str, conditions: List[QueryCondition], 
                         schema: str = "public", returning_columns: Optional[List[str]] = None) -> tuple[Composed, List[Any]]:
        """构建DELETE SQL语句"""
        # 构建WHERE子句
        where_clauses, params = self._build_where_clause(conditions)

        # 构建完整SQL
        sql_parts = [
            SQL("DELETE FROM {}.{}").format(
                Identifier(schema),
                Identifier(table_name)
            )
        ]
        
        if where_clauses:
            sql_parts.append(SQL(" WHERE {}").format(where_clauses))
        
        # 添加RETURNING子句
        if returning_columns:
            sql_parts.append(SQL(" RETURNING {}").format(
                SQL(", ").join(Identifier(col) for col in returning_columns)
            ))
        
        sql = SQL("").join(sql_parts)
        return sql, params

    def _build_select_sql(self, table_name: str, columns: Optional[List[str]] = None,
                         conditions: Optional[List[QueryCondition]] = None,
                         options: Optional[QueryOptions] = None, 
                         schema: str = "public") -> tuple[Composed, List[Any]]:
        """构建SELECT SQL语句"""
        # 选择列
        if columns:
            select_columns = SQL(", ").join(Identifier(col) for col in columns)
        else:
            select_columns = SQL("*")

        # 构建基础SQL
        sql_parts = [
            SQL("SELECT {} FROM {}.{}").format(
                select_columns,
                Identifier(schema),
                Identifier(table_name)
            )
        ]
        params = []

        # 添加WHERE子句
        if conditions:
            where_clauses, where_params = self._build_where_clause(conditions)
            if where_clauses:
                sql_parts.append(SQL(" WHERE {}").format(where_clauses))
                params.extend(where_params)

        # 添加其他选项
        if options:
            if options.group_by:
                sql_parts.append(SQL(" GROUP BY {}").format(
                    SQL(", ").join(Identifier(col) for col in options.group_by)
                ))
            
            if options.having:
                having_clauses, having_params = self._build_where_clause(options.having)
                if having_clauses:
                    sql_parts.append(SQL(" HAVING {}").format(having_clauses))
                    params.extend(having_params)
            
            if options.order_by:
                order_clauses = []
                for order in options.order_by:
                    direction = order.get("direction", "ASC").upper()
                    order_clauses.append(SQL("{} {}").format(
                        Identifier(order["column"]),
                        SQL(direction)
                    ))
                sql_parts.append(SQL(" ORDER BY {}").format(SQL(", ").join(order_clauses)))
            
            if options.limit:
                sql_parts.append(SQL(" LIMIT %s"))
                params.append(options.limit)
            
            if options.offset:
                sql_parts.append(SQL(" OFFSET %s"))
                params.append(options.offset)

        sql = SQL("").join(sql_parts)
        return sql, params

    def _build_where_clause(self, conditions: List[QueryCondition]) -> tuple[Optional[Composed], List[Any]]:
        """构建WHERE子句"""
        if not conditions:
            return None, []

        clauses = []
        params = []
        
        for i, condition in enumerate(conditions):
            if condition.operator.upper() in ["IS NULL", "IS NOT NULL"]:
                clause = SQL("{} {}").format(
                    Identifier(condition.column),
                    SQL(condition.operator.upper())
                )
            elif condition.operator.upper() in ["IN", "NOT IN"]:
                if not isinstance(condition.value, (list, tuple)):
                    raise ValueError(f"Value for {condition.operator} must be a list or tuple")
                placeholders = SQL(", ").join([SQL("%s")] * len(condition.value))
                clause = SQL("{} {} ({})").format(
                    Identifier(condition.column),
                    SQL(condition.operator.upper()),
                    placeholders
                )
                params.extend(condition.value)
            else:
                clause = SQL("{} {} %s").format(
                    Identifier(condition.column),
                    SQL(condition.operator)
                )
                params.append(condition.value)
            
            if i > 0:
                logical_op = conditions[i-1].logical_operator.upper()
                clauses.append(SQL(f" {logical_op} "))
            
            clauses.append(clause)

        return SQL("").join(clauses), params

    async def create_table(self, table_def: TableDefinition) -> OperationResult:
        """创建表"""
        start_time = time.time()
        
        try:
            sql = self._build_create_table_sql(table_def)
            logger.info(f"Creating table: {table_def.schema}.{table_def.name}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Table {table_def.schema}.{table_def.name} created successfully",
                operation_type=OperationType.CREATE,
                table_name=f"{table_def.schema}.{table_def.name}",
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error creating table {table_def.name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to create table: {str(e)}",
                operation_type=OperationType.CREATE,
                table_name=f"{table_def.schema}.{table_def.name}",
                execution_time_ms=execution_time
            )

    async def insert_records(self, table_name: str, data: List[Dict[str, Any]], 
                           schema: str = "public", returning_columns: Optional[List[str]] = None) -> OperationResult:
        """插入记录（支持批量）"""
        start_time = time.time()
        
        try:
            if len(data) == 1:
                # 单条记录插入
                sql, params = self._build_insert_sql(table_name, data, schema, returning_columns)
                sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
                
                logger.info(f"Inserting single record: {sql_str}")
                
                result = await self.sql_driver.execute_query(sql_str, params[0])
            else:
                # 批量插入
                sql, all_params = self._build_insert_sql(table_name, data, schema, returning_columns)
                sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
                
                logger.info(f"Batch inserting {len(data)} records: {sql_str}")
                
                # 执行批量插入
                returned_data = []
                for params in all_params:
                    result = await self.sql_driver.execute_query(sql_str, params)
                    if result and returning_columns:
                        returned_data.extend([row.cells for row in result])
                
                result = returned_data if returning_columns else None
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Successfully inserted {len(data)} record(s) into {schema}.{table_name}",
                operation_type=OperationType.INSERT,
                table_name=f"{schema}.{table_name}",
                affected_rows=len(data),
                execution_time_ms=execution_time,
                returned_data=[row.cells for row in result] if result else []
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error inserting records into {table_name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to insert records: {str(e)}",
                operation_type=OperationType.INSERT,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )

    async def update_records(self, table_name: str, data: Dict[str, Any], 
                           conditions: List[QueryCondition], schema: str = "public",
                           returning_columns: Optional[List[str]] = None) -> OperationResult:
        """更新记录"""
        start_time = time.time()
        
        try:
            sql, params = self._build_update_sql(table_name, data, conditions, schema, returning_columns)
            sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
            
            logger.info(f"Updating records: {sql_str}")
            
            result = await self.sql_driver.execute_query(sql_str, params)
            
            execution_time = (time.time() - start_time) * 1000
            affected_rows = len(result) if result else 0
            
            return OperationResult(
                success=True,
                message=f"Successfully updated {affected_rows} record(s) in {schema}.{table_name}",
                operation_type=OperationType.UPDATE,
                table_name=f"{schema}.{table_name}",
                affected_rows=affected_rows,
                execution_time_ms=execution_time,
                returned_data=[row.cells for row in result] if result else []
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error updating records in {table_name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to update records: {str(e)}",
                operation_type=OperationType.UPDATE,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )

    async def delete_records(self, table_name: str, conditions: List[QueryCondition], 
                           schema: str = "public", returning_columns: Optional[List[str]] = None) -> OperationResult:
        """删除记录"""
        start_time = time.time()
        
        try:
            sql, params = self._build_delete_sql(table_name, conditions, schema, returning_columns)
            sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
            
            logger.info(f"Deleting records: {sql_str}")
            
            result = await self.sql_driver.execute_query(sql_str, params)
            
            execution_time = (time.time() - start_time) * 1000
            affected_rows = len(result) if result else 0
            
            return OperationResult(
                success=True,
                message=f"Successfully deleted {affected_rows} record(s) from {schema}.{table_name}",
                operation_type=OperationType.DELETE,
                table_name=f"{schema}.{table_name}",
                affected_rows=affected_rows,
                execution_time_ms=execution_time,
                returned_data=[row.cells for row in result] if result else []
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error deleting records from {table_name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to delete records: {str(e)}",
                operation_type=OperationType.DELETE,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )

    async def query_records(self, table_name: str, columns: Optional[List[str]] = None,
                          conditions: Optional[List[QueryCondition]] = None,
                          options: Optional[QueryOptions] = None, 
                          schema: str = "public") -> QueryResult:
        """查询记录"""
        start_time = time.time()
        
        try:
            sql, params = self._build_select_sql(table_name, columns, conditions, options, schema)
            sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
            
            logger.info(f"Querying records: {sql_str}")
            
            result = await self.sql_driver.execute_query(sql_str, params)
            
            execution_time = (time.time() - start_time) * 1000
            
            data = [row.cells for row in result] if result else []
            column_names = list(data[0].keys()) if data else []
            
            return QueryResult(
                success=True,
                message=f"Successfully queried {len(data)} record(s) from {schema}.{table_name}",
                data=data,
                columns=column_names,
                total_count=len(data),
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error querying records from {table_name}: {e}")
            
            return QueryResult(
                success=False,
                message=f"Failed to query records: {str(e)}",
                data=[],
                execution_time_ms=execution_time
            ) 