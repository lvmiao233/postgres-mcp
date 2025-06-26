"""
简化的CRUD操作实现类

提供基本的数据库增删改查操作，使用简单的字符串SQL构建。
"""

import logging
import time
from typing import Any, Dict, List, Optional

from ..sql.sql_driver import SqlDriver
from .data_types import (
    ColumnDefinition, 
    DataType,
    OperationResult,
    OperationType,
    QueryCondition,
    QueryOptions,
    QueryResult,
    TableDefinition,
)

logger = logging.getLogger(__name__)


class SimpleCrudOperations:
    """简化的CRUD操作类"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def _format_data_type(self, column: ColumnDefinition) -> str:
        """格式化数据类型为PostgreSQL语法"""
        if column.data_type == DataType.VECTOR:
            if column.vector_dimensions is None:
                raise ValueError(f"Vector column {column.name} must specify vector_dimensions")
            return f"vector({column.vector_dimensions})"
        elif column.data_type in [DataType.VARCHAR, DataType.CHAR]:
            return f"{column.data_type.value}(255)"
        elif column.data_type in [DataType.DECIMAL, DataType.NUMERIC]:
            return f"{column.data_type.value}(10,2)"
        else:
            return column.data_type.value

    async def create_table(self, table_def: TableDefinition) -> OperationResult:
        """创建表"""
        start_time = time.time()
        
        try:
            # 构建列定义
            column_defs = []
            for col in table_def.columns:
                col_def = f'"{col.name}" {self._format_data_type(col)}'
                
                if not col.nullable:
                    col_def += " NOT NULL"
                
                if col.default is not None:
                    # 特殊函数名称不需要引号
                    special_functions = ['CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME', 'NOW()', 'UUID_GENERATE_V4()']
                    if isinstance(col.default, str) and col.default not in special_functions:
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
            sql = f'CREATE TABLE "{table_def.schema}"."{table_def.name}" ({columns_sql})'
            
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
            if not data:
                raise ValueError("No data provided for insert")

            # 获取所有列名
            columns = list(data[0].keys())
            
            # 构建SQL
            quoted_columns = ", ".join([f'"{col}"' for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f'INSERT INTO "{schema}"."{table_name}" ({quoted_columns}) VALUES ({placeholders})'
            
            # 添加RETURNING子句
            if returning_columns:
                quoted_returning = ", ".join([f'"{col}"' for col in returning_columns])
                sql += f" RETURNING {quoted_returning}"
            
            logger.info(f"Inserting {len(data)} record(s) into {schema}.{table_name}")
            logger.debug(f"SQL: {sql}")
            
            returned_data = []
            affected_rows = 0
            
            # 执行插入操作
            for row in data:
                params = []
                for col in columns:
                    value = row.get(col)
                    # 如果值是字典或列表，转换为JSON字符串
                    if isinstance(value, (dict, list)):
                        import json
                        value = json.dumps(value)
                    params.append(value)
                
                result = await self.sql_driver.execute_query(sql, params)
                affected_rows += 1
                
                if result and returning_columns:
                    returned_data.extend([row.cells for row in result])
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Successfully inserted {affected_rows} record(s) into {schema}.{table_name}",
                operation_type=OperationType.INSERT,
                table_name=f"{schema}.{table_name}",
                affected_rows=affected_rows,
                execution_time_ms=execution_time,
                returned_data=returned_data
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

    async def query_records(self, table_name: str, columns: Optional[List[str]] = None,
                          conditions: Optional[List[QueryCondition]] = None,
                          options: Optional[QueryOptions] = None, 
                          schema: str = "public") -> QueryResult:
        """查询记录"""
        start_time = time.time()
        
        try:
            # 构建SELECT子句
            if columns:
                select_columns = ", ".join([f'"{col}"' for col in columns])
            else:
                select_columns = "*"

            sql = f'SELECT {select_columns} FROM "{schema}"."{table_name}"'
            params = []

            # 添加WHERE子句
            if conditions:
                where_parts = []
                for i, condition in enumerate(conditions):
                    if i > 0:
                        logical_op = conditions[i-1].logical_operator.upper()
                        where_parts.append(f" {logical_op} ")
                    
                    if condition.operator.upper() in ["IS NULL", "IS NOT NULL"]:
                        where_parts.append(f'"{condition.column}" {condition.operator.upper()}')
                    elif condition.operator.upper() in ["IN", "NOT IN"]:
                        if not isinstance(condition.value, (list, tuple)):
                            raise ValueError(f"Value for {condition.operator} must be a list or tuple")
                        placeholders = ", ".join(["%s"] * len(condition.value))
                        where_parts.append(f'"{condition.column}" {condition.operator.upper()} ({placeholders})')
                        params.extend(condition.value)
                    else:
                        where_parts.append(f'"{condition.column}" {condition.operator} %s')
                        params.append(condition.value)
                
                if where_parts:
                    sql += " WHERE " + "".join(where_parts)

            # 添加其他选项
            if options:
                if options.order_by:
                    order_parts = []
                    for order in options.order_by:
                        direction = order.get("direction", "ASC").upper()
                        order_parts.append(f'"{order["column"]}" {direction}')
                    sql += " ORDER BY " + ", ".join(order_parts)
                
                if options.limit:
                    sql += f" LIMIT {options.limit}"
                
                if options.offset:
                    sql += f" OFFSET {options.offset}"

            logger.info(f"Querying records from {schema}.{table_name}")
            logger.debug(f"SQL: {sql}")
            
            result = await self.sql_driver.execute_query(sql, params)
            
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

    async def update_records(self, table_name: str, data: Dict[str, Any], 
                           conditions: List[QueryCondition], schema: str = "public",
                           returning_columns: Optional[List[str]] = None) -> OperationResult:
        """更新记录"""
        start_time = time.time()
        
        try:
            if not data:
                raise ValueError("No data provided for update")

            # 构建SET子句
            set_parts = []
            params = []
            for column, value in data.items():
                set_parts.append(f'"{column}" = %s')
                params.append(value)

            sql = f'UPDATE "{schema}"."{table_name}" SET {", ".join(set_parts)}'

            # 添加WHERE子句
            if conditions:
                where_parts = []
                for i, condition in enumerate(conditions):
                    if i > 0:
                        logical_op = conditions[i-1].logical_operator.upper()
                        where_parts.append(f" {logical_op} ")
                    
                    if condition.operator.upper() in ["IS NULL", "IS NOT NULL"]:
                        where_parts.append(f'"{condition.column}" {condition.operator.upper()}')
                    elif condition.operator.upper() in ["IN", "NOT IN"]:
                        if not isinstance(condition.value, (list, tuple)):
                            raise ValueError(f"Value for {condition.operator} must be a list or tuple")
                        placeholders = ", ".join(["%s"] * len(condition.value))
                        where_parts.append(f'"{condition.column}" {condition.operator.upper()} ({placeholders})')
                        params.extend(condition.value)
                    else:
                        where_parts.append(f'"{condition.column}" {condition.operator} %s')
                        params.append(condition.value)
                
                if where_parts:
                    sql += " WHERE " + "".join(where_parts)

            # 添加RETURNING子句
            if returning_columns:
                quoted_returning = ", ".join([f'"{col}"' for col in returning_columns])
                sql += f" RETURNING {quoted_returning}"
            
            logger.info(f"Updating records in {schema}.{table_name}")
            logger.debug(f"SQL: {sql}")
            
            result = await self.sql_driver.execute_query(sql, params)
            
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
            sql = f'DELETE FROM "{schema}"."{table_name}"'
            params = []

            # 添加WHERE子句
            if conditions:
                where_parts = []
                for i, condition in enumerate(conditions):
                    if i > 0:
                        logical_op = conditions[i-1].logical_operator.upper()
                        where_parts.append(f" {logical_op} ")
                    
                    if condition.operator.upper() in ["IS NULL", "IS NOT NULL"]:
                        where_parts.append(f'"{condition.column}" {condition.operator.upper()}')
                    elif condition.operator.upper() in ["IN", "NOT IN"]:
                        if not isinstance(condition.value, (list, tuple)):
                            raise ValueError(f"Value for {condition.operator} must be a list or tuple")
                        placeholders = ", ".join(["%s"] * len(condition.value))
                        where_parts.append(f'"{condition.column}" {condition.operator.upper()} ({placeholders})')
                        params.extend(condition.value)
                    else:
                        where_parts.append(f'"{condition.column}" {condition.operator} %s')
                        params.append(condition.value)
                
                if where_parts:
                    sql += " WHERE " + "".join(where_parts)

            # 添加RETURNING子句
            if returning_columns:
                quoted_returning = ", ".join([f'"{col}"' for col in returning_columns])
                sql += f" RETURNING {quoted_returning}"
            
            logger.info(f"Deleting records from {schema}.{table_name}")
            logger.debug(f"SQL: {sql}")
            
            result = await self.sql_driver.execute_query(sql, params)
            
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