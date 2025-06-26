"""
数据库模式操作实现类

提供数据库模式、索引、扩展等管理功能。
"""

import logging
import time
from typing import Any, Dict, List, Optional
from psycopg.sql import SQL, Identifier, Literal, Composed

from ..sql.sql_driver import SqlDriver
from .data_types import (
    DatabaseResult,
    IndexDefinition,
    OperationResult,
    OperationType,
)

logger = logging.getLogger(__name__)


class SchemaOperations:
    """数据库模式操作类"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    async def create_schema(self, schema_name: str, if_not_exists: bool = True) -> OperationResult:
        """创建数据库模式"""
        start_time = time.time()
        
        try:
            if if_not_exists:
                sql = SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema_name))
            else:
                sql = SQL("CREATE SCHEMA {}").format(Identifier(schema_name))
            
            sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
            logger.info(f"Creating schema: {sql_str}")
            
            await self.sql_driver.execute_query(sql_str)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Schema {schema_name} created successfully",
                operation_type=OperationType.CREATE,
                table_name=schema_name,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error creating schema {schema_name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to create schema: {str(e)}",
                operation_type=OperationType.CREATE,
                table_name=schema_name,
                execution_time_ms=execution_time
            )

    async def drop_schema(self, schema_name: str, cascade: bool = False, if_exists: bool = True) -> OperationResult:
        """删除数据库模式"""
        start_time = time.time()
        
        try:
            sql_parts = ["DROP SCHEMA"]
            
            if if_exists:
                sql_parts.append("IF EXISTS")
            
            sql_parts.append("{}")
            
            if cascade:
                sql_parts.append("CASCADE")
            
            sql = SQL(" ".join(sql_parts)).format(Identifier(schema_name))
            sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
            
            logger.info(f"Dropping schema: {sql_str}")
            
            await self.sql_driver.execute_query(sql_str)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Schema {schema_name} dropped successfully",
                operation_type=OperationType.DROP,
                table_name=schema_name,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error dropping schema {schema_name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to drop schema: {str(e)}",
                operation_type=OperationType.DROP,
                table_name=schema_name,
                execution_time_ms=execution_time
            )

    async def create_index(self, index_def: IndexDefinition) -> OperationResult:
        """创建索引"""
        start_time = time.time()
        
        try:
            sql_parts = ["CREATE"]
            
            if index_def.unique:
                sql_parts.append("UNIQUE")
            
            sql_parts.append("INDEX {} ON {} USING {} ({})")
            
            if index_def.partial_condition:
                sql_parts.append("WHERE {}")
            
            # 处理向量索引的特殊选项
            if index_def.index_type.lower() in ["hnsw", "ivfflat"] and index_def.vector_index_options:
                with_options = []
                for key, value in index_def.vector_index_options.items():
                    if isinstance(value, str):
                        with_options.append(f"{key} = '{value}'")
                    else:
                        with_options.append(f"{key} = {value}")
                
                if with_options:
                    sql_parts.append("WITH ({})")
            
            base_sql = " ".join(sql_parts)
            
            # 构建完整的SQL
            if index_def.partial_condition:
                if index_def.vector_index_options and index_def.index_type.lower() in ["hnsw", "ivfflat"]:
                    sql = SQL(base_sql).format(
                        Identifier(index_def.name),
                        Identifier(index_def.table_name),
                        SQL(index_def.index_type),
                        SQL(", ").join(Identifier(col) for col in index_def.columns),
                        SQL(index_def.partial_condition),
                        SQL(", ".join(f"{k} = {v}" for k, v in index_def.vector_index_options.items()))
                    )
                else:
                    sql = SQL(base_sql).format(
                        Identifier(index_def.name),
                        Identifier(index_def.table_name),
                        SQL(index_def.index_type),
                        SQL(", ").join(Identifier(col) for col in index_def.columns),
                        SQL(index_def.partial_condition)
                    )
            else:
                if index_def.vector_index_options and index_def.index_type.lower() in ["hnsw", "ivfflat"]:
                    with_clause = ", ".join(f"{k} = {v}" for k, v in index_def.vector_index_options.items())
                    sql = SQL(base_sql).format(
                        Identifier(index_def.name),
                        Identifier(index_def.table_name),
                        SQL(index_def.index_type),
                        SQL(", ").join(Identifier(col) for col in index_def.columns),
                        SQL(with_clause)
                    )
                else:
                    sql = SQL("CREATE{} INDEX {} ON {} USING {} ({})").format(
                        SQL(" UNIQUE" if index_def.unique else ""),
                        Identifier(index_def.name),
                        Identifier(index_def.table_name),
                        SQL(index_def.index_type),
                        SQL(", ").join(Identifier(col) for col in index_def.columns)
                    )
            
            sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
            logger.info(f"Creating index: {sql_str}")
            
            await self.sql_driver.execute_query(sql_str)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Index {index_def.name} created successfully",
                operation_type=OperationType.CREATE,
                table_name=index_def.table_name,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error creating index {index_def.name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to create index: {str(e)}",
                operation_type=OperationType.CREATE,
                table_name=index_def.table_name,
                execution_time_ms=execution_time
            )

    async def drop_index(self, index_name: str, if_exists: bool = True, cascade: bool = False) -> OperationResult:
        """删除索引"""
        start_time = time.time()
        
        try:
            sql_parts = ["DROP INDEX"]
            
            if if_exists:
                sql_parts.append("IF EXISTS")
            
            sql_parts.append("{}")
            
            if cascade:
                sql_parts.append("CASCADE")
            
            sql = SQL(" ".join(sql_parts)).format(Identifier(index_name))
            sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
            
            logger.info(f"Dropping index: {sql_str}")
            
            await self.sql_driver.execute_query(sql_str)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Index {index_name} dropped successfully",
                operation_type=OperationType.DROP,
                table_name=index_name,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error dropping index {index_name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to drop index: {str(e)}",
                operation_type=OperationType.DROP,
                table_name=index_name,
                execution_time_ms=execution_time
            )

    async def drop_table(self, table_name: str, schema: str = "public", 
                        cascade: bool = False, if_exists: bool = True) -> OperationResult:
        """删除表"""
        start_time = time.time()
        
        try:
            sql_parts = ["DROP TABLE"]
            
            if if_exists:
                sql_parts.append("IF EXISTS")
            
            sql_parts.append("{}.{}")
            
            if cascade:
                sql_parts.append("CASCADE")
            
            sql = SQL(" ".join(sql_parts)).format(
                Identifier(schema),
                Identifier(table_name)
            )
            sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
            
            logger.info(f"Dropping table: {sql_str}")
            
            await self.sql_driver.execute_query(sql_str)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Table {schema}.{table_name} dropped successfully",
                operation_type=OperationType.DROP,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error dropping table {table_name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to drop table: {str(e)}",
                operation_type=OperationType.DROP,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )

    async def create_extension(self, extension_name: str, if_not_exists: bool = True, 
                             schema: Optional[str] = None) -> OperationResult:
        """创建扩展"""
        start_time = time.time()
        
        try:
            sql_parts = ["CREATE EXTENSION"]
            
            if if_not_exists:
                sql_parts.append("IF NOT EXISTS")
            
            sql_parts.append("{}")
            
            if schema:
                sql_parts.append("WITH SCHEMA {}")
            
            if schema:
                sql = SQL(" ".join(sql_parts)).format(
                    Identifier(extension_name),
                    Identifier(schema)
                )
            else:
                sql = SQL(" ".join(sql_parts)).format(Identifier(extension_name))
            
            sql_str = sql.as_string(self.sql_driver.conn.pool._conninfo)  # type: ignore
            logger.info(f"Creating extension: {sql_str}")
            
            await self.sql_driver.execute_query(sql_str)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Extension {extension_name} created successfully",
                operation_type=OperationType.CREATE,
                table_name=extension_name,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error creating extension {extension_name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to create extension: {str(e)}",
                operation_type=OperationType.CREATE,
                table_name=extension_name,
                execution_time_ms=execution_time
            ) 