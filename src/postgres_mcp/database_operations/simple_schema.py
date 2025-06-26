"""
简化的数据库模式操作实现类

提供数据库模式、索引、扩展等管理功能，使用简单的字符串SQL构建。
"""

import logging
import time
from typing import Any, Dict, List, Optional

from ..sql.sql_driver import SqlDriver
from .data_types import (
    IndexDefinition,
    OperationResult,
    OperationType,
)

logger = logging.getLogger(__name__)


class SimpleSchemaOperations:
    """简化的数据库模式操作类"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    async def create_schema(self, schema_name: str, if_not_exists: bool = True) -> OperationResult:
        """创建数据库模式"""
        start_time = time.time()
        
        try:
            if if_not_exists:
                sql = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'
            else:
                sql = f'CREATE SCHEMA "{schema_name}"'
            
            logger.info(f"Creating schema: {schema_name}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
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
            
            sql_parts.append(f'"{schema_name}"')
            
            if cascade:
                sql_parts.append("CASCADE")
            
            sql = " ".join(sql_parts)
            
            logger.info(f"Dropping schema: {schema_name}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
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

    async def create_index(self, index_def: IndexDefinition, schema: str = "public") -> OperationResult:
        """创建索引"""
        start_time = time.time()
        
        try:
            sql_parts = ["CREATE"]
            
            if index_def.unique:
                sql_parts.append("UNIQUE")
            
            sql_parts.append("INDEX")
            sql_parts.append(f'"{index_def.name}"')
            sql_parts.append("ON")
            sql_parts.append(f'"{schema}"."{index_def.table_name}"')
            sql_parts.append("USING")
            sql_parts.append(index_def.index_type)
            
            # 添加列
            quoted_columns = ", ".join([f'"{col}"' for col in index_def.columns])
            sql_parts.append(f"({quoted_columns})")
            
            # 添加WHERE子句（部分索引）
            if index_def.partial_condition:
                sql_parts.append("WHERE")
                sql_parts.append(index_def.partial_condition)
            
            # 处理向量索引的特殊选项
            if index_def.index_type.lower() in ["hnsw", "ivfflat"] and index_def.vector_index_options:
                with_options = []
                for key, value in index_def.vector_index_options.items():
                    if isinstance(value, str):
                        with_options.append(f"{key} = '{value}'")
                    else:
                        with_options.append(f"{key} = {value}")
                
                if with_options:
                    sql_parts.append("WITH")
                    sql_parts.append(f"({', '.join(with_options)})")
            
            sql = " ".join(sql_parts)
            
            logger.info(f"Creating index: {index_def.name}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
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
            
            sql_parts.append(f'"{index_name}"')
            
            if cascade:
                sql_parts.append("CASCADE")
            
            sql = " ".join(sql_parts)
            
            logger.info(f"Dropping index: {index_name}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
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
            
            sql_parts.append(f'"{schema}"."{table_name}"')
            
            if cascade:
                sql_parts.append("CASCADE")
            
            sql = " ".join(sql_parts)
            
            logger.info(f"Dropping table: {schema}.{table_name}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
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
            
            sql_parts.append(f'"{extension_name}"')
            
            if schema:
                sql_parts.append("WITH SCHEMA")
                sql_parts.append(f'"{schema}"')
            
            sql = " ".join(sql_parts)
            
            logger.info(f"Creating extension: {extension_name}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
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