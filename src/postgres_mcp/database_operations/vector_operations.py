"""
向量数据库操作实现类

提供pgvector扩展的专用功能：
- 向量数据插入和查询
- 向量相似性搜索
- 向量索引管理
- 距离函数支持
"""

import logging
import time
from typing import Any, Dict, List, Optional

from ..sql.sql_driver import SqlDriver
from .data_types import (
    ColumnDefinition,
    DataType,
    IndexDefinition,
    OperationResult,
    OperationType,
    QueryResult,
    VectorSearchOptions,
)

logger = logging.getLogger(__name__)


class VectorOperations:
    """向量数据库操作类"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    async def create_vector_table(self, table_name: str, schema: str = "public",
                                 vector_column: str = "embedding", 
                                 vector_dimensions: int = 1536,
                                 additional_columns: Optional[List[ColumnDefinition]] = None) -> OperationResult:
        """创建专用的向量表"""
        start_time = time.time()
        
        try:
            # 构建基础列
            columns = [
                ColumnDefinition(
                    name="id",
                    data_type=DataType.SERIAL,
                    primary_key=True,
                    nullable=False
                ),
                ColumnDefinition(
                    name=vector_column,
                    data_type=DataType.VECTOR,
                    vector_dimensions=vector_dimensions,
                    nullable=True
                ),
                ColumnDefinition(
                    name="created_at",
                    data_type=DataType.TIMESTAMP,
                    default="CURRENT_TIMESTAMP",
                    nullable=False
                )
            ]
            
            # 添加额外列
            if additional_columns:
                columns.extend(additional_columns)
            
            # 构建SQL
            column_defs = []
            for col in columns:
                col_def = f'"{col.name}" {self._format_data_type(col)}'
                
                if not col.nullable:
                    col_def += " NOT NULL"
                
                if col.default is not None:
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
            
            columns_sql = ", ".join(column_defs)
            sql = f'CREATE TABLE "{schema}"."{table_name}" ({columns_sql})'
            
            logger.info(f"Creating vector table: {schema}.{table_name} with {vector_dimensions}D {vector_column}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Vector table {schema}.{table_name} created successfully with {vector_dimensions}D {vector_column} column",
                operation_type=OperationType.CREATE,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error creating vector table {table_name}: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to create vector table: {str(e)}",
                operation_type=OperationType.CREATE,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )

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

    async def vector_similarity_search(self, table_name: str, schema: str = "public",
                                     vector_column: str = "embedding",
                                     search_options: VectorSearchOptions = None,
                                     additional_columns: Optional[List[str]] = None) -> QueryResult:
        """向量相似性搜索"""
        start_time = time.time()
        
        try:
            if search_options is None:
                raise ValueError("search_options is required for vector similarity search")
            
            # 构建SELECT子句
            select_columns = ["id"]
            if additional_columns:
                select_columns.extend(additional_columns)
            
            if search_options.include_distance:
                distance_expr = self._get_distance_expression(
                    vector_column, 
                    search_options.vector, 
                    search_options.distance_function
                )
                select_columns.append(f"{distance_expr} AS distance")
            
            select_sql = ", ".join([f'"{col}"' if col != f"{distance_expr} AS distance" else col for col in select_columns])
            
            # 构建查询SQL
            sql = f'SELECT {select_sql} FROM "{schema}"."{table_name}"'
            
            # 添加距离阈值条件
            params = []
            if search_options.threshold is not None:
                distance_expr = self._get_distance_expression(
                    vector_column, 
                    search_options.vector, 
                    search_options.distance_function
                )
                sql += f" WHERE {distance_expr} < %s"
                params.append(search_options.threshold)
            
            # 添加ORDER BY（按距离排序）
            distance_expr = self._get_distance_expression(
                vector_column, 
                search_options.vector, 
                search_options.distance_function
            )
            sql += f" ORDER BY {distance_expr}"
            
            # 添加LIMIT
            if search_options.limit:
                sql += f" LIMIT {search_options.limit}"
            
            logger.info(f"Vector similarity search in {schema}.{table_name}")
            logger.debug(f"SQL: {sql}")
            logger.debug(f"Search vector dimensions: {len(search_options.vector)}")
            
            result = await self.sql_driver.execute_query(sql, params)
            
            execution_time = (time.time() - start_time) * 1000
            
            data = [row.cells for row in result] if result else []
            column_names = list(data[0].keys()) if data else []
            
            return QueryResult(
                success=True,
                message=f"Vector similarity search completed, found {len(data)} results",
                data=data,
                columns=column_names,
                total_count=len(data),
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error in vector similarity search: {e}")
            
            return QueryResult(
                success=False,
                message=f"Vector similarity search failed: {str(e)}",
                data=[],
                execution_time_ms=execution_time
            )

    def _get_distance_expression(self, vector_column: str, search_vector: List[float], distance_function: str) -> str:
        """生成距离计算表达式"""
        vector_str = "[" + ",".join(map(str, search_vector)) + "]"
        
        if distance_function == "cosine":
            return f'"{vector_column}" <=> \'{vector_str}\'::vector'
        elif distance_function == "euclidean":
            return f'"{vector_column}" <-> \'{vector_str}\'::vector'
        elif distance_function == "inner_product":
            return f'"{vector_column}" <#> \'{vector_str}\'::vector'
        else:
            raise ValueError(f"Unsupported distance function: {distance_function}")
    
    async def vector_knn_search(self, table_name: str, schema: str = "public",
                               vector_column: str = "embedding",
                               search_vector: List[float],
                               k: int = 10,
                               distance_function: str = "cosine",
                               additional_columns: Optional[List[str]] = None,
                               include_distance: bool = True) -> QueryResult:
        """K最近邻向量搜索（专用于精确的K-NN搜索）"""
        start_time = time.time()
        
        try:
            # 构建SELECT子句
            select_columns = ["id"]
            if additional_columns:
                select_columns.extend(additional_columns)
            
            if include_distance:
                distance_expr = self._get_distance_expression(
                    vector_column, 
                    search_vector, 
                    distance_function
                )
                select_columns.append(f"{distance_expr} AS distance")
            
            select_sql = ", ".join([f'"{col}"' if not col.endswith(' AS distance') else col for col in select_columns])
            
            # 构建查询SQL - 专注于KNN搜索
            distance_expr = self._get_distance_expression(
                vector_column, 
                search_vector, 
                distance_function
            )
            
            sql = f'''SELECT {select_sql} 
                     FROM "{schema}"."{table_name}" 
                     ORDER BY {distance_expr} 
                     LIMIT {k}'''
            
            logger.info(f"Vector KNN search (k={k}) in {schema}.{table_name}")
            logger.debug(f"SQL: {sql}")
            logger.debug(f"Search vector dimensions: {len(search_vector)}")
            
            result = await self.sql_driver.execute_query(sql)
            
            execution_time = (time.time() - start_time) * 1000
            
            data = [row.cells for row in result] if result else []
            column_names = list(data[0].keys()) if data else []
            
            return QueryResult(
                success=True,
                message=f"Vector KNN search completed, found {len(data)} nearest neighbors",
                data=data,
                columns=column_names,
                total_count=len(data),
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error in vector KNN search: {e}")
            
            return QueryResult(
                success=False,
                message=f"Vector KNN search failed: {str(e)}",
                data=[],
                execution_time_ms=execution_time
            )

    async def create_vector_index(self, table_name: str, schema: str = "public",
                                vector_column: str = "embedding",
                                index_name: Optional[str] = None,
                                index_type: str = "hnsw",
                                index_options: Optional[Dict[str, Any]] = None) -> OperationResult:
        """创建向量索引"""
        start_time = time.time()
        
        try:
            if index_name is None:
                index_name = f"idx_{table_name}_{vector_column}_{index_type}"
            
            # 默认索引选项
            default_options = {
                "m": 16,          # HNSW参数：每个节点的最大连接数
                "ef_construction": 64  # HNSW参数：构建时的搜索范围
            }
            
            if index_options:
                default_options.update(index_options)
            
            # 构建索引创建SQL
            sql_parts = [
                "CREATE INDEX",
                f'"{index_name}"',
                "ON",
                f'"{schema}"."{table_name}"',
                "USING",
                index_type,
                f'("{vector_column}")'
            ]
            
            # 添加索引选项
            if index_type.lower() == "hnsw" and default_options:
                with_options = []
                for key, value in default_options.items():
                    with_options.append(f"{key} = {value}")
                
                if with_options:
                    sql_parts.append("WITH")
                    sql_parts.append(f"({', '.join(with_options)})")
            
            sql = " ".join(sql_parts)
            
            logger.info(f"Creating vector index: {index_name} on {schema}.{table_name}.{vector_column}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Vector index {index_name} created successfully using {index_type}",
                operation_type=OperationType.CREATE,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error creating vector index: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to create vector index: {str(e)}",
                operation_type=OperationType.CREATE,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )

    async def insert_vector_data(self, table_name: str,
                                vectors: List[List[float]],
                                schema: str = "public",
                                vector_column: str = "embedding",
                                additional_data: Optional[List[Dict[str, Any]]] = None,
                                returning_columns: Optional[List[str]] = None) -> OperationResult:
        """批量插入向量数据"""
        start_time = time.time()
        
        try:
            if not vectors:
                raise ValueError("No vectors provided for insert")
            
            # 准备数据
            data_rows = []
            for i, vector in enumerate(vectors):
                row_data = {vector_column: vector}
                
                # 添加额外数据
                if additional_data and i < len(additional_data):
                    row_data.update(additional_data[i])
                
                data_rows.append(row_data)
            
            # 获取列名
            columns = list(data_rows[0].keys())
            
            # 构建SQL
            quoted_columns = ", ".join([f'"{col}"' for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f'INSERT INTO "{schema}"."{table_name}" ({quoted_columns}) VALUES ({placeholders})'
            
            # 添加RETURNING子句
            if returning_columns:
                quoted_returning = ", ".join([f'"{col}"' for col in returning_columns])
                sql += f" RETURNING {quoted_returning}"
            
            logger.info(f"Inserting {len(data_rows)} vector record(s) into {schema}.{table_name}")
            logger.debug(f"SQL: {sql}")
            
            returned_data = []
            affected_rows = 0
            
            # 执行插入操作
            for row in data_rows:
                params = []
                for col in columns:
                    value = row.get(col)
                    # 向量数据需要特殊处理
                    if col == vector_column and isinstance(value, list):
                        value = "[" + ",".join(map(str, value)) + "]"
                    # JSONB数据处理
                    elif isinstance(value, (dict, list)) and col != vector_column:
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
                message=f"Successfully inserted {affected_rows} vector record(s) into {schema}.{table_name}",
                operation_type=OperationType.INSERT,
                table_name=f"{schema}.{table_name}",
                affected_rows=affected_rows,
                execution_time_ms=execution_time,
                returned_data=returned_data
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error inserting vector data: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to insert vector data: {str(e)}",
                operation_type=OperationType.INSERT,
                table_name=f"{schema}.{table_name}",
                execution_time_ms=execution_time
            )
    
    async def get_vector_index_stats(self, table_name: str, schema: str = "public",
                                   vector_column: str = "embedding") -> QueryResult:
        """获取向量索引统计信息"""
        start_time = time.time()
        
        try:
            # 查询向量索引信息
            sql = """
            SELECT 
                schemaname,
                tablename,
                indexname,
                indexdef,
                pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
                (SELECT amname FROM pg_am WHERE oid = (
                    SELECT relam FROM pg_class WHERE relname = indexname
                )) AS index_method,
                (SELECT n_tup_ins FROM pg_stat_user_tables WHERE 
                 schemaname = %s AND tablename = %s) AS table_inserts,
                (SELECT n_tup_upd FROM pg_stat_user_tables WHERE 
                 schemaname = %s AND tablename = %s) AS table_updates,
                (SELECT n_tup_del FROM pg_stat_user_tables WHERE 
                 schemaname = %s AND tablename = %s) AS table_deletes
            FROM pg_indexes 
            WHERE schemaname = %s 
            AND tablename = %s 
            AND indexdef LIKE %s
            """
            
            column_pattern = f'%{vector_column}%'
            params = [schema, table_name, schema, table_name, schema, table_name, 
                     schema, table_name, column_pattern]
            
            result = await self.sql_driver.execute_query(sql, params)
            
            execution_time = (time.time() - start_time) * 1000
            
            data = [row.cells for row in result] if result else []
            column_names = list(data[0].keys()) if data else []
            
            logger.info(f"Retrieved vector index stats for {schema}.{table_name}.{vector_column}")
            
            return QueryResult(
                success=True,
                message=f"Vector index statistics retrieved for {schema}.{table_name}",
                data=data,
                columns=column_names,
                total_count=len(data),
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error getting vector index stats: {e}")
            
            return QueryResult(
                success=False,
                message=f"Failed to get vector index stats: {str(e)}",
                data=[],
                execution_time_ms=execution_time
            )
    
    async def optimize_vector_index(self, index_name: str, schema: str = "public") -> OperationResult:
        """优化向量索引（重建索引以提高性能）"""
        start_time = time.time()
        
        try:
            # 重建索引以优化性能
            sql = f'REINDEX INDEX "{schema}"."{index_name}"'
            
            logger.info(f"Optimizing vector index: {schema}.{index_name}")
            logger.debug(f"SQL: {sql}")
            
            await self.sql_driver.execute_query(sql)
            
            execution_time = (time.time() - start_time) * 1000
            
            return OperationResult(
                success=True,
                message=f"Vector index {schema}.{index_name} optimized successfully",
                operation_type=OperationType.UPDATE,
                table_name=f"{schema}.{index_name}",
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Error optimizing vector index: {e}")
            
            return OperationResult(
                success=False,
                message=f"Failed to optimize vector index: {str(e)}",
                operation_type=OperationType.UPDATE,
                table_name=f"{schema}.{index_name}",
                execution_time_ms=execution_time
            )