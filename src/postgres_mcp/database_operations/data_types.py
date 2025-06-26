"""
数据库操作的数据类型定义
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
from enum import Enum


class OperationType(str, Enum):
    """操作类型枚举"""
    SELECT = "select"
    INSERT = "insert"  
    UPDATE = "update"
    DELETE = "delete"
    CREATE = "create"
    DROP = "drop"
    ALTER = "alter"


class DataType(str, Enum):
    """支持的数据类型"""
    INTEGER = "integer"
    BIGINT = "bigint"
    SERIAL = "serial"
    BIGSERIAL = "bigserial"
    TEXT = "text"
    VARCHAR = "varchar"
    CHAR = "char"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    DATE = "date"
    TIME = "time"
    JSON = "json"
    JSONB = "jsonb"
    UUID = "uuid"
    VECTOR = "vector"  # pgvector 支持
    DECIMAL = "decimal"
    NUMERIC = "numeric"
    REAL = "real"
    DOUBLE_PRECISION = "double precision"


@dataclass
class ColumnDefinition:
    """列定义"""
    name: str
    data_type: DataType
    nullable: bool = True
    default: Optional[Any] = None
    primary_key: bool = False
    unique: bool = False
    check_constraint: Optional[str] = None
    # 向量特定属性
    vector_dimensions: Optional[int] = None


@dataclass  
class TableDefinition:
    """表定义"""
    name: str
    columns: List[ColumnDefinition]
    schema: str = "public"
    constraints: List[str] = None
    indexes: List[Dict[str, Any]] = None

    def __post_init__(self):
        if self.constraints is None:
            self.constraints = []
        if self.indexes is None:
            self.indexes = []


@dataclass
class QueryCondition:
    """查询条件"""
    column: str
    operator: str  # =, !=, >, <, >=, <=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL
    value: Any
    logical_operator: str = "AND"  # AND, OR


@dataclass
class QueryOptions:
    """查询选项"""
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: Optional[List[Dict[str, str]]] = None  # [{"column": "name", "direction": "ASC"}]
    group_by: Optional[List[str]] = None
    having: Optional[List[QueryCondition]] = None

    def __post_init__(self):
        if self.order_by is None:
            self.order_by = []
        if self.group_by is None:
            self.group_by = []
        if self.having is None:
            self.having = []


@dataclass
class DatabaseResult:
    """数据库操作结果基类"""
    success: bool
    message: str
    execution_time_ms: Optional[float] = None
    affected_rows: Optional[int] = None


@dataclass
class QueryResult:
    """查询结果"""
    success: bool
    message: str
    data: List[Dict[str, Any]]
    execution_time_ms: Optional[float] = None
    affected_rows: Optional[int] = None
    total_count: Optional[int] = None
    columns: Optional[List[str]] = None

    def __post_init__(self):
        if self.columns is None:
            self.columns = []


@dataclass  
class OperationResult:
    """操作结果（增删改）"""
    success: bool
    message: str
    operation_type: OperationType
    table_name: str
    execution_time_ms: Optional[float] = None
    affected_rows: Optional[int] = None
    returned_data: Optional[List[Dict[str, Any]]] = None

    def __post_init__(self):
        if self.returned_data is None:
            self.returned_data = []


@dataclass
class VectorSearchOptions:
    """向量搜索选项"""
    vector: List[float]
    distance_function: str = "cosine"  # cosine, euclidean, inner_product
    limit: int = 10
    threshold: Optional[float] = None
    include_distance: bool = True


@dataclass
class IndexDefinition:
    """索引定义"""
    name: str
    table_name: str
    columns: List[str]
    index_type: str = "btree"  # btree, hash, gin, gist, brin, spgist
    unique: bool = False
    partial_condition: Optional[str] = None
    # 向量索引特定选项
    vector_index_options: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.vector_index_options is None:
            self.vector_index_options = {} 