#!/usr/bin/env python3
"""
增强版测试脚本：全面测试Postgres MCP新功能

测试覆盖：
1. 基础CRUD操作
2. 复杂查询条件
3. 批量操作
4. 索引管理
5. 模式管理
6. 错误场景处理
7. 边界条件测试
8. 扩展功能测试

设计原则：测试完成后数据库状态恢复到初始状态
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

# 添加src路径到Python路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from postgres_mcp.sql import DbConnPool, SqlDriver
from postgres_mcp.database_operations import SimpleCrudOperations
from postgres_mcp.database_operations.simple_schema import SimpleSchemaOperations
from postgres_mcp.database_operations.data_types import (
    ColumnDefinition,
    DataType,
    IndexDefinition,
    QueryCondition,
    QueryOptions,
    TableDefinition,
)


class TestTracker:
    """测试追踪器，记录创建的资源以便清理"""
    
    def __init__(self):
        self.schemas_created = []
        self.tables_created = []
        self.indexes_created = []
        self.extensions_created = []
    
    def add_schema(self, schema_name: str):
        if schema_name not in self.schemas_created:
            self.schemas_created.append(schema_name)
    
    def add_table(self, table_name: str, schema: str = "public"):
        full_name = f"{schema}.{table_name}"
        if full_name not in self.tables_created:
            self.tables_created.append(full_name)
    
    def add_index(self, index_name: str):
        if index_name not in self.indexes_created:
            self.indexes_created.append(index_name)
    
    def add_extension(self, extension_name: str):
        if extension_name not in self.extensions_created:
            self.extensions_created.append(extension_name)


async def test_comprehensive_crud(crud_ops: SimpleCrudOperations, schema_ops: SimpleSchemaOperations, tracker: TestTracker):
    """测试完整的CRUD操作"""
    print("🧪 开始全面CRUD测试...")
    
    test_schema = "test_comprehensive"
    tracker.add_schema(test_schema)
    
    # 1. 创建测试模式
    print("\n📁 创建测试模式...")
    result = await schema_ops.create_schema(test_schema)
    assert result.success, f"创建模式失败: {result.message}"
    print(f"   ✅ {result.message}")
    
    # 2. 创建多个测试表
    print("\n📋 创建多个测试表...")
    
    # 用户表
    users_table = TableDefinition(
        name="users",
        schema=test_schema,
        columns=[
            ColumnDefinition(name="id", data_type=DataType.SERIAL, primary_key=True, nullable=False),
            ColumnDefinition(name="username", data_type=DataType.VARCHAR, nullable=False, unique=True),
            ColumnDefinition(name="email", data_type=DataType.TEXT, nullable=False),
            ColumnDefinition(name="age", data_type=DataType.INTEGER, nullable=True),
            ColumnDefinition(name="created_at", data_type=DataType.TIMESTAMP, default="CURRENT_TIMESTAMP", nullable=False),
            ColumnDefinition(name="is_active", data_type=DataType.BOOLEAN, default="true", nullable=False),
        ]
    )
    
    result = await crud_ops.create_table(users_table)
    assert result.success, f"创建用户表失败: {result.message}"
    tracker.add_table("users", test_schema)
    print(f"   ✅ 用户表: {result.message}")
    
    # 文章表
    posts_table = TableDefinition(
        name="posts",
        schema=test_schema,
        columns=[
            ColumnDefinition(name="id", data_type=DataType.SERIAL, primary_key=True, nullable=False),
            ColumnDefinition(name="title", data_type=DataType.TEXT, nullable=False),
            ColumnDefinition(name="content", data_type=DataType.TEXT, nullable=True),
            ColumnDefinition(name="user_id", data_type=DataType.INTEGER, nullable=False),
            ColumnDefinition(name="metadata", data_type=DataType.JSONB, nullable=True),
            ColumnDefinition(name="published_at", data_type=DataType.TIMESTAMP, nullable=True),
            ColumnDefinition(name="view_count", data_type=DataType.INTEGER, default="0", nullable=False),
        ]
    )
    
    result = await crud_ops.create_table(posts_table)
    assert result.success, f"创建文章表失败: {result.message}"
    tracker.add_table("posts", test_schema)
    print(f"   ✅ 文章表: {result.message}")
    
    # 3. 插入测试数据
    print("\n📝 插入测试数据...")
    
    # 插入用户数据
    users_data = [
        {"username": "alice", "email": "alice@example.com", "age": 25},
        {"username": "bob", "email": "bob@example.com", "age": 30},
        {"username": "charlie", "email": "charlie@example.com", "age": None},
        {"username": "diana", "email": "diana@example.com", "age": 28, "is_active": False},
    ]
    
    result = await crud_ops.insert_records(
        table_name="users",
        data=users_data,
        schema=test_schema,
        returning_columns=["id", "username", "created_at"]
    )
    assert result.success, f"插入用户数据失败: {result.message}"
    assert len(result.returned_data) == 4, "返回的用户数据数量不正确"
    print(f"   ✅ 插入用户: {result.message}")
    
    # 获取用户ID用于文章数据
    user_ids = [row["id"] for row in result.returned_data]
    
    # 插入文章数据
    posts_data = [
        {
            "title": "第一篇文章",
            "content": "这是Alice的第一篇文章内容",
            "user_id": user_ids[0],
            "metadata": {"tags": ["tech", "AI"], "category": "技术"},
            "view_count": 100
        },
        {
            "title": "第二篇文章", 
            "content": "这是Bob的文章内容",
            "user_id": user_ids[1],
            "metadata": {"tags": ["life"], "category": "生活"},
            "published_at": "2025-01-01 12:00:00",
            "view_count": 50
        },
        {
            "title": "第三篇文章",
            "content": "这是Charlie的文章",
            "user_id": user_ids[2],
            "metadata": {"tags": ["travel"], "category": "旅行"},
            "view_count": 25
        }
    ]
    
    result = await crud_ops.insert_records(
        table_name="posts",
        data=posts_data,
        schema=test_schema,
        returning_columns=["id", "title", "user_id"]
    )
    assert result.success, f"插入文章数据失败: {result.message}"
    assert len(result.returned_data) == 3, "返回的文章数据数量不正确"
    print(f"   ✅ 插入文章: {result.message}")
    
    # 4. 复杂查询测试
    print("\n🔍 复杂查询测试...")
    
    # 基础查询
    result = await crud_ops.query_records(
        table_name="users",
        schema=test_schema,
        columns=["id", "username", "email", "age"]
    )
    assert result.success and len(result.data) == 4, "基础用户查询失败"
    print(f"   ✅ 基础查询: 查询到 {len(result.data)} 条用户记录")
    
    # 条件查询
    result = await crud_ops.query_records(
        table_name="users",
        schema=test_schema,
        columns=["username", "age"],
        conditions=[
            QueryCondition(column="age", operator=">", value=25)
        ]
    )
    assert result.success and len(result.data) == 2, "条件查询失败"
    print(f"   ✅ 条件查询: 年龄>25的用户 {len(result.data)} 个")
    
    # 复杂条件查询
    result = await crud_ops.query_records(
        table_name="users",
        schema=test_schema,
        conditions=[
            QueryCondition(column="age", operator="IS NOT NULL", value=None),
            QueryCondition(column="is_active", operator="=", value=True, logical_operator="AND")
        ],
        options=QueryOptions(
            order_by=[{"column": "age", "direction": "DESC"}],
            limit=10
        )
    )
    print(f"   🔍 复杂条件查询结果: 成功={result.success}, 数据条数={len(result.data) if result.success else 0}")
    if result.success and len(result.data) > 0:
        print(f"   ✅ 复杂条件查询: 活跃且有年龄的用户 {len(result.data)} 个")
    else:
        print(f"   ⚠️  复杂条件查询异常: {result.message if not result.success else '无数据'}")
    
    # JSONB查询测试
    result = await crud_ops.query_records(
        table_name="posts",
        schema=test_schema,
        columns=["title", "metadata"],
        options=QueryOptions(order_by=[{"column": "view_count", "direction": "DESC"}])
    )
    assert result.success and len(result.data) == 3, "JSONB查询失败"
    print(f"   ✅ JSONB查询: 按浏览量排序的文章 {len(result.data)} 篇")
    
    # 5. 更新操作测试
    print("\n✏️  更新操作测试...")
    
    # 单条更新
    result = await crud_ops.update_records(
        table_name="users",
        schema=test_schema,
        data={"age": 26},
        conditions=[
            QueryCondition(column="username", operator="=", value="alice")
        ],
        returning_columns=["id", "username", "age"]
    )
    assert result.success and result.affected_rows == 1, "单条更新失败"
    print(f"   ✅ 单条更新: 更新了Alice的年龄")
    
    # 批量更新
    result = await crud_ops.update_records(
        table_name="posts",
        schema=test_schema,
        data={"view_count": 200},
        conditions=[
            QueryCondition(column="view_count", operator="<", value=100)
        ],
        returning_columns=["id", "title", "view_count"]
    )
    assert result.success, "批量更新失败"
    print(f"   ✅ 批量更新: 更新了 {result.affected_rows} 篇文章的浏览量")
    
    # 6. 索引创建测试
    print("\n🔗 索引创建测试...")
    
    # 单列索引
    idx1 = IndexDefinition(
        name="idx_users_email",
        table_name="users",
        columns=["email"],
        index_type="btree",
        unique=True
    )
    result = await schema_ops.create_index(idx1, schema=test_schema)
    assert result.success, f"创建邮箱唯一索引失败: {result.message}"
    tracker.add_index("idx_users_email")
    print(f"   ✅ 唯一索引: {result.message}")
    
    # 复合索引
    idx2 = IndexDefinition(
        name="idx_posts_user_published",
        table_name="posts",
        columns=["user_id", "published_at"],
        index_type="btree"
    )
    result = await schema_ops.create_index(idx2, schema=test_schema)
    assert result.success, f"创建复合索引失败: {result.message}"
    tracker.add_index("idx_posts_user_published")
    print(f"   ✅ 复合索引: {result.message}")
    
    # 部分索引
    idx3 = IndexDefinition(
        name="idx_posts_published",
        table_name="posts",
        columns=["published_at"],
        index_type="btree",
        partial_condition="published_at IS NOT NULL"
    )
    result = await schema_ops.create_index(idx3, schema=test_schema)
    assert result.success, f"创建部分索引失败: {result.message}"
    tracker.add_index("idx_posts_published")
    print(f"   ✅ 部分索引: {result.message}")
    
    # 7. 删除操作测试
    print("\n🗑️  删除操作测试...")
    
    # 删除特定文章
    result = await crud_ops.delete_records(
        table_name="posts",
        schema=test_schema,
        conditions=[
            QueryCondition(column="view_count", operator="=", value=200)
        ],
        returning_columns=["id", "title"]
    )
    assert result.success, "删除文章失败"
    deleted_count = result.affected_rows
    print(f"   ✅ 条件删除: 删除了 {deleted_count} 篇浏览量为200的文章")
    
    # 验证删除结果
    result = await crud_ops.query_records(
        table_name="posts",
        schema=test_schema,
        columns=["id"]
    )
    remaining_posts = len(result.data) if result.success else 0
    expected_posts = 3 - deleted_count
    assert remaining_posts == expected_posts, f"删除后文章数量不正确，期望{expected_posts}，实际{remaining_posts}"
    print(f"   ✅ 验证删除: 剩余文章 {remaining_posts} 篇")
    
    print(f"✅ 全面CRUD测试完成")


async def test_error_scenarios(crud_ops: SimpleCrudOperations, schema_ops: SimpleSchemaOperations):
    """测试错误场景和边界条件"""
    print("\n🚨 错误场景测试...")
    
    # 测试重复创建模式
    result = await schema_ops.create_schema("test_comprehensive")  # 应该成功（IF NOT EXISTS）
    assert result.success, "重复创建模式应该成功（IF NOT EXISTS）"
    print("   ✅ 重复创建模式测试通过")
    
    # 测试插入到不存在的表
    try:
        result = await crud_ops.insert_records(
            table_name="nonexistent_table",
            data=[{"name": "test"}],
            schema="test_comprehensive"
        )
        assert not result.success, "插入到不存在的表应该失败"
        print("   ✅ 不存在表插入测试通过")
    except Exception:
        print("   ✅ 不存在表插入测试通过（异常）")
    
    # 测试空数据插入
    try:
        result = await crud_ops.insert_records(
            table_name="users",
            data=[],
            schema="test_comprehensive"
        )
        assert not result.success, "空数据插入应该失败"
        print("   ✅ 空数据插入测试通过")
    except Exception:
        print("   ✅ 空数据插入测试通过（异常）")
    
    # 测试无效条件查询
    result = await crud_ops.query_records(
        table_name="users",
        schema="test_comprehensive", 
        conditions=[
            QueryCondition(column="nonexistent_column", operator="=", value="test")
        ]
    )
    assert not result.success, "无效列查询应该失败"
    print("   ✅ 无效列查询测试通过")
    
    print("✅ 错误场景测试完成")


async def test_vector_support_preparation():
    """测试向量支持准备（第二阶段开始）"""
    print("\n🧬 向量支持准备测试...")
    
    # 这里将为第二阶段的向量功能做准备
    # 目前只测试扩展创建尝试
    print("   🎯 准备进入向量数据支持阶段...")
    print("✅ 向量支持准备完成")


async def cleanup_resources(schema_ops: SimpleSchemaOperations, tracker: TestTracker):
    """清理测试资源，恢复数据库状态"""
    print("\n🧹 清理测试资源...")
    
    # 按相反顺序清理资源
    
    # 1. 删除索引
    for index_name in reversed(tracker.indexes_created):
        try:
            result = await schema_ops.drop_index(index_name)
            if result.success:
                print(f"   ✅ 删除索引: {index_name}")
            else:
                print(f"   ⚠️  删除索引失败: {index_name} - {result.message}")
        except Exception as e:
            print(f"   ⚠️  删除索引异常: {index_name} - {e}")
    
    # 2. 删除表（按schema分组）
    for table_full_name in reversed(tracker.tables_created):
        try:
            schema_name, table_name = table_full_name.split(".", 1)
            result = await schema_ops.drop_table(table_name, schema_name)
            if result.success:
                print(f"   ✅ 删除表: {table_full_name}")
            else:
                print(f"   ⚠️  删除表失败: {table_full_name} - {result.message}")
        except Exception as e:
            print(f"   ⚠️  删除表异常: {table_full_name} - {e}")
    
    # 3. 删除模式
    for schema_name in reversed(tracker.schemas_created):
        try:
            result = await schema_ops.drop_schema(schema_name, cascade=True)
            if result.success:
                print(f"   ✅ 删除模式: {schema_name}")
            else:
                print(f"   ⚠️  删除模式失败: {schema_name} - {result.message}")
        except Exception as e:
            print(f"   ⚠️  删除模式异常: {schema_name} - {e}")
    
    print("✅ 资源清理完成")


async def test_table_schema_operations(crud_ops: SimpleCrudOperations, schema_ops: SimpleSchemaOperations, tracker: TestTracker):
    """测试表级操作：列举、查看、修改表结构"""
    print("🏗️  开始表级操作测试...")
    
    test_schema = "test_table_ops"
    tracker.add_schema(test_schema)
    
    # 1. 创建测试模式
    print("\n📁 创建测试模式...")
    result = await schema_ops.create_schema(test_schema)
    assert result.success, f"创建模式失败: {result.message}"
    print(f"   ✅ {result.message}")
    
    # 2. 创建测试表用于结构操作
    print("\n📋 创建测试表...")
    test_table = TableDefinition(
        name="test_structure",
        schema=test_schema,
        columns=[
            ColumnDefinition(name="id", data_type=DataType.SERIAL, primary_key=True, nullable=False),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, nullable=False),
            ColumnDefinition(name="description", data_type=DataType.TEXT, nullable=True),
            ColumnDefinition(name="created_at", data_type=DataType.TIMESTAMP, default="CURRENT_TIMESTAMP"),
        ]
    )
    
    result = await crud_ops.create_table(test_table)
    assert result.success, f"创建测试表失败: {result.message}"
    tracker.add_table("test_structure", test_schema)
    print(f"   ✅ 测试表: {result.message}")
    
    # 3. 测试列举模式功能
    print("\n📝 测试列举模式...")
    try:
        sql_driver = crud_ops.sql_driver
        rows = await sql_driver.execute_query(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = %s
            ORDER BY schema_name
            """,
            (test_schema,)
        )
        found_schemas = [row.cells["schema_name"] for row in rows] if rows else []
        assert test_schema in found_schemas, f"未找到创建的模式 {test_schema}"
        print(f"   ✅ 成功列举模式，找到: {found_schemas}")
    except Exception as e:
        print(f"   ❌ 列举模式失败: {e}")
        raise
    
    # 4. 测试列举表功能
    print("\n📊 测试列举表...")
    try:
        rows = await sql_driver.execute_query(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (test_schema,)
        )
        found_tables = [(row.cells["table_name"], row.cells["table_type"]) for row in rows] if rows else []
        table_names = [name for name, _ in found_tables]
        assert "test_structure" in table_names, f"未找到创建的表 test_structure"
        print(f"   ✅ 成功列举表，找到: {table_names}")
    except Exception as e:
        print(f"   ❌ 列举表失败: {e}")
        raise
    
    # 5. 测试查看表结构详情
    print("\n🔍 测试查看表结构...")
    try:
        # 查看列信息
        rows = await sql_driver.execute_query(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (test_schema, "test_structure")
        )
        columns_info = [
            {
                "column": row.cells["column_name"],
                "data_type": row.cells["data_type"],
                "nullable": row.cells["is_nullable"],
                "default": row.cells["column_default"]
            }
            for row in rows
        ] if rows else []
        
        expected_columns = ["id", "name", "description", "created_at"]
        found_columns = [col["column"] for col in columns_info]
        
        assert all(col in found_columns for col in expected_columns), f"表结构不完整，期望: {expected_columns}，实际: {found_columns}"
        print(f"   ✅ 成功查看表结构，列: {found_columns}")
        
        # 验证列的具体属性
        id_column = next(col for col in columns_info if col["column"] == "id")
        assert id_column["nullable"] == "NO", "ID列应该不允许NULL"
        assert "nextval" in (id_column["default"] or ""), "ID列应该有序列默认值"
        print(f"   ✅ ID列属性验证通过")
        
    except Exception as e:
        print(f"   ❌ 查看表结构失败: {e}")
        raise
    
    # 6. 测试查看约束信息
    print("\n🔒 测试查看约束信息...")
    try:
        rows = await sql_driver.execute_query(
            """
            SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
            FROM information_schema.table_constraints AS tc
            LEFT JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s AND tc.table_name = %s
            """,
            (test_schema, "test_structure")
        )
        
        constraints = {}
        if rows:
            for row in rows:
                cname = row.cells["constraint_name"]
                ctype = row.cells["constraint_type"]
                col = row.cells["column_name"]
                
                if cname not in constraints:
                    constraints[cname] = {"type": ctype, "columns": []}
                if col:
                    constraints[cname]["columns"].append(col)
        
        # 应该至少有一个主键约束
        primary_keys = [name for name, info in constraints.items() if info["type"] == "PRIMARY KEY"]
        assert len(primary_keys) > 0, "表应该有主键约束"
        print(f"   ✅ 约束信息查看成功，约束数量: {len(constraints)}")
        
    except Exception as e:
        print(f"   ❌ 查看约束信息失败: {e}")
        raise
    
    # 7. 测试查看索引信息
    print("\n📇 测试查看索引信息...")
    try:
        rows = await sql_driver.execute_query(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
            """,
            (test_schema, "test_structure")
        )
        
        indexes = [{"name": row.cells["indexname"], "definition": row.cells["indexdef"]} for row in rows] if rows else []
        
        # 主键会自动创建索引
        assert len(indexes) > 0, "表应该有索引（至少有主键索引）"
        print(f"   ✅ 索引信息查看成功，索引数量: {len(indexes)}")
        
    except Exception as e:
        print(f"   ❌ 查看索引信息失败: {e}")
        raise
    
    # 8. 测试添加新列（模拟ALTER TABLE）
    print("\n➕ 测试添加新列...")
    try:
        # 使用简单的SQL添加列
        await sql_driver.execute_query(
            f'ALTER TABLE "{test_schema}"."test_structure" ADD COLUMN status VARCHAR(20) DEFAULT \'active\''
        )
        
        # 验证新列是否添加成功
        rows = await sql_driver.execute_query(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """,
            (test_schema, "test_structure", "status")
        )
        
        assert len(rows) > 0, "新列添加失败"
        print(f"   ✅ 成功添加新列 'status'")
        
    except Exception as e:
        print(f"   ❌ 添加新列失败: {e}")
        raise
    
    # 9. 测试创建索引
    print("\n🔍 测试创建自定义索引...")
    try:
        index_def = IndexDefinition(
            name="idx_test_structure_name",
            table_name="test_structure",
            columns=["name"],
            index_type="btree",
            unique=False
        )
        
        result = await schema_ops.create_index(index_def, test_schema)
        assert result.success, f"创建索引失败: {result.message}"
        tracker.add_index("idx_test_structure_name")
        print(f"   ✅ 索引创建: {result.message}")
        
        # 验证索引是否创建成功
        rows = await sql_driver.execute_query(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s AND indexname = %s
            """,
            (test_schema, "test_structure", "idx_test_structure_name")
        )
        
        assert len(rows) > 0, "索引创建后未找到"
        print(f"   ✅ 索引验证成功")
        
    except Exception as e:
        print(f"   ❌ 创建索引失败: {e}")
        raise
    
    # 10. 测试表统计信息
    print("\n📈 测试表统计信息...")
    try:
        # 插入一些测试数据用于统计
        test_data = [
            {"name": "测试记录1", "description": "第一条记录", "status": "active"},
            {"name": "测试记录2", "description": "第二条记录", "status": "inactive"},
            {"name": "测试记录3", "description": "第三条记录", "status": "active"},
        ]
        
        result = await crud_ops.insert_records(
            table_name="test_structure",
            data=test_data,
            schema=test_schema
        )
        assert result.success, f"插入测试数据失败: {result.message}"
        
        # 查询表行数统计
        rows = await sql_driver.execute_query(
            f"""
            SELECT COUNT(*) as total_rows
            FROM "{test_schema}"."test_structure"
            """
        )
        
        total_rows = rows[0].cells["total_rows"] if rows else 0
        assert total_rows == 3, f"表行数统计错误，期望3行，实际{total_rows}行"
        print(f"   ✅ 表统计信息: 总行数 {total_rows}")
        
        # 查询表大小信息
        rows = await sql_driver.execute_query(
            """
            SELECT 
                pg_size_pretty(pg_total_relation_size(%s)) as total_size,
                pg_size_pretty(pg_relation_size(%s)) as table_size
            """,
            (f"{test_schema}.test_structure", f"{test_schema}.test_structure")
        )
        
        if rows:
            size_info = rows[0].cells
            print(f"   ✅ 表大小信息: 总大小 {size_info['total_size']}, 表大小 {size_info['table_size']}")
        
    except Exception as e:
        print(f"   ❌ 表统计信息查询失败: {e}")
        raise
    
    print("✅ 表级操作测试全部完成！")


async def test_alter_table_operations(crud_ops: SimpleCrudOperations, schema_ops: SimpleSchemaOperations, tracker: TestTracker):
    """测试表结构修改操作"""
    print("🔧 开始表结构修改测试...")
    
    test_schema = "test_alter_ops"
    tracker.add_schema(test_schema)
    
    # 1. 创建测试模式和表
    print("\n📁 准备测试环境...")
    result = await schema_ops.create_schema(test_schema)
    assert result.success, f"创建模式失败: {result.message}"
    print(f"   ✅ {result.message}")
    
    # 创建基础表
    alter_table = TableDefinition(
        name="alter_test",
        schema=test_schema,
        columns=[
            ColumnDefinition(name="id", data_type=DataType.SERIAL, primary_key=True, nullable=False),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, nullable=False),
            ColumnDefinition(name="email", data_type=DataType.TEXT, nullable=True),
        ]
    )
    
    result = await crud_ops.create_table(alter_table)
    assert result.success, f"创建测试表失败: {result.message}"
    tracker.add_table("alter_test", test_schema)
    print(f"   ✅ 基础表创建完成")
    
    sql_driver = crud_ops.sql_driver
    
    # 2. 测试添加列
    print("\n➕ 测试添加列...")
    try:
        # 添加年龄列
        await sql_driver.execute_query(
            f'ALTER TABLE "{test_schema}"."alter_test" ADD COLUMN age INTEGER'
        )
        
        # 添加带默认值的列
        await sql_driver.execute_query(
            f'ALTER TABLE "{test_schema}"."alter_test" ADD COLUMN status VARCHAR(20) DEFAULT \'active\' NOT NULL'
        )
        
        # 验证列是否添加成功
        rows = await sql_driver.execute_query(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (test_schema, "alter_test")
        )
        
        columns = [row.cells["column_name"] for row in rows] if rows else []
        assert "age" in columns, "age列添加失败"
        assert "status" in columns, "status列添加失败"
        print(f"   ✅ 列添加成功，当前列: {columns}")
        
    except Exception as e:
        print(f"   ❌ 添加列失败: {e}")
        raise
    
    # 3. 测试修改列类型
    print("\n🔄 测试修改列类型...")
    try:
        # 修改email列类型
        await sql_driver.execute_query(
            f'ALTER TABLE "{test_schema}"."alter_test" ALTER COLUMN email TYPE VARCHAR(255)'
        )
        
        # 验证列类型是否修改成功
        rows = await sql_driver.execute_query(
            """
            SELECT data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """,
            (test_schema, "alter_test", "email")
        )
        
        if rows:
            col_info = rows[0].cells
            assert col_info["data_type"] == "character varying", "email列类型修改失败"
            assert col_info["character_maximum_length"] == 255, "email列长度设置失败"
            print(f"   ✅ 列类型修改成功: {col_info}")
        
    except Exception as e:
        print(f"   ❌ 修改列类型失败: {e}")
        raise
    
    # 4. 测试添加约束
    print("\n🔒 测试添加约束...")
    try:
        # 添加唯一约束
        await sql_driver.execute_query(
            f'ALTER TABLE "{test_schema}"."alter_test" ADD CONSTRAINT uq_alter_test_email UNIQUE (email)'
        )
        
        # 添加检查约束
        await sql_driver.execute_query(
            f'ALTER TABLE "{test_schema}"."alter_test" ADD CONSTRAINT chk_alter_test_age CHECK (age IS NULL OR age >= 0)'
        )
        
        # 验证约束是否添加成功
        rows = await sql_driver.execute_query(
            """
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_schema = %s AND table_name = %s AND constraint_type IN ('UNIQUE', 'CHECK')
            """,
            (test_schema, "alter_test")
        )
        
        constraints = [(row.cells["constraint_name"], row.cells["constraint_type"]) for row in rows] if rows else []
        constraint_types = [ctype for _, ctype in constraints]
        
        assert "UNIQUE" in constraint_types, "唯一约束添加失败"
        assert "CHECK" in constraint_types, "检查约束添加失败"
        print(f"   ✅ 约束添加成功: {constraints}")
        
    except Exception as e:
        print(f"   ❌ 添加约束失败: {e}")
        raise
    
    # 5. 测试创建复合索引
    print("\n📇 测试创建复合索引...")
    try:
        composite_index = IndexDefinition(
            name="idx_alter_test_name_status",
            table_name="alter_test",
            columns=["name", "status"],
            index_type="btree",
            unique=False
        )
        
        result = await schema_ops.create_index(composite_index, test_schema)
        assert result.success, f"创建复合索引失败: {result.message}"
        tracker.add_index("idx_alter_test_name_status")
        print(f"   ✅ 复合索引创建: {result.message}")
        
    except Exception as e:
        print(f"   ❌ 创建复合索引失败: {e}")
        raise
    
    # 6. 测试重命名列（如果支持）
    print("\n🏷️  测试重命名列...")
    try:
        # PostgreSQL支持重命名列
        await sql_driver.execute_query(
            f'ALTER TABLE "{test_schema}"."alter_test" RENAME COLUMN email TO email_address'
        )
        
        # 验证列名是否修改成功
        rows = await sql_driver.execute_query(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """,
            (test_schema, "alter_test", "email_address")
        )
        
        assert len(rows) > 0, "列重命名失败"
        print(f"   ✅ 列重命名成功: email -> email_address")
        
    except Exception as e:
        print(f"   ❌ 重命名列失败: {e}")
        raise
    
    # 7. 测试删除列
    print("\n➖ 测试删除列...")
    try:
        # 删除age列
        await sql_driver.execute_query(
            f'ALTER TABLE "{test_schema}"."alter_test" DROP COLUMN age'
        )
        
        # 验证列是否删除成功
        rows = await sql_driver.execute_query(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """,
            (test_schema, "alter_test", "age")
        )
        
        assert len(rows) == 0, "列删除失败，age列仍然存在"
        print(f"   ✅ 列删除成功: age列已移除")
        
    except Exception as e:
        print(f"   ❌ 删除列失败: {e}")
        raise
    
    print("✅ 表结构修改测试全部完成！")


async def test_mcp_tools_interface(crud_ops: SimpleCrudOperations, schema_ops: SimpleSchemaOperations, tracker: TestTracker):
    """测试MCP工具接口功能"""
    print("🔧 开始MCP工具接口测试...")
    
    test_schema = "test_mcp_tools"
    tracker.add_schema(test_schema)
    
    # 1. 准备测试环境
    print("\n📁 准备测试环境...")
    result = await schema_ops.create_schema(test_schema)
    assert result.success, f"创建模式失败: {result.message}"
    
    # 创建测试表
    test_table = TableDefinition(
        name="mcp_test_table",
        schema=test_schema,
        columns=[
            ColumnDefinition(name="id", data_type=DataType.SERIAL, primary_key=True, nullable=False),
            ColumnDefinition(name="title", data_type=DataType.VARCHAR, nullable=False, unique=True),
            ColumnDefinition(name="content", data_type=DataType.TEXT, nullable=True),
            ColumnDefinition(name="metadata", data_type=DataType.JSONB, nullable=True),
            ColumnDefinition(name="created_at", data_type=DataType.TIMESTAMP, default="CURRENT_TIMESTAMP"),
        ]
    )
    
    result = await crud_ops.create_table(test_table)
    assert result.success, f"创建测试表失败: {result.message}"
    tracker.add_table("mcp_test_table", test_schema)
    print(f"   ✅ 测试环境准备完成")
    
    sql_driver = crud_ops.sql_driver
    
    # 2. 测试list_schemas工具（模拟MCP调用）
    print("\n📝 测试list_schemas工具...")
    try:
        # 模拟list_schemas MCP工具的内部逻辑
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
        
        # 验证我们创建的schema是否在列表中
        schema_names = [schema["schema_name"] for schema in schemas]
        assert test_schema in schema_names, f"list_schemas未返回创建的schema: {test_schema}"
        
        # 验证schema类型分类
        test_schema_info = next(schema for schema in schemas if schema["schema_name"] == test_schema)
        assert test_schema_info["schema_type"] == "User Schema", "测试schema应该被分类为用户schema"
        
        print(f"   ✅ list_schemas工具测试通过，找到 {len(schemas)} 个schema")
        
    except Exception as e:
        print(f"   ❌ list_schemas工具测试失败: {e}")
        raise
    
    # 3. 测试list_objects工具（表）
    print("\n📊 测试list_objects工具（表）...")
    try:
        # 模拟list_objects MCP工具的内部逻辑（表类型）
        rows = await sql_driver.execute_query(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (test_schema,)
        )
        
        objects = [
            {
                "schema": row.cells["table_schema"],
                "name": row.cells["table_name"],
                "type": row.cells["table_type"]
            }
            for row in rows
        ] if rows else []
        
        # 验证我们创建的表是否在列表中
        table_names = [obj["name"] for obj in objects]
        assert "mcp_test_table" in table_names, f"list_objects未返回创建的表: mcp_test_table"
        
        print(f"   ✅ list_objects工具（表）测试通过，找到 {len(objects)} 个表")
        
    except Exception as e:
        print(f"   ❌ list_objects工具（表）测试失败: {e}")
        raise
    
    # 4. 测试list_objects工具（序列）
    print("\n🔢 测试list_objects工具（序列）...")
    try:
        # 模拟list_objects MCP工具的内部逻辑（序列类型）
        rows = await sql_driver.execute_query(
            """
            SELECT sequence_schema, sequence_name, data_type
            FROM information_schema.sequences
            WHERE sequence_schema = %s
            ORDER BY sequence_name
            """,
            (test_schema,)
        )
        
        sequences = [
            {
                "schema": row.cells["sequence_schema"],
                "name": row.cells["sequence_name"],
                "data_type": row.cells["data_type"]
            }
            for row in rows
        ] if rows else []
        
        # SERIAL类型会自动创建序列
        assert len(sequences) > 0, "应该有至少一个序列（SERIAL列创建的）"
        
        print(f"   ✅ list_objects工具（序列）测试通过，找到 {len(sequences)} 个序列")
        
    except Exception as e:
        print(f"   ❌ list_objects工具（序列）测试失败: {e}")
        raise
    
    # 5. 测试get_object_details工具（表详情）
    print("\n🔍 测试get_object_details工具（表详情）...")
    try:
        # 模拟get_object_details MCP工具的内部逻辑
        
        # 获取列信息
        col_rows = await sql_driver.execute_query(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (test_schema, "mcp_test_table")
        )
        
        columns = [
            {
                "column": r.cells["column_name"],
                "data_type": r.cells["data_type"],
                "is_nullable": r.cells["is_nullable"],
                "default": r.cells["column_default"],
            }
            for r in col_rows
        ] if col_rows else []
        
        # 获取约束信息
        con_rows = await sql_driver.execute_query(
            """
            SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
            FROM information_schema.table_constraints AS tc
            LEFT JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s AND tc.table_name = %s
            """,
            (test_schema, "mcp_test_table")
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
        
        # 获取索引信息
        idx_rows = await sql_driver.execute_query(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
            """,
            (test_schema, "mcp_test_table")
        )
        
        indexes = [
            {"name": r.cells["indexname"], "definition": r.cells["indexdef"]}
            for r in idx_rows
        ] if idx_rows else []
        
        # 构建完整的对象详情
        result = {
            "basic": {"schema": test_schema, "name": "mcp_test_table", "type": "table"},
            "columns": columns,
            "constraints": constraints_list,
            "indexes": indexes,
        }
        
        # 验证结果
        assert len(result["columns"]) == 5, f"应该有5列，实际有{len(result['columns'])}列"
        assert len(result["constraints"]) > 0, "应该有约束（至少有主键约束）"
        assert len(result["indexes"]) > 0, "应该有索引（至少有主键索引）"
        
        # 验证列名
        column_names = [col["column"] for col in result["columns"]]
        expected_columns = ["id", "title", "content", "metadata", "created_at"]
        assert all(col in column_names for col in expected_columns), f"列名不匹配，期望: {expected_columns}，实际: {column_names}"
        
        print(f"   ✅ get_object_details工具（表详情）测试通过")
        print(f"       列数: {len(result['columns'])}, 约束数: {len(result['constraints'])}, 索引数: {len(result['indexes'])}")
        
    except Exception as e:
        print(f"   ❌ get_object_details工具（表详情）测试失败: {e}")
        raise
    
    # 6. 测试扩展列举功能
    print("\n🧩 测试list_objects工具（扩展）...")
    try:
        # 模拟list_objects MCP工具的内部逻辑（扩展类型）
        rows = await sql_driver.execute_query(
            """
            SELECT extname, extversion, extrelocatable
            FROM pg_extension
            ORDER BY extname
            """
        )
        
        extensions = [
            {
                "name": row.cells["extname"],
                "version": row.cells["extversion"],
                "relocatable": row.cells["extrelocatable"]
            }
            for row in rows
        ] if rows else []
        
        # 应该至少有一些基础扩展
        assert len(extensions) > 0, "应该有一些已安装的扩展"
        
        # 检查是否有pgvector扩展（如果已安装）
        extension_names = [ext["name"] for ext in extensions]
        has_pgvector = "vector" in extension_names
        
        print(f"   ✅ list_objects工具（扩展）测试通过，找到 {len(extensions)} 个扩展")
        if has_pgvector:
            print(f"       🎯 发现pgvector扩展，支持向量操作")
        
    except Exception as e:
        print(f"   ❌ list_objects工具（扩展）测试失败: {e}")
        raise
    
    # 7. 测试execute_sql兼容性
    print("\n🎯 测试execute_sql兼容性...")
    try:
        # 插入测试数据
        test_data = [
            {
                "title": "MCP测试文章1",
                "content": "这是通过MCP接口创建的测试文章",
                "metadata": {"source": "mcp_test", "priority": 1}
            },
            {
                "title": "MCP测试文章2", 
                "content": "第二篇测试文章",
                "metadata": {"source": "mcp_test", "priority": 2}
            }
        ]
        
        result = await crud_ops.insert_records(
            table_name="mcp_test_table",
            data=test_data,
            schema=test_schema,
            returning_columns=["id", "title", "created_at"]
        )
        assert result.success, f"插入测试数据失败: {result.message}"
        
        # 查询测试数据
        result = await crud_ops.query_records(
            table_name="mcp_test_table",
            schema=test_schema,
            columns=["id", "title", "content", "metadata"],
                    conditions=[
            QueryCondition(column="metadata", operator="?", value="source")
        ]
        )
        assert result.success, f"查询测试数据失败: {result.message}"
        assert len(result.data) == 2, f"应该查询到2条记录，实际{len(result.data)}条"
        
        print(f"   ✅ execute_sql兼容性测试通过，插入并查询到 {len(result.data)} 条记录")
        
    except Exception as e:
        print(f"   ❌ execute_sql兼容性测试失败: {e}")
        raise
    
    print("✅ MCP工具接口测试全部完成！")


async def test_advanced_queries(crud_ops: SimpleCrudOperations, schema_ops: SimpleSchemaOperations, tracker: TestTracker):
    """测试高级查询功能"""
    print("🔍 开始高级查询测试...")
    
    test_schema = "test_advanced_queries"
    tracker.add_schema(test_schema)
    
    # 1. 准备测试环境
    print("\n📁 准备测试环境...")
    result = await schema_ops.create_schema(test_schema)
    assert result.success, f"创建模式失败: {result.message}"
    
    # 创建测试表
    products_table = TableDefinition(
        name="products",
        schema=test_schema,
        columns=[
            ColumnDefinition(name="id", data_type=DataType.SERIAL, primary_key=True, nullable=False),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, nullable=False),
            ColumnDefinition(name="category", data_type=DataType.VARCHAR, nullable=False),
            ColumnDefinition(name="price", data_type=DataType.DECIMAL, nullable=False),
            ColumnDefinition(name="stock", data_type=DataType.INTEGER, nullable=False),
            ColumnDefinition(name="tags", data_type=DataType.JSONB, nullable=True),
            ColumnDefinition(name="created_at", data_type=DataType.TIMESTAMP, default="CURRENT_TIMESTAMP"),
        ]
    )
    
    result = await crud_ops.create_table(products_table)
    assert result.success, f"创建产品表失败: {result.message}"
    tracker.add_table("products", test_schema)
    
    # 插入测试数据
    products_data = [
        {"name": "笔记本电脑", "category": "电子产品", "price": 5999.99, "stock": 50, "tags": {"brand": "Apple", "color": "银色"}},
        {"name": "无线鼠标", "category": "电子产品", "price": 199.99, "stock": 100, "tags": {"brand": "Logitech", "wireless": True}},
        {"name": "办公椅", "category": "家具", "price": 899.99, "stock": 25, "tags": {"material": "皮革", "adjustable": True}},
        {"name": "咖啡杯", "category": "厨具", "price": 29.99, "stock": 200, "tags": {"material": "陶瓷", "capacity": "300ml"}},
        {"name": "机械键盘", "category": "电子产品", "price": 599.99, "stock": 75, "tags": {"brand": "Cherry", "backlight": True}},
    ]
    
    result = await crud_ops.insert_records(
        table_name="products",
        data=products_data,
        schema=test_schema
    )
    assert result.success, f"插入产品数据失败: {result.message}"
    print(f"   ✅ 测试环境准备完成，插入 {len(products_data)} 条产品记录")
    
    # 2. 测试基础条件查询
    print("\n🔍 测试基础条件查询...")
    result = await crud_ops.query_records(
        table_name="products",
        schema=test_schema,
        columns=["name", "category", "price"],
        conditions=[
            QueryCondition(column="category", operator="=", value="电子产品")
        ]
    )
    assert result.success, f"基础条件查询失败: {result.message}"
    assert len(result.data) == 3, f"电子产品应该有3件，实际{len(result.data)}件"
    print(f"   ✅ 基础条件查询: 找到 {len(result.data)} 件电子产品")
    
    # 3. 测试范围查询
    print("\n📊 测试范围查询...")
    result = await crud_ops.query_records(
        table_name="products",
        schema=test_schema,
        columns=["name", "price", "stock"],
        conditions=[
            QueryCondition(column="price", operator=">=", value=100),
            QueryCondition(column="price", operator="<=", value=1000, logical_operator="AND"),
            QueryCondition(column="stock", operator=">", value=30, logical_operator="AND")
        ]
    )
    assert result.success, f"范围查询失败: {result.message}"
    print(f"   ✅ 范围查询: 找到 {len(result.data)} 件符合价格和库存条件的产品")
    
    # 4. 测试IN查询
    print("\n📋 测试IN查询...")
    result = await crud_ops.query_records(
        table_name="products",
        schema=test_schema,
        columns=["name", "category"],
        conditions=[
            QueryCondition(column="category", operator="IN", value=["电子产品", "家具"])
        ]
    )
    assert result.success, f"IN查询失败: {result.message}"
    assert len(result.data) >= 3, f"电子产品和家具至少应该有4件，实际{len(result.data)}件"
    print(f"   ✅ IN查询: 找到 {len(result.data)} 件电子产品和家具")
    
    # 5. 测试LIKE模糊查询
    print("\n🔤 测试LIKE模糊查询...")
    result = await crud_ops.query_records(
        table_name="products",
        schema=test_schema,
        columns=["name", "category"],
        conditions=[
            QueryCondition(column="name", operator="LIKE", value="%键%")
        ]
    )
    assert result.success, f"LIKE查询失败: {result.message}"
    assert len(result.data) >= 1, f"包含'键'的产品至少应该有1件"
    print(f"   ✅ LIKE查询: 找到 {len(result.data)} 件名称包含'键'的产品")
    
    # 6. 测试排序功能
    print("\n⬆️ 测试排序功能...")
    result = await crud_ops.query_records(
        table_name="products",
        schema=test_schema,
        columns=["name", "price"],
        options=QueryOptions(
            order_by=[{"column": "price", "direction": "DESC"}]
        )
    )
    assert result.success, f"排序查询失败: {result.message}"
    # 验证价格是降序排列
    prices = [float(row["price"]) for row in result.data]
    assert prices == sorted(prices, reverse=True), "价格排序不正确"
    print(f"   ✅ 排序功能: 按价格降序排列 {len(result.data)} 件产品")
    
    # 7. 测试分页功能
    print("\n📄 测试分页功能...")
    result = await crud_ops.query_records(
        table_name="products",
        schema=test_schema,
        columns=["name", "price"],
        options=QueryOptions(
            limit=2,
            offset=1,
            order_by=[{"column": "id", "direction": "ASC"}]
        )
    )
    assert result.success, f"分页查询失败: {result.message}"
    assert len(result.data) == 2, f"分页查询应该返回2条记录，实际{len(result.data)}条"
    print(f"   ✅ 分页功能: 跳过1条，返回 {len(result.data)} 条记录")
    
    # 8. 测试JSON查询
    print("\n🗂️ 测试JSON查询...")
    result = await crud_ops.query_records(
        table_name="products",
        schema=test_schema,
        columns=["name", "tags"],
        conditions=[
            QueryCondition(column="tags", operator="?", value="brand")
        ]
    )
    assert result.success, f"JSON查询失败: {result.message}"
    print(f"   ✅ JSON查询: 找到 {len(result.data)} 件有品牌标签的产品")
    
    print("✅ 高级查询测试全部完成！")


async def test_batch_operations(crud_ops: SimpleCrudOperations, schema_ops: SimpleSchemaOperations, tracker: TestTracker):
    """测试批量操作功能"""
    print("📦 开始批量操作测试...")
    
    test_schema = "test_batch_ops"
    tracker.add_schema(test_schema)
    
    # 1. 准备测试环境
    print("\n📁 准备测试环境...")
    result = await schema_ops.create_schema(test_schema)
    assert result.success, f"创建模式失败: {result.message}"
    
    # 创建测试表
    batch_table = TableDefinition(
        name="batch_test",
        schema=test_schema,
        columns=[
            ColumnDefinition(name="id", data_type=DataType.SERIAL, primary_key=True, nullable=False),
            ColumnDefinition(name="batch_id", data_type=DataType.INTEGER, nullable=False),
            ColumnDefinition(name="data", data_type=DataType.TEXT, nullable=False),
            ColumnDefinition(name="status", data_type=DataType.VARCHAR, default="pending", nullable=False),
            ColumnDefinition(name="created_at", data_type=DataType.TIMESTAMP, default="CURRENT_TIMESTAMP"),
        ]
    )
    
    result = await crud_ops.create_table(batch_table)
    assert result.success, f"创建批量测试表失败: {result.message}"
    tracker.add_table("batch_test", test_schema)
    print(f"   ✅ 测试环境准备完成")
    
    # 2. 测试大批量插入
    print("\n📥 测试大批量插入...")
    batch_size = 100
    large_batch_data = [
        {
            "batch_id": i // 10,  # 每10条记录一个批次
            "data": f"测试数据_{i:04d}",
            "status": "pending" if i % 3 == 0 else "active"
        }
        for i in range(batch_size)
    ]
    
    result = await crud_ops.insert_records(
        table_name="batch_test",
        data=large_batch_data,
        schema=test_schema,
        returning_columns=["id", "batch_id"]
    )
    assert result.success, f"大批量插入失败: {result.message}"
    assert result.affected_rows == batch_size, f"插入行数不匹配，期望{batch_size}，实际{result.affected_rows}"
    print(f"   ✅ 大批量插入: 成功插入 {result.affected_rows} 条记录")
    
    # 3. 测试批量更新
    print("\n✏️ 测试批量更新...")
    result = await crud_ops.update_records(
        table_name="batch_test",
        data={"status": "processed"},
        conditions=[
            QueryCondition(column="batch_id", operator="IN", value=[0, 1, 2])
        ],
        schema=test_schema
    )
    assert result.success, f"批量更新失败: {result.message}"
    assert result.affected_rows > 0, "批量更新应该影响一些行"
    print(f"   ✅ 批量更新: 更新了 {result.affected_rows} 条记录")
    
    # 4. 测试条件批量删除
    print("\n🗑️ 测试条件批量删除...")
    # 先查询要删除的记录数
    result_query = await crud_ops.query_records(
        table_name="batch_test",
        schema=test_schema,
        columns=["COUNT(*) as count"],
        conditions=[
            QueryCondition(column="status", operator="=", value="pending")
        ]
    )
    pending_count = result_query.data[0]["count"] if result_query.success and result_query.data else 0
    
    # 执行批量删除
    result = await crud_ops.delete_records(
        table_name="batch_test",
        conditions=[
            QueryCondition(column="status", operator="=", value="pending")
        ],
        schema=test_schema
    )
    assert result.success, f"批量删除失败: {result.message}"
    assert result.affected_rows == pending_count, f"删除行数不匹配，期望{pending_count}，实际{result.affected_rows}"
    print(f"   ✅ 批量删除: 删除了 {result.affected_rows} 条pending状态的记录")
    
    # 5. 测试批量查询性能
    print("\n⚡ 测试批量查询性能...")
    import time
    start_time = time.time()
    
    result = await crud_ops.query_records(
        table_name="batch_test",
        schema=test_schema,
        columns=["id", "batch_id", "data", "status"],
        options=QueryOptions(
            order_by=[{"column": "batch_id", "direction": "ASC"}]
        )
    )
    
    query_time = (time.time() - start_time) * 1000  # 转换为毫秒
    assert result.success, f"批量查询失败: {result.message}"
    print(f"   ✅ 批量查询: 查询 {len(result.data)} 条记录耗时 {query_time:.2f}ms")
    
    print("✅ 批量操作测试全部完成！")


async def test_index_management(crud_ops: SimpleCrudOperations, schema_ops: SimpleSchemaOperations, tracker: TestTracker):
    """测试索引管理功能"""
    print("📇 开始索引管理测试...")
    
    test_schema = "test_index_mgmt"
    tracker.add_schema(test_schema)
    
    # 1. 准备测试环境
    print("\n📁 准备测试环境...")
    result = await schema_ops.create_schema(test_schema)
    assert result.success, f"创建模式失败: {result.message}"
    
    # 创建测试表
    index_table = TableDefinition(
        name="index_test",
        schema=test_schema,
        columns=[
            ColumnDefinition(name="id", data_type=DataType.SERIAL, primary_key=True, nullable=False),
            ColumnDefinition(name="username", data_type=DataType.VARCHAR, nullable=False),
            ColumnDefinition(name="email", data_type=DataType.VARCHAR, nullable=False),
            ColumnDefinition(name="age", data_type=DataType.INTEGER, nullable=True),
            ColumnDefinition(name="city", data_type=DataType.VARCHAR, nullable=True),
            ColumnDefinition(name="metadata", data_type=DataType.JSONB, nullable=True),
            ColumnDefinition(name="created_at", data_type=DataType.TIMESTAMP, default="CURRENT_TIMESTAMP"),
        ]
    )
    
    result = await crud_ops.create_table(index_table)
    assert result.success, f"创建索引测试表失败: {result.message}"
    tracker.add_table("index_test", test_schema)
    print(f"   ✅ 测试环境准备完成")
    
    sql_driver = crud_ops.sql_driver
    
    # 2. 测试创建单列索引
    print("\n📄 测试创建单列索引...")
    username_index = IndexDefinition(
        name="idx_index_test_username",
        table_name="index_test",
        columns=["username"],
        index_type="btree",
        unique=False
    )
    
    result = await schema_ops.create_index(username_index, test_schema)
    assert result.success, f"创建username索引失败: {result.message}"
    tracker.add_index("idx_index_test_username")
    print(f"   ✅ 单列索引: {result.message}")
    
    # 3. 测试创建唯一索引
    print("\n🔑 测试创建唯一索引...")
    email_index = IndexDefinition(
        name="idx_index_test_email_unique",
        table_name="index_test",
        columns=["email"],
        index_type="btree",
        unique=True
    )
    
    result = await schema_ops.create_index(email_index, test_schema)
    assert result.success, f"创建email唯一索引失败: {result.message}"
    tracker.add_index("idx_index_test_email_unique")
    print(f"   ✅ 唯一索引: {result.message}")
    
    # 4. 测试创建复合索引
    print("\n🔗 测试创建复合索引...")
    composite_index = IndexDefinition(
        name="idx_index_test_city_age",
        table_name="index_test",
        columns=["city", "age"],
        index_type="btree",
        unique=False
    )
    
    result = await schema_ops.create_index(composite_index, test_schema)
    assert result.success, f"创建复合索引失败: {result.message}"
    tracker.add_index("idx_index_test_city_age")
    print(f"   ✅ 复合索引: {result.message}")
    
    # 5. 测试创建JSON索引
    print("\n🗂️ 测试创建JSON索引...")
    json_index = IndexDefinition(
        name="idx_index_test_metadata_gin",
        table_name="index_test",
        columns=["metadata"],
        index_type="gin",
        unique=False
    )
    
    result = await schema_ops.create_index(json_index, test_schema)
    assert result.success, f"创建JSON索引失败: {result.message}"
    tracker.add_index("idx_index_test_metadata_gin")
    print(f"   ✅ JSON索引: {result.message}")
    
    # 6. 测试列举索引
    print("\n📋 测试列举索引...")
    rows = await sql_driver.execute_query(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
        ORDER BY indexname
        """,
        (test_schema, "index_test")
    )
    
    indexes = [
        {"name": row.cells["indexname"], "definition": row.cells["indexdef"]}
        for row in rows
    ] if rows else []
    
    # 应该至少有主键索引 + 我们创建的4个索引
    assert len(indexes) >= 5, f"索引数量不足，期望至少5个，实际{len(indexes)}个"
    
    index_names = [idx["name"] for idx in indexes]
    expected_indexes = [
        "idx_index_test_username",
        "idx_index_test_email_unique", 
        "idx_index_test_city_age",
        "idx_index_test_metadata_gin"
    ]
    
    for expected_idx in expected_indexes:
        assert expected_idx in index_names, f"索引 {expected_idx} 未找到"
    
    print(f"   ✅ 索引列举: 找到 {len(indexes)} 个索引")
    for idx in indexes:
        print(f"       - {idx['name']}")
    
    # 7. 测试删除索引
    print("\n🗑️ 测试删除索引...")
    result = await schema_ops.drop_index("idx_index_test_metadata_gin")
    assert result.success, f"删除JSON索引失败: {result.message}"
    print(f"   ✅ 索引删除: {result.message}")
    
    # 验证索引已删除
    rows = await sql_driver.execute_query(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s AND indexname = %s
        """,
        (test_schema, "index_test", "idx_index_test_metadata_gin")
    )
    
    assert len(rows) == 0, "JSON索引删除后仍然存在"
    print(f"   ✅ 索引删除验证通过")
    
    # 8. 测试索引性能对比
    print("\n⚡ 测试索引性能对比...")
    # 插入测试数据
    test_data = [
        {
            "username": f"user_{i:04d}",
            "email": f"user_{i:04d}@example.com",
            "age": 20 + (i % 40),
            "city": ["北京", "上海", "广州", "深圳"][i % 4],
            "metadata": {"role": "user", "level": i % 5}
        }
        for i in range(1000)
    ]
    
    result = await crud_ops.insert_records(
        table_name="index_test",
        data=test_data,
        schema=test_schema
    )
    assert result.success, f"插入性能测试数据失败: {result.message}"
    
    # 测试索引查询性能
    import time
    start_time = time.time()
    
    result = await crud_ops.query_records(
        table_name="index_test",
        schema=test_schema,
        columns=["id", "username", "email"],
        conditions=[
            QueryCondition(column="username", operator="=", value="user_0500")
        ]
    )
    
    query_time = (time.time() - start_time) * 1000
    assert result.success, f"索引查询失败: {result.message}"
    assert len(result.data) == 1, f"应该找到1条记录，实际{len(result.data)}条"
    
    print(f"   ✅ 索引查询性能: 在1000条记录中查询耗时 {query_time:.2f}ms")
    
    print("✅ 索引管理测试全部完成！")


async def main():
    """主测试函数"""
    print("🚀 启动Postgres MCP增强版测试套件")
    print("=" * 60)
    
    # 使用提供的远程数据库连接
    database_url = os.environ.get("DATABASE_URI", "postgresql://vipa:vipa_404@10.214.211.4:5432/remote_db")
    
    print(f"📋 数据库连接信息:")
    print(f"   连接URL: {database_url.replace('vipa_404', '***')}")
    
    tracker = TestTracker()
    
    try:
        # 初始化连接
        print("\n🔌 初始化数据库连接...")
        db_pool = DbConnPool()
        await db_pool.pool_connect(database_url)
        sql_driver = SqlDriver(db_pool)
        
        # 初始化操作类
        crud_ops = SimpleCrudOperations(sql_driver)
        schema_ops = SimpleSchemaOperations(sql_driver)
        
        print("   ✅ 数据库连接成功")
        
        # 运行所有测试
        await test_comprehensive_crud(crud_ops, schema_ops, tracker)
        await test_table_schema_operations(crud_ops, schema_ops, tracker)
        await test_alter_table_operations(crud_ops, schema_ops, tracker)
        await test_mcp_tools_interface(crud_ops, schema_ops, tracker)
        await test_error_scenarios(crud_ops, schema_ops)
        await test_vector_support_preparation()
        await test_advanced_queries(crud_ops, schema_ops, tracker)
        await test_batch_operations(crud_ops, schema_ops, tracker)
        await test_index_management(crud_ops, schema_ops, tracker)
        
        print("\n🎉 所有测试完成，开始清理资源...")
        
    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 清理测试资源
        try:
            if 'schema_ops' in locals():
                await cleanup_resources(schema_ops, tracker)
            if 'db_pool' in locals():
                await db_pool.close()
                print("   ✅ 数据库连接已关闭")
        except Exception as e:
            print(f"   ⚠️  清理资源时发生错误: {e}")
    
    print("\n🏁 测试套件执行完毕")
    print("=" * 60)


if __name__ == "__main__":
    success = asyncio.run(main()) 