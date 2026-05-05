package com.victor.sql_api.assistant.metadata.infrastructure;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class MetadataCatalogGateway {
    private final JdbcTemplate jdbcTemplate;

    public MetadataCatalogGateway(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> fetchActiveTables() {
        return jdbcTemplate.queryForList("""
                select
                    t.table_schema,
                    t.table_name,
                    t.table_comment,
                    t.table_description_llm,
                    t.is_active
                from eqt_metadata.md_table t
                where coalesce(t.is_active, true) = true
                """);
    }

    public List<Map<String, Object>> fetchColumnsByTableFilter(String tableFilter, Object[] params) {
        String sql = """
                select
                    t.table_schema,
                    t.table_name,
                    c.column_name,
                    c.data_type,
                    c.column_comment,
                    c.semantic_role,
                    c.is_primary_key,
                    c.is_foreign_key
                from eqt_metadata.md_column c
                inner join eqt_metadata.md_table t on t.id_md_table = c.id_md_table
                where coalesce(t.is_active, true) = true
                  and (%s)
                """.formatted(tableFilter);
        return jdbcTemplate.queryForList(sql, params);
    }

    public List<Map<String, Object>> fetchRelationshipsByTableFilters(String sourceFilter, String targetFilter, Object[] params) {
        String sql = """
                select
                    source_table_schema,
                    source_table_name,
                    source_column_name,
                    target_table_schema,
                    target_table_name,
                    target_column_name,
                    relationship_type
                from eqt_metadata.md_relationship
                where (%s)
                  and (%s)
                """.formatted(sourceFilter, targetFilter);
        return jdbcTemplate.queryForList(sql, params);
    }
}




