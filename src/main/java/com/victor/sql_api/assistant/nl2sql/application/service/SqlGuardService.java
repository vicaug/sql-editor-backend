package com.victor.sql_api.assistant.nl2sql.application.service;

import com.victor.sql_api.assistant.metadata.domain.model.MetadataContext;
import com.victor.sql_api.assistant.metadata.domain.model.RelevantColumn;
import com.victor.sql_api.assistant.metadata.domain.model.RelevantRelationship;
import com.victor.sql_api.assistant.metadata.domain.model.RelevantTable;
import com.victor.sql_api.assistant.nl2sql.domain.model.QueryUnderstanding;
import com.victor.sql_api.assistant.nl2sql.domain.model.SqlValidationResult;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class SqlGuardService {

    private static final Set<Class<?>> BLOCKED_STATEMENTS = Set.of(
            net.sf.jsqlparser.statement.insert.Insert.class,
            net.sf.jsqlparser.statement.update.Update.class,
            net.sf.jsqlparser.statement.delete.Delete.class,
            net.sf.jsqlparser.statement.drop.Drop.class,
            net.sf.jsqlparser.statement.alter.Alter.class,
            net.sf.jsqlparser.statement.create.table.CreateTable.class,
            net.sf.jsqlparser.statement.truncate.Truncate.class,
            net.sf.jsqlparser.statement.merge.Merge.class
    );

    public SqlValidationResult validate(String sql, MetadataContext metadataContext) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        LinkedHashSet<String> detectedTables = new LinkedHashSet<>();
        LinkedHashSet<String> detectedColumns = new LinkedHashSet<>();

        if (sql == null || sql.trim().isBlank()) {
            errors.add("SQL vazio nao e permitido");
            return result(errors, warnings, detectedTables, detectedColumns);
        }

        if (containsMultipleStatements(sql)) {
            errors.add("Multiple statements are not allowed");
            return result(errors, warnings, detectedTables, detectedColumns);
        }

        Statement statement;
        try {
            statement = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException ex) {
            errors.add("Invalid SQL syntax");
            return result(errors, warnings, detectedTables, detectedColumns);
        }

        if (!isSelectStatement(statement)) {
            errors.add("Only SELECT statements are allowed");
            return result(errors, warnings, detectedTables, detectedColumns);
        }

        detectedTables.addAll(extractTables(statement));
        detectedColumns.addAll(extractColumns(statement));

        validateTables(metadataContext, detectedTables, errors);
        validateColumns(metadataContext, detectedColumns, warnings);
        validateJoins(statement, metadataContext, warnings);

        if (!hasGroupBy(statement) && !hasAggregation(statement) && !hasLimit(statement)) {
            warnings.add("Missing LIMIT");
        }
        if (hasSelectAll(statement)) {
            warnings.add("SELECT * detected; prefer explicit columns");
        }

        return result(errors, warnings, detectedTables, detectedColumns);
    }

    // Compatibility overload to avoid breaking current integration points.
    public SqlValidationResult validate(String sql, MetadataContext metadataContext, QueryUnderstanding understanding) {
        return validate(sql, metadataContext);
    }

    List<String> extractTables(Statement stmt) {
        if (!(stmt instanceof Select select)) {
            return List.of();
        }
        TablesNamesFinder finder = new TablesNamesFinder();
        List<String> tables = finder.getTableList((Statement) select);
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        for (String table : tables) {
            if (table != null && !table.isBlank()) {
                normalized.add(table.trim().toLowerCase(Locale.ROOT));
            }
        }
        return new ArrayList<>(normalized);
    }

    List<String> extractColumns(Statement stmt) {
        if (!(stmt instanceof Select select)) {
            return List.of();
        }
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        collectColumnsFromSelectNode(select, columns);
        return new ArrayList<>(columns);
    }

    boolean isSelectStatement(Statement stmt) {
        if (stmt == null) {
            return false;
        }
        if (stmt instanceof Select) {
            return true;
        }
        return BLOCKED_STATEMENTS.stream().noneMatch(type -> type.isAssignableFrom(stmt.getClass())) && stmt.toString().trim().toLowerCase(Locale.ROOT).startsWith("with");
    }

    boolean containsMultipleStatements(String sql) {
        if (sql == null) {
            return false;
        }
        String trimmed = sql.trim();
        if (trimmed.isEmpty()) {
            return false;
        }
        String withoutFinalSemicolon = trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
        return withoutFinalSemicolon.contains(";");
    }

    void validateTables(MetadataContext metadataContext, Set<String> detectedTables, List<String> errors) {
        Set<String> allowed = new HashSet<>();
        if (metadataContext != null && metadataContext.tables() != null) {
            for (RelevantTable table : metadataContext.tables()) {
                String fq = (table.schemaName() + "." + table.tableName()).toLowerCase(Locale.ROOT);
                allowed.add(fq);
                allowed.add(table.tableName().toLowerCase(Locale.ROOT));
            }
        }

        for (String detected : detectedTables) {
            if (!allowed.contains(detected.toLowerCase(Locale.ROOT))) {
                errors.add("Table " + detected + " is not allowed");
            }
        }
    }

    void validateColumns(MetadataContext metadataContext, Set<String> detectedColumns, List<String> warnings) {
        Set<String> allowedSimple = new HashSet<>();
        Set<String> allowedQualified = new HashSet<>();
        if (metadataContext != null && metadataContext.columns() != null) {
            for (RelevantColumn col : metadataContext.columns()) {
                allowedSimple.add(col.columnName().toLowerCase(Locale.ROOT));
                allowedQualified.add((col.tableName() + "." + col.columnName()).toLowerCase(Locale.ROOT));
                allowedQualified.add((col.schemaName() + "." + col.tableName() + "." + col.columnName()).toLowerCase(Locale.ROOT));
            }
        }

        for (String detected : detectedColumns) {
            String normalized = detected.toLowerCase(Locale.ROOT);
            String simple = normalized.contains(".") ? normalized.substring(normalized.lastIndexOf('.') + 1) : normalized;
            if (!allowedSimple.contains(simple) && !allowedQualified.contains(normalized)) {
                warnings.add("Column " + detected + " is outside the metadata context");
            }
        }
    }

    void validateJoins(Statement stmt, MetadataContext metadataContext, List<String> warnings) {
        if (!(stmt instanceof Select select) || metadataContext == null || metadataContext.relationships() == null) {
            return;
        }

        Set<String> allowedLinks = new HashSet<>();
        for (RelevantRelationship rel : metadataContext.relationships()) {
            String forward = (rel.fromTable() + "." + rel.fromColumn() + "->" + rel.toTable() + "." + rel.toColumn()).toLowerCase(Locale.ROOT);
            String backward = (rel.toTable() + "." + rel.toColumn() + "->" + rel.fromTable() + "." + rel.fromColumn()).toLowerCase(Locale.ROOT);
            allowedLinks.add(forward);
            allowedLinks.add(backward);
        }

        List<String> joinLinks = extractJoinLinks(select);
        for (String link : joinLinks) {
            if (!allowedLinks.contains(link.toLowerCase(Locale.ROOT))) {
                warnings.add("Join relationship not found in metadata: " + link);
            }
        }
    }

    private SqlValidationResult result(
            List<String> errors,
            List<String> warnings,
            Set<String> detectedTables,
            Set<String> detectedColumns
    ) {
        return new SqlValidationResult(
                errors.isEmpty(),
                List.copyOf(errors),
                List.copyOf(warnings),
                List.copyOf(detectedTables),
                List.copyOf(detectedColumns)
        );
    }

    private void collectColumnsFromSelectNode(Select body, Set<String> out) {
        if (body == null) {
            return;
        }
        if (body instanceof PlainSelect plainSelect) {
            collectColumnsFromPlainSelect(plainSelect, out);
            return;
        }
        if (body instanceof SetOperationList setOperationList) {
            for (Select select : setOperationList.getSelects()) {
                collectColumnsFromSelectNode(select, out);
            }
        }
    }

    private void collectColumnsFromPlainSelect(PlainSelect select, Set<String> out) {
        for (SelectItem<?> item : select.getSelectItems()) {
            if (item instanceof SelectItem<?> selectItem) {
                Expression expression = selectItem.getExpression();
                collectColumnsFromExpression(expression, out);
            }
        }

        collectColumnsFromExpression(select.getWhere(), out);
        collectColumnsFromExpression(select.getHaving(), out);

        if (select.getGroupBy() != null && select.getGroupBy().getGroupByExpressionList() != null) {
            for (Object expression : select.getGroupBy().getGroupByExpressionList()) {
                if (expression instanceof Expression expr) {
                    collectColumnsFromExpression(expr, out);
                }
            }
        }

        if (select.getOrderByElements() != null) {
            for (OrderByElement orderByElement : select.getOrderByElements()) {
                collectColumnsFromExpression(orderByElement.getExpression(), out);
            }
        }
    }

    private void collectColumnsFromExpression(Expression expression, Set<String> out) {
        if (expression == null) {
            return;
        }

        expression.accept(new net.sf.jsqlparser.util.deparser.ExpressionDeParser() {
            @Override
            public <S> StringBuilder visit(Column column, S context) {
                Table table = column.getTable();
                String tableName = table == null ? "" : table.getName();
                String name = (tableName == null || tableName.isBlank())
                        ? column.getColumnName()
                        : tableName + "." + column.getColumnName();
                out.add(name.toLowerCase(Locale.ROOT));
                return super.visit(column, context);
            }

            @Override
            public <S> StringBuilder visit(Function function, S context) {
                ExpressionList<?> parameters = function.getParameters();
                if (parameters != null) {
                    for (Object param : parameters) {
                        if (param instanceof Expression expr) {
                            collectColumnsFromExpression(expr, out);
                        }
                    }
                }
                return super.visit(function, context);
            }
        });
    }

    private List<String> extractJoinLinks(Select body) {
        if (!(body instanceof PlainSelect plainSelect) || plainSelect.getJoins() == null) {
            return List.of();
        }

        List<String> links = new ArrayList<>();
        for (Join join : plainSelect.getJoins()) {
            if (join.getOnExpressions() == null) {
                continue;
            }
            for (Expression onExpression : join.getOnExpressions()) {
                LinkedHashSet<String> columns = new LinkedHashSet<>();
                collectColumnsFromExpression(onExpression, columns);
                if (columns.size() < 2) {
                    continue;
                }
                List<String> items = new ArrayList<>(columns);
                String left = normalizeJoinColumn(items.get(0));
                String right = normalizeJoinColumn(items.get(1));
                if (left != null && right != null) {
                    links.add(left + "->" + right);
                }
            }
        }
        return links;
    }

    private String normalizeJoinColumn(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String[] parts = value.toLowerCase(Locale.ROOT).split("\\.");
        if (parts.length == 2) {
            return parts[0] + "." + parts[1];
        }
        if (parts.length >= 3) {
            return parts[parts.length - 2] + "." + parts[parts.length - 1];
        }
        return null;
    }

    private boolean hasGroupBy(Statement stmt) {
        if (!(stmt instanceof Select plainSelect)) {
            return false;
        }
        if (plainSelect instanceof PlainSelect ps) {
            return ps.getGroupBy() != null
                    && ps.getGroupBy().getGroupByExpressionList() != null
                    && !ps.getGroupBy().getGroupByExpressionList().isEmpty();
        }
        return false;
    }

    private boolean hasAggregation(Statement stmt) {
        if (!(stmt instanceof Select plainSelect) || !(plainSelect instanceof PlainSelect ps)) {
            return false;
        }
        for (SelectItem<?> item : ps.getSelectItems()) {
            Expression expr = item.getExpression();
            if (expr instanceof Function function) {
                String name = function.getName();
                if (name != null) {
                    String upper = name.toUpperCase(Locale.ROOT);
                    if (upper.equals("SUM") || upper.equals("COUNT") || upper.equals("AVG") || upper.equals("MIN") || upper.equals("MAX")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean hasLimit(Statement stmt) {
        if (!(stmt instanceof Select plainSelect) || !(plainSelect instanceof PlainSelect ps)) {
            return false;
        }
        return ps.getLimit() != null;
    }

    private boolean hasSelectAll(Statement stmt) {
        if (!(stmt instanceof Select plainSelect) || !(plainSelect instanceof PlainSelect ps)) {
            return false;
        }
        for (SelectItem<?> item : ps.getSelectItems()) {
            if (item.getExpression() instanceof net.sf.jsqlparser.statement.select.AllColumns) {
                return true;
            }
            if (item.getExpression() instanceof net.sf.jsqlparser.statement.select.AllTableColumns) {
                return true;
            }
        }
        return false;
    }
}
