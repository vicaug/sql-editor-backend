package com.victor.sql_api.assistant.metadata_retrieval.model;

public record RetrievalConstraints(
        int maxTables,
        int maxColumnsPerTable,
        int maxRelationships,
        int maxTotalColumns,
        double minTableScoreThreshold,
        double minColumnScoreThreshold
) {
    public RetrievalConstraints normalized() {
        RetrievalConstraints defaults = defaults();
        int normalizedMaxTables = maxTables > 0 ? maxTables : defaults.maxTables();
        int normalizedMaxColumnsPerTable = maxColumnsPerTable > 0 ? maxColumnsPerTable : defaults.maxColumnsPerTable();
        int normalizedMaxRelationships = maxRelationships > 0 ? maxRelationships : defaults.maxRelationships();
        int normalizedMaxTotalColumns = maxTotalColumns > 0 ? maxTotalColumns : defaults.maxTotalColumns();
        double normalizedMinTableScore = minTableScoreThreshold > 0.0 ? minTableScoreThreshold : defaults.minTableScoreThreshold();
        double normalizedMinColumnScore = minColumnScoreThreshold > 0.0 ? minColumnScoreThreshold : defaults.minColumnScoreThreshold();
        return new RetrievalConstraints(
                normalizedMaxTables,
                normalizedMaxColumnsPerTable,
                normalizedMaxRelationships,
                normalizedMaxTotalColumns,
                clamp01(normalizedMinTableScore),
                clamp01(normalizedMinColumnScore)
        );
    }

    private static double clamp01(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }

    public static RetrievalConstraints defaults() {
        return new RetrievalConstraints(
            12,
            20,
            12,
            24,
            0.20,
            0.05
        );
    }
}






