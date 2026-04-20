package com.victor.sql_api.shared.api;

import java.time.Instant;

public record ResponseMeta(
        Instant timestamp,
        String traceId
) {
}
