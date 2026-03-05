-- ============================================================
-- Create monitoring schema in Unity Catalog
-- ============================================================

CREATE CATALOG IF NOT EXISTS ${catalog};
CREATE SCHEMA IF NOT EXISTS ${catalog}.monitoring
  COMMENT 'Streaming monitoring tables for structured streaming observability';
