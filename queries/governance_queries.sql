-- ============================================================
-- wh_governance validation queries
-- Run against: Nations Analytics / wh_governance
-- ============================================================


-- 1. Hierarchy Overview — counts by object type
-- -----------------------------------------------
SELECT
    objects_type,
    COUNT(*) AS total
FROM data_lineage.objects
GROUP BY objects_type
ORDER BY total DESC;


-- 2. Report → Page rollup — pages and tables per report
-- -------------------------------------------------------
SELECT
    r.item_name        AS report,
    p.item_name        AS page,
    COUNT(t.objects_id) AS table_count
FROM data_lineage.objects r
JOIN data_lineage.objects p ON p.parent_objects_id = r.objects_id AND p.objects_type = 'Page'
LEFT JOIN data_lineage.objects t ON t.parent_objects_id = p.objects_id AND t.objects_type = 'Table'
WHERE r.objects_type = 'Report'
GROUP BY r.item_name, p.item_name
ORDER BY r.item_name, p.item_name;


-- 3. Page → Table drill-down — all tables under every page
-- ----------------------------------------------------------
SELECT
    r.item_name  AS report,
    p.item_name  AS page,
    t.item_name  AS table_name,
    t.layer,
    JSON_VALUE(t.properties_json, '$.row_count') AS row_count
FROM data_lineage.objects t
JOIN data_lineage.objects p ON p.objects_id = t.parent_objects_id AND p.objects_type = 'Page'
JOIN data_lineage.objects r ON r.objects_id = p.parent_objects_id AND r.objects_type = 'Report'
WHERE t.objects_type = 'Table'
ORDER BY r.item_name, p.item_name, t.item_name;


-- 4. Unconnected tables — tables that fell back to an artifact parent (no report)
-- ---------------------------------------------------------------------------------
SELECT
    a.item_name  AS artifact,
    t.item_name  AS table_name,
    t.layer
FROM data_lineage.objects t
JOIN data_lineage.objects a ON a.objects_id = t.parent_objects_id
WHERE t.objects_type = 'Table'
  AND a.objects_type IN ('Lakehouse', 'Warehouse')
ORDER BY a.item_name, t.item_name;


-- 5. Notebook lineage — what feeds what (relationships table)
-- ------------------------------------------------------------
SELECT
    src.item_name        AS source,
    src.objects_type     AS source_type,
    rel.relationships_type,
    tgt.item_name        AS target,
    tgt.layer            AS target_layer,
    rel.confidence_score,
    rel.detection_method,
    rel.transformation_expr
FROM data_lineage.relationships rel
JOIN data_lineage.objects src ON src.objects_id = rel.source_objects_id
JOIN data_lineage.objects tgt ON tgt.objects_id = rel.target_objects_id
ORDER BY rel.confidence_score DESC;


-- 6. Full lineage chain — Report → Page → Table → source notebook
-- -----------------------------------------------------------------
SELECT
    r.item_name   AS report,
    p.item_name   AS page,
    t.item_name   AS table_name,
    t.layer,
    src.item_name AS source_notebook,
    rel.confidence_score,
    rel.detection_method
FROM data_lineage.objects t
JOIN data_lineage.objects p  ON p.objects_id = t.parent_objects_id AND p.objects_type = 'Page'
JOIN data_lineage.objects r  ON r.objects_id = p.parent_objects_id AND r.objects_type = 'Report'
LEFT JOIN data_lineage.relationships rel ON rel.target_objects_id = t.objects_id
LEFT JOIN data_lineage.objects src ON src.objects_id = rel.source_objects_id
WHERE t.objects_type = 'Table'
ORDER BY r.item_name, p.item_name, t.item_name;
