DROP table IF EXISTS gold_bec_dwh.DIM_SEGMENT9_HCY;
CREATE TABLE gold_bec_dwh.DIM_SEGMENT9_HCY AS
(
  SELECT DISTINCT
    fnd.structure AS structure,
    l1_vsid AS l1_vsid,
    l1_value AS l1_value,
    l1_description AS l1_description,
    l2_value AS l2_value,
    l2_description AS l2_description,
    l3_value AS l3_value,
    l3_description AS l3_description,
    l4_value AS l4_value,
    l4_description AS l4_description,
    l5_value AS l5_value,
    l5_description AS l5_description,
    l6_value AS l6_value,
    l6_description AS l6_description,
    l7_value AS l7_value,
    l7_description AS l7_description,
    l8_value AS l8_value,
    l8_description AS l8_description,
    l9_value AS l9_value,
    l9_description AS l9_description,
    l10_value AS l10_value,
    l10_description AS l10_description,
    'N' AS is_deleted_flg, /* audit columns */
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(fnd.structure, '0') || '-' || COALESCE(l1_vsid, 0) || '-' || COALESCE(l1_value, '0') || '-' || COALESCE(l2_value, '0') || '-' || COALESCE(l3_value, '0') || '-' || COALESCE(l4_value, '0') || '-' || COALESCE(l5_value, '0') || '-' || COALESCE(l6_value, '0') || '-' || COALESCE(l7_value, '0') || '-' || COALESCE(l8_value, '0') || '-' || COALESCE(l9_value, '0') || '-' || COALESCE(l10_value, '0') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      fifs.flex_value_set_id AS vsid,
      fifst.id_flex_structure_code AS structure
    FROM (
      SELECT
        *
      FROM silver_bec_ods.fnd_id_flex_segments
      WHERE
        is_deleted_flg <> 'Y'
    ) AS fifs
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_id_flex_structures
      WHERE
        is_deleted_flg <> 'Y'
    ) AS fifst
      ON fifs.id_flex_num = fifst.id_flex_num
    WHERE
      fifs.id_flex_code = 'GL#' AND fifst.id_flex_code = 'GL#' AND fifs.segment_num = 9
  ) AS fnd
  JOIN (
    SELECT
      h1.flex_value_set_id AS l1_vsid,
      h1.parent_flex_value AS l1_value,
      ffvt.description AS l1_description,
      h1.child_flex_value AS l2_value,
      ffvt2.description AS l2_description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h2
      ON h1.parent_flex_value = h2.child_flex_value
      AND h1.flex_value_set_id = h2.flex_value_set_id
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv
      ON ffv.flex_value_set_id = h1.flex_value_set_id
      AND ffv.flex_value = h1.parent_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt
      ON ffvt.flex_value_id = ffv.flex_value_id AND ffvt.language = 'US'
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv2
      ON ffv2.flex_value_set_id = h1.flex_value_set_id
      AND ffv2.flex_value = h1.child_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt2
      ON ffvt2.flex_value_id = ffv2.flex_value_id AND ffvt2.language = 'US'
  )
    ON fnd.vsid = l1_vsid
  /* level 3 */
  LEFT OUTER JOIN (
    SELECT
      h1.flex_value_set_id AS l3_vsid,
      h1.parent_flex_value AS l3_parent,
      h1.child_flex_value AS l3_value,
      ffvt.description AS l3_description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h2
      ON h1.parent_flex_value = h2.child_flex_value
      AND h1.flex_value_set_id = h2.flex_value_set_id
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv
      ON ffv.flex_value_set_id = h1.flex_value_set_id AND ffv.flex_value = h1.child_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt
      ON ffvt.flex_value_id = ffv.flex_value_id AND ffvt.language = 'US'
  ) AS l3
    ON fnd.vsid = l3_vsid AND l2_value = l3_parent
  /* level 4 */
  LEFT OUTER JOIN (
    SELECT
      h1.flex_value_set_id AS l4_vsid,
      h1.parent_flex_value AS l4_parent,
      h1.child_flex_value AS l4_value,
      ffvt.description AS l4_description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h2
      ON h1.parent_flex_value = h2.child_flex_value
      AND h1.flex_value_set_id = h2.flex_value_set_id
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv
      ON ffv.flex_value_set_id = h1.flex_value_set_id AND ffv.flex_value = h1.child_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt
      ON ffvt.flex_value_id = ffv.flex_value_id AND ffvt.language = 'US'
  ) AS l4
    ON fnd.vsid = l4_vsid AND l3_value = l4_parent
  /* level 5 */
  LEFT OUTER JOIN (
    SELECT
      h1.flex_value_set_id AS l5_vsid,
      h1.parent_flex_value AS l5_parent,
      h1.child_flex_value AS l5_value,
      ffvt.description AS l5_description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h2
      ON h1.parent_flex_value = h2.child_flex_value
      AND h1.flex_value_set_id = h2.flex_value_set_id
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv
      ON ffv.flex_value_set_id = h1.flex_value_set_id AND ffv.flex_value = h1.child_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt
      ON ffvt.flex_value_id = ffv.flex_value_id AND ffvt.language = 'US'
  ) AS l5
    ON fnd.vsid = l5_vsid AND l4_value = l5_parent
  /* level 6 */
  LEFT OUTER JOIN (
    SELECT
      h1.flex_value_set_id AS l6_vsid,
      h1.parent_flex_value AS l6_parent,
      h1.child_flex_value AS l6_value,
      ffvt.description AS l6_description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h2
      ON h1.parent_flex_value = h2.child_flex_value
      AND h1.flex_value_set_id = h2.flex_value_set_id
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv
      ON ffv.flex_value_set_id = h1.flex_value_set_id AND ffv.flex_value = h1.child_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt
      ON ffvt.flex_value_id = ffv.flex_value_id AND ffvt.language = 'US'
  ) AS l6
    ON fnd.vsid = l6_vsid AND l5_value = l6_parent
  /* level 7 */
  LEFT OUTER JOIN (
    SELECT
      h1.flex_value_set_id AS l7_vsid,
      h1.parent_flex_value AS l7_parent,
      h1.child_flex_value AS l7_value,
      ffvt.description AS l7_description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h2
      ON h1.parent_flex_value = h2.child_flex_value
      AND h1.flex_value_set_id = h2.flex_value_set_id
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv
      ON ffv.flex_value_set_id = h1.flex_value_set_id AND ffv.flex_value = h1.child_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt
      ON ffvt.flex_value_id = ffv.flex_value_id AND ffvt.language = 'US'
  ) AS l7
    ON fnd.vsid = l7_vsid AND l6_value = l7_parent
  /* level 8 */
  LEFT OUTER JOIN (
    SELECT
      h1.flex_value_set_id AS l8_vsid,
      h1.parent_flex_value AS l8_parent,
      h1.child_flex_value AS l8_value,
      ffvt.description AS l8_description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h2
      ON h1.parent_flex_value = h2.child_flex_value
      AND h1.flex_value_set_id = h2.flex_value_set_id
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv
      ON ffv.flex_value_set_id = h1.flex_value_set_id AND ffv.flex_value = h1.child_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt
      ON ffvt.flex_value_id = ffv.flex_value_id AND ffvt.language = 'US'
  ) AS l8
    ON fnd.vsid = l8_vsid AND l7_value = l8_parent
  /* level 9 */
  LEFT OUTER JOIN (
    SELECT
      h1.flex_value_set_id AS l9_vsid,
      h1.parent_flex_value AS l9_parent,
      h1.child_flex_value AS l9_value,
      ffvt.description AS l9_description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h2
      ON h1.parent_flex_value = h2.child_flex_value
      AND h1.flex_value_set_id = h2.flex_value_set_id
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv
      ON ffv.flex_value_set_id = h1.flex_value_set_id AND ffv.flex_value = h1.child_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt
      ON ffvt.flex_value_id = ffv.flex_value_id AND ffvt.language = 'US'
  ) AS l9
    ON fnd.vsid = l9_vsid AND l8_value = l9_parent
  /* level 10 */
  LEFT OUTER JOIN (
    SELECT
      h1.flex_value_set_id AS l10_vsid,
      h1.parent_flex_value AS l10_parent,
      h1.child_flex_value AS l10_value,
      ffvt.description AS l10_description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS h2
      ON h1.parent_flex_value = h2.child_flex_value
      AND h1.flex_value_set_id = h2.flex_value_set_id
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffv
      ON ffv.flex_value_set_id = h1.flex_value_set_id AND ffv.flex_value = h1.child_flex_value
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ffvt
      ON ffvt.flex_value_id = ffv.flex_value_id AND ffvt.language = 'US'
  ) AS l10
    ON fnd.vsid = l10_vsid AND l9_value = l10_parent
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_segment9_hcy' AND batch_name = 'gl';