DROP TABLE IF EXISTS gold_bec_dwh.DIM_SEGMENT4_HCY;
CREATE TABLE gold_bec_dwh.DIM_SEGMENT4_HCY AS
(
  SELECT DISTINCT
    FND.STRUCTURE AS STRUCTURE,
    L1_VSID AS L1_VSID,
    L1_VALUE AS L1_VALUE,
    L1_DESCRIPTION AS L1_DESCRIPTION,
    L2_VALUE AS L2_VALUE,
    L2_DESCRIPTION AS L2_DESCRIPTION,
    L3_VALUE AS L3_VALUE,
    L3_DESCRIPTION AS L3_DESCRIPTION,
    L4_VALUE AS L4_VALUE,
    L4_DESCRIPTION AS L4_DESCRIPTION,
    L5_VALUE AS L5_VALUE,
    L5_DESCRIPTION AS L5_DESCRIPTION,
    L6_VALUE AS L6_VALUE,
    L6_DESCRIPTION AS L6_DESCRIPTION,
    L7_VALUE AS L7_VALUE,
    L7_DESCRIPTION AS L7_DESCRIPTION,
    L8_VALUE AS L8_VALUE,
    L8_DESCRIPTION AS L8_DESCRIPTION,
    L9_VALUE AS L9_VALUE,
    L9_DESCRIPTION AS L9_DESCRIPTION,
    L10_VALUE AS L10_VALUE,
    L10_DESCRIPTION AS L10_DESCRIPTION,
    'N' AS IS_DELETED_FLG, /* AUDIT COLUMNS */
    (
      SELECT
        SYSTEM_ID
      FROM bec_etl_ctrl.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) AS SOURCE_APP_ID,
    (
      SELECT
        SYSTEM_ID
      FROM bec_etl_ctrl.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || COALESCE(FND.STRUCTURE, '0') || '-' || COALESCE(L1_VSID, 0) || '-' || COALESCE(L1_VALUE, '0') || '-' || COALESCE(L2_VALUE, '0') || '-' || COALESCE(L3_VALUE, '0') || '-' || COALESCE(L4_VALUE, '0') || '-' || COALESCE(L5_VALUE, '0') || '-' || COALESCE(L6_VALUE, '0') || '-' || COALESCE(L7_VALUE, '0') || '-' || COALESCE(L8_VALUE, '0') || '-' || COALESCE(L9_VALUE, '0') || '-' || COALESCE(L10_VALUE, '0') AS DW_LOAD_ID,
    CURRENT_TIMESTAMP() AS DW_INSERT_DATE,
    CURRENT_TIMESTAMP() AS DW_UPDATE_DATE
  FROM (
    SELECT
      FIFS.FLEX_VALUE_SET_ID AS VSID,
      FIFST.ID_FLEX_STRUCTURE_CODE AS STRUCTURE
    FROM (
      SELECT
        *
      FROM silver_bec_ods.fnd_id_flex_segments
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FIFS
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_id_flex_structures
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FIFST
      ON FIFS.ID_FLEX_NUM = FIFST.ID_FLEX_NUM
    WHERE
      FIFS.ID_FLEX_CODE = 'GL#' AND FIFST.ID_FLEX_CODE = 'GL#' AND FIFS.SEGMENT_NUM = 4
  ) AS FND
  JOIN (
    SELECT
      H1.FLEX_VALUE_SET_ID AS L1_VSID,
      H1.PARENT_FLEX_VALUE AS L1_VALUE,
      FFVT.DESCRIPTION AS L1_DESCRIPTION,
      H1.CHILD_FLEX_VALUE AS L2_VALUE,
      FFVT2.DESCRIPTION AS L2_DESCRIPTION
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H2
      ON H1.PARENT_FLEX_VALUE = H2.CHILD_FLEX_VALUE
      AND H1.FLEX_VALUE_SET_ID = H2.FLEX_VALUE_SET_ID
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV
      ON FFV.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID
      AND FFV.FLEX_VALUE = H1.PARENT_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT
      ON FFVT.FLEX_VALUE_ID = FFV.FLEX_VALUE_ID AND FFVT.LANGUAGE = 'US'
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV2
      ON FFV2.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID
      AND FFV2.FLEX_VALUE = H1.CHILD_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT2
      ON FFVT2.FLEX_VALUE_ID = FFV2.FLEX_VALUE_ID AND FFVT2.LANGUAGE = 'US'
  )
    ON FND.VSID = L1_VSID
  /* LEVEL 3 */
  LEFT OUTER JOIN (
    SELECT
      H1.FLEX_VALUE_SET_ID AS L3_VSID,
      H1.PARENT_FLEX_VALUE AS L3_PARENT,
      H1.CHILD_FLEX_VALUE AS L3_VALUE,
      FFVT.DESCRIPTION AS L3_DESCRIPTION
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H2
      ON H1.PARENT_FLEX_VALUE = H2.CHILD_FLEX_VALUE
      AND H1.FLEX_VALUE_SET_ID = H2.FLEX_VALUE_SET_ID
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV
      ON FFV.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID AND FFV.FLEX_VALUE = H1.CHILD_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT
      ON FFVT.FLEX_VALUE_ID = FFV.FLEX_VALUE_ID AND FFVT.LANGUAGE = 'US'
  ) AS L3
    ON FND.VSID = L3_VSID AND L2_VALUE = L3_PARENT
  /* LEVEL 4 */
  LEFT OUTER JOIN (
    SELECT
      H1.FLEX_VALUE_SET_ID AS L4_VSID,
      H1.PARENT_FLEX_VALUE AS L4_PARENT,
      H1.CHILD_FLEX_VALUE AS L4_VALUE,
      FFVT.DESCRIPTION AS L4_DESCRIPTION
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H2
      ON H1.PARENT_FLEX_VALUE = H2.CHILD_FLEX_VALUE
      AND H1.FLEX_VALUE_SET_ID = H2.FLEX_VALUE_SET_ID
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV
      ON FFV.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID AND FFV.FLEX_VALUE = H1.CHILD_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT
      ON FFVT.FLEX_VALUE_ID = FFV.FLEX_VALUE_ID AND FFVT.LANGUAGE = 'US'
  ) AS L4
    ON FND.VSID = L4_VSID AND L3_VALUE = L4_PARENT
  /* LEVEL 5 */
  LEFT OUTER JOIN (
    SELECT
      H1.FLEX_VALUE_SET_ID AS L5_VSID,
      H1.PARENT_FLEX_VALUE AS L5_PARENT,
      H1.CHILD_FLEX_VALUE AS L5_VALUE,
      FFVT.DESCRIPTION AS L5_DESCRIPTION
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H2
      ON H1.PARENT_FLEX_VALUE = H2.CHILD_FLEX_VALUE
      AND H1.FLEX_VALUE_SET_ID = H2.FLEX_VALUE_SET_ID
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV
      ON FFV.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID AND FFV.FLEX_VALUE = H1.CHILD_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT
      ON FFVT.FLEX_VALUE_ID = FFV.FLEX_VALUE_ID AND FFVT.LANGUAGE = 'US'
  ) AS L5
    ON FND.VSID = L5_VSID AND L4_VALUE = L5_PARENT
  /* LEVEL 6 */
  LEFT OUTER JOIN (
    SELECT
      H1.FLEX_VALUE_SET_ID AS L6_VSID,
      H1.PARENT_FLEX_VALUE AS L6_PARENT,
      H1.CHILD_FLEX_VALUE AS L6_VALUE,
      FFVT.DESCRIPTION AS L6_DESCRIPTION
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H2
      ON H1.PARENT_FLEX_VALUE = H2.CHILD_FLEX_VALUE
      AND H1.FLEX_VALUE_SET_ID = H2.FLEX_VALUE_SET_ID
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV
      ON FFV.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID AND FFV.FLEX_VALUE = H1.CHILD_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT
      ON FFVT.FLEX_VALUE_ID = FFV.FLEX_VALUE_ID AND FFVT.LANGUAGE = 'US'
  ) AS L6
    ON FND.VSID = L6_VSID AND L5_VALUE = L6_PARENT
  /* LEVEL 7 */
  LEFT OUTER JOIN (
    SELECT
      H1.FLEX_VALUE_SET_ID AS L7_VSID,
      H1.PARENT_FLEX_VALUE AS L7_PARENT,
      H1.CHILD_FLEX_VALUE AS L7_VALUE,
      FFVT.DESCRIPTION AS L7_DESCRIPTION
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H2
      ON H1.PARENT_FLEX_VALUE = H2.CHILD_FLEX_VALUE
      AND H1.FLEX_VALUE_SET_ID = H2.FLEX_VALUE_SET_ID
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV
      ON FFV.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID AND FFV.FLEX_VALUE = H1.CHILD_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT
      ON FFVT.FLEX_VALUE_ID = FFV.FLEX_VALUE_ID AND FFVT.LANGUAGE = 'US'
  ) AS L7
    ON FND.VSID = L7_VSID AND L6_VALUE = L7_PARENT
  /* LEVEL 8 */
  LEFT OUTER JOIN (
    SELECT
      H1.FLEX_VALUE_SET_ID AS L8_VSID,
      H1.PARENT_FLEX_VALUE AS L8_PARENT,
      H1.CHILD_FLEX_VALUE AS L8_VALUE,
      FFVT.DESCRIPTION AS L8_DESCRIPTION
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H2
      ON H1.PARENT_FLEX_VALUE = H2.CHILD_FLEX_VALUE
      AND H1.FLEX_VALUE_SET_ID = H2.FLEX_VALUE_SET_ID
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV
      ON FFV.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID AND FFV.FLEX_VALUE = H1.CHILD_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT
      ON FFVT.FLEX_VALUE_ID = FFV.FLEX_VALUE_ID AND FFVT.LANGUAGE = 'US'
  ) AS L8
    ON FND.VSID = L8_VSID AND L7_VALUE = L8_PARENT
  /* LEVEL 9 */
  LEFT OUTER JOIN (
    SELECT
      H1.FLEX_VALUE_SET_ID AS L9_VSID,
      H1.PARENT_FLEX_VALUE AS L9_PARENT,
      H1.CHILD_FLEX_VALUE AS L9_VALUE,
      FFVT.DESCRIPTION AS L9_DESCRIPTION
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H2
      ON H1.PARENT_FLEX_VALUE = H2.CHILD_FLEX_VALUE
      AND H1.FLEX_VALUE_SET_ID = H2.FLEX_VALUE_SET_ID
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV
      ON FFV.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID AND FFV.FLEX_VALUE = H1.CHILD_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT
      ON FFVT.FLEX_VALUE_ID = FFV.FLEX_VALUE_ID AND FFVT.LANGUAGE = 'US'
  ) AS L9
    ON FND.VSID = L9_VSID AND L8_VALUE = L9_PARENT
  /* LEVEL 10 */
  LEFT OUTER JOIN (
    SELECT
      H1.FLEX_VALUE_SET_ID AS L10_VSID,
      H1.PARENT_FLEX_VALUE AS L10_PARENT,
      H1.CHILD_FLEX_VALUE AS L10_VALUE,
      FFVT.DESCRIPTION AS L10_DESCRIPTION
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H1
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_seg_val_norm_hierarchy
      WHERE
        is_deleted_flg <> 'Y'
    ) AS H2
      ON H1.PARENT_FLEX_VALUE = H2.CHILD_FLEX_VALUE
      AND H1.FLEX_VALUE_SET_ID = H2.FLEX_VALUE_SET_ID
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFV
      ON FFV.FLEX_VALUE_SET_ID = H1.FLEX_VALUE_SET_ID AND FFV.FLEX_VALUE = H1.CHILD_FLEX_VALUE
    JOIN (
      SELECT
        *
      FROM silver_bec_ods.fnd_flex_values_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FFVT
      ON FFVT.FLEX_VALUE_ID = FFV.FLEX_VALUE_ID AND FFVT.LANGUAGE = 'US'
  ) AS L10
    ON FND.VSID = L10_VSID AND L9_VALUE = L10_PARENT
);
UPDATE BEC_ETL_CTRL.BATCH_DW_INFO SET LOAD_TYPE = 'I', LAST_REFRESH_DATE = CURRENT_TIMESTAMP()
WHERE
  DW_TABLE_NAME = 'dim_segment4_hcy' AND BATCH_NAME = 'gl';