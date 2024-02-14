/* Delete Records */
DELETE FROM silver_bec_ods.XXBEC_PEGGING_DATA
WHERE
  (COALESCE(LEVEL_SEQ, 0), COALESCE(PEGGING_ID, 0), COALESCE(MSC_TRXN_ID, 0), COALESCE(PARENT_PEGGING_ID, 0), COALESCE(TRANSACTION_ID, 0), COALESCE(PLAN_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(ITEM_ID, 0)) IN (
    SELECT
      COALESCE(stg.LEVEL_SEQ, 0),
      COALESCE(stg.PEGGING_ID, 0),
      COALESCE(stg.MSC_TRXN_ID, 0),
      COALESCE(stg.PARENT_PEGGING_ID, 0),
      COALESCE(stg.TRANSACTION_ID, 0),
      COALESCE(stg.PLAN_ID, 0),
      COALESCE(stg.ORGANIZATION_ID, 0),
      COALESCE(stg.ITEM_ID, 0)
    FROM silver_bec_ods.xxbec_pegging_data AS ods, bronze_bec_ods_stg.xxbec_pegging_data AS stg
    WHERE
      ods.LEVEL_SEQ = stg.LEVEL_SEQ
      AND ods.PEGGING_ID = stg.PEGGING_ID
      AND ods.MSC_TRXN_ID = stg.MSC_TRXN_ID
      AND ods.PARENT_PEGGING_ID = stg.PARENT_PEGGING_ID
      AND ods.TRANSACTION_ID = stg.TRANSACTION_ID
      AND ods.PLAN_ID = stg.PLAN_ID
      AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND ods.ITEM_ID = stg.ITEM_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.xxbec_pegging_data (
  level_seq,
  plan_id,
  organization_id,
  pegging_id,
  prev_pegging_id,
  parent_pegging_id,
  demand_id,
  transaction_id,
  msc_trxn_id,
  demand_qty,
  demand_date,
  item_id,
  item_org,
  pegged_qty,
  origination_name,
  end_pegged_qty,
  end_demand_qty,
  end_demand_date,
  end_item_org,
  end_origination_name,
  end_satisfied_date,
  end_disposition,
  order_type,
  end_demand_class,
  sr_instance_id,
  op_seq_num,
  item_desc_org,
  new_order_date,
  new_dock_date,
  new_start_date,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    level_seq,
    plan_id,
    organization_id,
    pegging_id,
    prev_pegging_id,
    parent_pegging_id,
    demand_id,
    transaction_id,
    msc_trxn_id,
    demand_qty,
    demand_date,
    item_id,
    item_org,
    pegged_qty,
    origination_name,
    end_pegged_qty,
    end_demand_qty,
    end_demand_date,
    end_item_org,
    end_origination_name,
    end_satisfied_date,
    end_disposition,
    order_type,
    end_demand_class,
    sr_instance_id,
    op_seq_num,
    item_desc_org,
    new_order_date,
    new_dock_date,
    new_start_date,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.xxbec_pegging_data
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(LEVEL_SEQ, 0), COALESCE(PEGGING_ID, 0), COALESCE(MSC_TRXN_ID, 0), COALESCE(PARENT_PEGGING_ID, 0), COALESCE(TRANSACTION_ID, 0), COALESCE(PLAN_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(ITEM_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(LEVEL_SEQ, 0),
        COALESCE(PEGGING_ID, 0),
        COALESCE(MSC_TRXN_ID, 0),
        COALESCE(PARENT_PEGGING_ID, 0),
        COALESCE(TRANSACTION_ID, 0),
        COALESCE(PLAN_ID, 0),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(ITEM_ID, 0),
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.xxbec_pegging_data
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(LEVEL_SEQ, 0),
        COALESCE(PEGGING_ID, 0),
        COALESCE(MSC_TRXN_ID, 0),
        COALESCE(PARENT_PEGGING_ID, 0),
        COALESCE(TRANSACTION_ID, 0),
        COALESCE(PLAN_ID, 0),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(ITEM_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.xxbec_pegging_data SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.xxbec_pegging_data SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(LEVEL_SEQ, 0), COALESCE(PEGGING_ID, 0), COALESCE(MSC_TRXN_ID, 0), COALESCE(PARENT_PEGGING_ID, 0), COALESCE(TRANSACTION_ID, 0), COALESCE(PLAN_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(ITEM_ID, 0)) IN (
    SELECT
      COALESCE(LEVEL_SEQ, 0),
      COALESCE(PEGGING_ID, 0),
      COALESCE(MSC_TRXN_ID, 0),
      COALESCE(PARENT_PEGGING_ID, 0),
      COALESCE(TRANSACTION_ID, 0),
      COALESCE(PLAN_ID, 0),
      COALESCE(ORGANIZATION_ID, 0),
      COALESCE(ITEM_ID, 0)
    FROM bec_raw_dl_ext.xxbec_pegging_data
    WHERE
      (COALESCE(LEVEL_SEQ, 0), COALESCE(PEGGING_ID, 0), COALESCE(MSC_TRXN_ID, 0), COALESCE(PARENT_PEGGING_ID, 0), COALESCE(TRANSACTION_ID, 0), COALESCE(PLAN_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(ITEM_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(LEVEL_SEQ, 0),
          COALESCE(PEGGING_ID, 0),
          COALESCE(MSC_TRXN_ID, 0),
          COALESCE(PARENT_PEGGING_ID, 0),
          COALESCE(TRANSACTION_ID, 0),
          COALESCE(PLAN_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(ITEM_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.xxbec_pegging_data
        GROUP BY
          COALESCE(LEVEL_SEQ, 0),
          COALESCE(PEGGING_ID, 0),
          COALESCE(MSC_TRXN_ID, 0),
          COALESCE(PARENT_PEGGING_ID, 0),
          COALESCE(TRANSACTION_ID, 0),
          COALESCE(PLAN_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(ITEM_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xxbec_pegging_data';