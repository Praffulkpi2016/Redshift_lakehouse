TRUNCATE table bronze_bec_ods_stg.xxbec_pegging_data;
INSERT INTO bronze_bec_ods_stg.xxbec_pegging_data (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.XXBEC_PEGGING_DATA
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (LEVEL_SEQ, PEGGING_ID, MSC_TRXN_ID, PARENT_PEGGING_ID, TRANSACTION_ID, PLAN_ID, ORGANIZATION_ID, ITEM_ID, kca_seq_id) IN (
      SELECT
        LEVEL_SEQ,
        PEGGING_ID,
        MSC_TRXN_ID,
        PARENT_PEGGING_ID,
        TRANSACTION_ID,
        PLAN_ID,
        ORGANIZATION_ID,
        ITEM_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.xxbec_pegging_data
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        LEVEL_SEQ,
        PEGGING_ID,
        MSC_TRXN_ID,
        PARENT_PEGGING_ID,
        TRANSACTION_ID,
        PLAN_ID,
        ORGANIZATION_ID,
        ITEM_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'xxbec_pegging_data'
    )
);