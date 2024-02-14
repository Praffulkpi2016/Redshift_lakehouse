CALL fact_ascp_horizontal_plan_snp_proc(to_date(getdate(), 'yyyy-mm-dd'));
UPDATE gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN_SNP SET plan_name_archive_date = (
  SELECT DISTINCT
    plans.compile_designator || '-' || FACT_ASCP_HORIZONTAL_PLAN_SNP.snp_date
  FROM BEC_dwh.DIM_ASCP_PLANS AS PLANS
  WHERE
    FACT_ASCP_HORIZONTAL_PLAN_SNP.plan_id = PLANS.plan_id
    AND FACT_ASCP_HORIZONTAL_PLAN_SNP.sr_instance_id = PLANS.sr_instance_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP(), load_type = 'I'
WHERE
  dw_table_name = 'fact_ascp_horizontal_plan_snp' AND batch_name = 'ascp';