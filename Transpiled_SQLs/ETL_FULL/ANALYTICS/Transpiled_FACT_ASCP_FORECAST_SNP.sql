CALL fact_ascp_forecast_snp_proc(to_date(getdate(), 'yyyy-mm-dd'));
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_forecast_snp' AND batch_name = 'ascp';