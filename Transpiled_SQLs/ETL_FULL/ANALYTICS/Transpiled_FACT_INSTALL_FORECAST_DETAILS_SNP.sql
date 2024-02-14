CALL fact_install_forecast_details_snp_proc(to_date(getdate(), 'yyyy-mm-dd'));
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP(), load_type = 'I'
WHERE
  dw_table_name = 'fact_install_forecast_details_snp' AND batch_name = 'install';