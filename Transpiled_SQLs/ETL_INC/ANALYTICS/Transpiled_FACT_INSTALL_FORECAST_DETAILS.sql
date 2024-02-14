TRUNCATE table bec_install.fact_install_forecast_details;
CALL populate_forecast_details();
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP(), load_type = 'I'
WHERE
  dw_table_name = 'fact_install_forecast_details' AND batch_name = 'install';