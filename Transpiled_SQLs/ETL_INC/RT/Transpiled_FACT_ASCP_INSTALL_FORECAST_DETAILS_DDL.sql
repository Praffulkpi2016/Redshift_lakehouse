DROP table IF EXISTS BEC_DWH.FACT_ASCP_INSTALL_FORECAST_DETAILS;
CREATE TABLE BEC_DWH.FACT_ASCP_INSTALL_FORECAST_DETAILS (
  forecast_designator STRING,
  part_number STRING,
  organization_id DECIMAL(15, 0),
  forecast_date TIMESTAMP,
  current_forecast_quantity DECIMAL(28, 10),
  available_onhand_qty DECIMAL(28, 10),
  available_po_qty DECIMAL(28, 10),
  available_plan_qty DECIMAL(28, 10),
  remaining_onhand_qty DECIMAL(28, 10),
  remaining_po_qty DECIMAL(28, 10),
  remaining_plan_qty DECIMAL(28, 10),
  total_used_qty DECIMAL(28, 10)
);