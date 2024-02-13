create or replace
procedure fact_install_forecast_details_snp_proc(v_current_date date)
as $$

begin
execute 'delete from bec_install.fact_install_forecast_details_snp where batch_id =
(select distinct batch_id from bec_install.fact_install_forecast_details)';

execute 'INSERT INTO bec_install.fact_install_forecast_details_snp
(batch_id,	
	forecast_designator,
	part_number,
	organization_id,
	forecast_date,
	current_forecast_quantity,
	available_onhand_qty,
	available_po_qty,
	available_plan_qty,
	remaining_onhand_qty,
	remaining_po_qty,
	remaining_plan_qty,
	total_used_qty,
    snp_date)
(select batch_id,	
	forecast_designator,
	part_number,
	organization_id,
	forecast_date,
	current_forecast_quantity,
	available_onhand_qty,
	available_po_qty,
	available_plan_qty,
	remaining_onhand_qty,
	remaining_po_qty,
	remaining_plan_qty,
	total_used_qty,'
	||quote_literal(v_current_date)|| ' from bec_install.fact_install_forecast_details)';
	
-- Purge the Snapshot data which exceeds 90 days and keep only last date of every month.

/*execute 'delete from bec_install.fact_install_forecast_details_snp where 
 snp_date in (SELECT
  distinct snp_date
FROM
  bec_install.fact_install_forecast_details_snp
WHERE
  snp_date not IN (
    SELECT   MAX(snp_date)
    FROM     bec_install.fact_install_forecast_details_snp
    GROUP BY date_part(month,snp_date), date_part(year,snp_date)
  )) and snp_date <'|| quote_literal(v_current_date)||'-90';*/

end;

$$ language plpgsql;