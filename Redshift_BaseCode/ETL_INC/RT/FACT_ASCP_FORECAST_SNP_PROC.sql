create or replace
procedure fact_ascp_forecast_snp_proc(v_current_date date)
as $$

begin 
execute 'delete from bec_dwh.fact_ascp_forecast_snp where snp_date =' || quote_literal(v_current_date);

execute 'INSERT INTO bec_dwh.fact_ascp_forecast_snp
(
organization_id
,inventory_item_id
,transaction_id
,part_number
,item_description
,primary_uom_code
,bom_item_type_desc
,ato_forecast_control_desc
,mrp_planning_code_desc
,pick_components_flag
,forecast_designator
,bucket_type
,forecast_date
,current_forecast_quantity
,original_forecast_quantity
,comments
,header_attribute1
,line_attribute1
,datestamp
,source_app_id
,dw_load_id
,snp_date
)
(select 
organization_id
,inventory_item_id
,transaction_id
,part_number
,item_description
,primary_uom_code
,bom_item_type_desc
,ato_forecast_control_desc
,mrp_planning_code_desc
,pick_components_flag
,forecast_designator
,bucket_type
,forecast_date
,current_forecast_quantity
,original_forecast_quantity
,comments
,header_attribute1
,line_attribute1
,datestamp
,source_app_id
,dw_load_id,'
    ||quote_literal(v_current_date)|| ' from bec_dwh.fact_ascp_forecast
)';
	
-- Purge the Snapshot data which exceeds 90 days and keep only last date of every month.

execute 'delete from bec_dwh.fact_ascp_forecast_snp where 
 snp_date in (SELECT
  distinct snp_date
FROM
  bec_dwh.fact_ascp_forecast_snp
WHERE
  snp_date not IN (
    SELECT   MAX(snp_date)
    FROM     bec_dwh.fact_ascp_forecast_snp
    GROUP BY date_part(month,snp_date), date_part(year,snp_date)
  )) and snp_date <'|| quote_literal(v_current_date)||'-90';

end;

$$ language plpgsql;