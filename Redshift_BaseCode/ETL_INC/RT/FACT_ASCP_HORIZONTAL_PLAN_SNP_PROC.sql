create or replace
procedure FACT_ASCP_HORIZONTAL_PLAN_SNP_PROC(v_current_date date)
as $$

begin 
execute 'delete from bec_dwh.FACT_ASCP_HORIZONTAL_PLAN_SNP where snp_date =' || quote_literal(v_current_date);

execute 'INSERT INTO bec_dwh.FACT_ASCP_HORIZONTAL_PLAN_SNP
(
inventory_item_id,
organization_id,
plan_id,
category_set_id,
sr_instance_id,
fill_kill_flag,
so_line_split,
quantity_rate,
new_due_date,
order_type_entity,
plan_id_key,
organization_id_key,
inventory_item_id_key,
is_deleted_flg,
source_app_id,
dw_load_id
,snp_date
)
(
select
	inventory_item_id,
	organization_id,
	plan_id,
	category_set_id,
	sr_instance_id,
	fill_kill_flag,
	so_line_split,
	quantity_rate,
	new_due_date,
	order_type_entity,
	plan_id_key,
	organization_id_key,
	inventory_item_id_key,
	is_deleted_flg,
	source_app_id,
	dw_load_id,'
    ||quote_literal(v_current_date)|| ' from bec_dwh.FACT_ASCP_HORIZONTAL_PLAN
)';
	
-- Purge the Snapshot data which exceeds 90 days and keep only last date of every month.

execute 'delete from bec_dwh.FACT_ASCP_HORIZONTAL_PLAN_SNP where 
 snp_date in (SELECT
  distinct snp_date
FROM
  bec_dwh.FACT_ASCP_HORIZONTAL_PLAN_SNP
WHERE
  snp_date not IN (
    SELECT   MAX(snp_date)
    FROM     bec_dwh.FACT_ASCP_HORIZONTAL_PLAN_SNP
    GROUP BY date_part(month,snp_date), date_part(year,snp_date)
  )) and snp_date <'|| quote_literal(v_current_date)||'-90';

end;

$$ language plpgsql;