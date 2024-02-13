/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.FACT_CYCLE_COUNT_ENTRY;

create table bec_dwh.FACT_CYCLE_COUNT_ENTRY diststyle all sortkey(cycle_count_entry_id) as 
SELECT 
	cce.cycle_count_header_id, 
	cycle_count_entry_id, 
	cch.cycle_count_header_name, 
	CCH.ABC_ASSIGNMENT_GROUP_ID, 
	cch.organization_id, 
	msi.inventory_item_id,
	cce.creation_date, 
	cce.last_update_date, 
	count_list_sequence, 
	msi.segment1, 
	msi.description, 
	subinventory, 
	cce.locator_id ,
	revision, 
	lot_number, 
	lot_control, 
	approval_type, 
	serial_detail, 
	count_quantity_current, 
	system_quantity_current, 
	adjustment_quantity, 
	adjustment_date, 
	adjustment_amount, 
	item_unit_cost, 
	nvl(count_quantity_current, 0)* nvl(item_unit_cost, 0) extended_cost, 
	approval_date, 
	TO_CHAR (approval_date, 'YYYY') Approval_year, 
	TO_CHAR (approval_date, 'WW') work_week, 
	cce.counted_by_employee_id_current, 
	approver_employee_id, 
	DECODE (adjustment_quantity, 0, 1, 0) hit_miss_percent, 
	cce.transaction_reason_id, 
	(
		SELECT 
		reason_name 
		FROM 
		bec_ods.mtl_transaction_reasons
		WHERE is_deleted_flg <> 'Y'
		and reason_id = cce.transaction_reason_id
	) transaction_reason, 
	cce.reference_current, 
	cce.reference_first, 
	cce.reference_prior,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cce.cycle_count_header_id   cycle_count_header_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cycle_count_entry_id   cycle_count_entry_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||CCH.ABC_ASSIGNMENT_GROUP_ID   ABC_ASSIGNMENT_GROUP_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cch.organization_id   organization_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cce.transaction_reason_id   transaction_reason_id_KEY,
	'N' as is_deleted_flg, 
	(
	select 
		system_id 
	from 
		bec_etl_ctrl.etlsourceappid 
	where 
		source_system = 'EBS'
	) as source_app_id, 
	(
	select 
		system_id 
	from 
		bec_etl_ctrl.etlsourceappid 
	where 
		source_system = 'EBS'
	) || '-' || nvl(cycle_count_entry_id, 0) as dw_load_id,
	getdate() as dw_insert_date, 
	getdate() as dw_update_date 
FROM 
	(select * from bec_ods.mtl_cycle_count_entries where is_deleted_flg <> 'Y') cce, 
	(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
	(select * from bec_ods.mtl_cycle_count_headers where is_deleted_flg <> 'Y') cch 
WHERE 
	entry_status_code = 5 
	AND cce.inventory_item_id = msi.inventory_item_id 
	AND msi.organization_id = 90 
	AND cce.cycle_count_header_id = cch.cycle_count_header_id
;
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_cycle_count_entry' 
  and batch_name = 'inv';
commit;