/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;

truncate table bec_dwh.FACT_ASCP_HP_ITEMS_STG;

insert into bec_dwh.fact_ascp_hp_items_stg
(
SELECT DISTINCT
    inventory_item_id,
    sr_instance_id,
    plan_id,
    organization_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id as organization_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || sr_instance_id as sr_instance_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || plan_id as plan_id_key,
	-- audit columns
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || nvl(inventory_item_id, 0)|| '-' || nvl(sr_instance_id, 0)|| '-' || nvl(plan_id, 0)|| '-' || nvl(organization_id, 0) as dw_load_id, 
	getdate() as dw_insert_date,
	getdate() as dw_update_date 
FROM
    bec_dwh.fact_ascp_horizontal_plan
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ascp_hp_items_stg'
	and batch_name = 'ascp';