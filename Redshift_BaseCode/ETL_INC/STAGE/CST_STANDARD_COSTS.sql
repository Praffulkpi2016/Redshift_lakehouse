/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.CST_STANDARD_COSTS;

insert into	bec_ods_stg.CST_STANDARD_COSTS
   (cost_update_id,
	inventory_item_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	standard_cost_revision_date,
	standard_cost,
	inventory_adjustment_quantity,
	inventory_adjustment_value,
	intransit_adjustment_quantity,
	intransit_adjustment_value,
	wip_adjustment_quantity,
	wip_adjustment_value,
	last_cost_update_id,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		cost_update_id,
	inventory_item_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	standard_cost_revision_date,
	standard_cost,
	inventory_adjustment_quantity,
	inventory_adjustment_value,
	intransit_adjustment_quantity,
	intransit_adjustment_value,
	wip_adjustment_quantity,
	wip_adjustment_value,
	last_cost_update_id,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.CST_STANDARD_COSTS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,kca_seq_id) in 
	(select COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,max(kca_seq_id) from bec_raw_dl_ext.CST_STANDARD_COSTS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cst_standard_costs')
);
end;