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

truncate table bec_ods_stg.CST_COST_UPDATES;

insert into	bec_ods_stg.CST_COST_UPDATES
   (
	cost_update_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	status,
	organization_id,
	cost_type_id,
	update_date,
	description,
	range_option,
	update_resource_ovhd_flag,
	update_activity_flag,
	snapshot_saved_flag,
	inv_adjustment_account,
	single_item,
	item_range_low,
	item_range_high,
	category_id,
	category_set_id,
	inventory_adjustment_value,
	intransit_adjustment_value,
	wip_adjustment_value,
	scrap_adjustment_value,
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
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	status,
	organization_id,
	cost_type_id,
	update_date,
	description,
	range_option,
	update_resource_ovhd_flag,
	update_activity_flag,
	snapshot_saved_flag,
	inv_adjustment_account,
	single_item,
	item_range_low,
	item_range_high,
	category_id,
	category_set_id,
	inventory_adjustment_value,
	intransit_adjustment_value,
	wip_adjustment_value,
	scrap_adjustment_value,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.CST_COST_UPDATES
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(COST_UPDATE_ID,0)  ,kca_seq_id) in 
	(select nvl(COST_UPDATE_ID,0) as COST_UPDATE_ID ,max(kca_seq_id) from bec_raw_dl_ext.CST_COST_UPDATES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(COST_UPDATE_ID,0) )
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cst_cost_updates')
);
end;