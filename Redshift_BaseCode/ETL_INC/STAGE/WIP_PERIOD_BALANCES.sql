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

truncate
	table bec_ods_stg.WIP_PERIOD_BALANCES;

insert
	into
	bec_ods_stg.WIP_PERIOD_BALANCES
   (acct_period_id,
	wip_entity_id,
	repetitive_schedule_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	organization_id,
	class_type,
	tl_resource_in,
	tl_overhead_in,
	tl_outside_processing_in,
	pl_material_in,
	pl_material_overhead_in,
	pl_resource_in,
	pl_overhead_in,
	pl_outside_processing_in,
	tl_material_out,
	tl_material_overhead_out,
	tl_resource_out,
	tl_overhead_out,
	tl_outside_processing_out,
	pl_material_out,
	pl_material_overhead_out,
	pl_resource_out,
	pl_overhead_out,
	pl_outside_processing_out,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	tl_material_var,
	tl_material_overhead_var,
	tl_resource_var,
	tl_outside_processing_var,
	tl_overhead_var,
	pl_material_var,
	pl_material_overhead_var,
	pl_resource_var,
	pl_overhead_var,
	pl_outside_processing_var,
	tl_scrap_in,
	tl_scrap_out,
	tl_scrap_var,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
		acct_period_id,
		wip_entity_id,
		repetitive_schedule_id,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		organization_id,
		class_type,
		tl_resource_in,
		tl_overhead_in,
		tl_outside_processing_in,
		pl_material_in,
		pl_material_overhead_in,
		pl_resource_in,
		pl_overhead_in,
		pl_outside_processing_in,
		tl_material_out,
		tl_material_overhead_out,
		tl_resource_out,
		tl_overhead_out,
		tl_outside_processing_out,
		pl_material_out,
		pl_material_overhead_out,
		pl_resource_out,
		pl_overhead_out,
		pl_outside_processing_out,
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		tl_material_var,
		tl_material_overhead_var,
		tl_resource_var,
		tl_outside_processing_var,
		tl_overhead_var,
		pl_material_var,
		pl_material_overhead_var,
		pl_resource_var,
		pl_overhead_var,
		pl_outside_processing_var,
		tl_scrap_in,
		tl_scrap_out,
		tl_scrap_var,
		kca_operation,
		kca_seq_id
		,KCA_SEQ_DATE
	from
		bec_raw_dl_ext.WIP_PERIOD_BALANCES
	where
		kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		and (nvl(WIP_ENTITY_ID, 0),
		nvl(REPETITIVE_SCHEDULE_ID, 0),
		nvl(ACCT_PERIOD_ID, 0),
		kca_seq_id) in 
	(
		select
			nvl(WIP_ENTITY_ID, 0) as WIP_ENTITY_ID,
			nvl(REPETITIVE_SCHEDULE_ID, 0) as REPETITIVE_SCHEDULE_ID,
			nvl(ACCT_PERIOD_ID, 0) as ACCT_PERIOD_ID,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.WIP_PERIOD_BALANCES
		where
			kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		group by
			WIP_ENTITY_ID,
			REPETITIVE_SCHEDULE_ID,
			ACCT_PERIOD_ID)
		and (KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wip_period_balances')

            )
);
end;