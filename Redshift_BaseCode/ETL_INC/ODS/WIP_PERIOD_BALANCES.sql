/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;
-- Delete Records

delete
from
	bec_ods.WIP_PERIOD_BALANCES
where
	(nvl(WIP_ENTITY_ID, 0),
	nvl(REPETITIVE_SCHEDULE_ID, 0),
	nvl(ACCT_PERIOD_ID, 0)) in (
	select
		nvl(stg.WIP_ENTITY_ID, 0) as WIP_ENTITY_ID,
			nvl(stg.REPETITIVE_SCHEDULE_ID, 0) as REPETITIVE_SCHEDULE_ID,
			nvl(stg.ACCT_PERIOD_ID, 0) as ACCT_PERIOD_ID
	from
		bec_ods.WIP_PERIOD_BALANCES ods,
		bec_ods_stg.WIP_PERIOD_BALANCES stg
	where
		nvl(ods.WIP_ENTITY_ID, 0) = nvl(stg.WIP_ENTITY_ID, 0)
			and nvl(ods.REPETITIVE_SCHEDULE_ID, 0) = nvl(stg.REPETITIVE_SCHEDULE_ID, 0)
				and nvl(ods.ACCT_PERIOD_ID, 0) = nvl(stg.ACCT_PERIOD_ID, 0)
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.WIP_PERIOD_BALANCES
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
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
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
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.WIP_PERIOD_BALANCES
	where
		kca_operation in ('INSERT','UPDATE')
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
			bec_ods_stg.WIP_PERIOD_BALANCES
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			WIP_ENTITY_ID,
			REPETITIVE_SCHEDULE_ID,
			ACCT_PERIOD_ID)
);

commit;

-- Soft delete
update bec_ods.WIP_PERIOD_BALANCES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WIP_PERIOD_BALANCES set IS_DELETED_FLG = 'Y'
where (nvl(WIP_ENTITY_ID, 0),nvl(REPETITIVE_SCHEDULE_ID, 0),nvl(ACCT_PERIOD_ID, 0))  in
(
select nvl(WIP_ENTITY_ID, 0),nvl(REPETITIVE_SCHEDULE_ID, 0),nvl(ACCT_PERIOD_ID, 0) from bec_raw_dl_ext.WIP_PERIOD_BALANCES
where (nvl(WIP_ENTITY_ID, 0),nvl(REPETITIVE_SCHEDULE_ID, 0),nvl(ACCT_PERIOD_ID, 0),KCA_SEQ_ID)
in 
(
select nvl(WIP_ENTITY_ID, 0),nvl(REPETITIVE_SCHEDULE_ID, 0),nvl(ACCT_PERIOD_ID, 0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WIP_PERIOD_BALANCES
group by nvl(WIP_ENTITY_ID, 0),nvl(REPETITIVE_SCHEDULE_ID, 0),nvl(ACCT_PERIOD_ID, 0)
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'wip_period_balances';

commit;