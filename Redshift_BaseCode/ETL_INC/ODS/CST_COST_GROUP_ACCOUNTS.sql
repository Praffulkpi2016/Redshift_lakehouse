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
	bec_ods.CST_COST_GROUP_ACCOUNTS
where
	(COST_GROUP_ID,ORGANIZATION_ID) in (
	select
		stg.COST_GROUP_ID,stg.ORGANIZATION_ID
	from
		bec_ods.CST_COST_GROUP_ACCOUNTS ods,
		bec_ods_stg.CST_COST_GROUP_ACCOUNTS stg
	where
		ods.COST_GROUP_ID = stg.COST_GROUP_ID
		and ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.CST_COST_GROUP_ACCOUNTS
       (
    cost_group_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	material_account,
	material_overhead_account,
	resource_account,
	overhead_account,
	outside_processing_account,
	average_cost_var_account,
	encumbrance_account,
	payback_mat_var_account,
	payback_res_var_account,
	payback_osp_var_account,
	payback_moh_var_account,
	payback_ovh_var_account,
	expense_account,
	purchase_price_var_account,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
(
	select
		cost_group_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	material_account,
	material_overhead_account,
	resource_account,
	overhead_account,
	outside_processing_account,
	average_cost_var_account,
	encumbrance_account,
	payback_mat_var_account,
	payback_res_var_account,
	payback_osp_var_account,
	payback_moh_var_account,
	payback_ovh_var_account,
	expense_account,
	purchase_price_var_account,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.CST_COST_GROUP_ACCOUNTS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (COST_GROUP_ID,ORGANIZATION_ID,kca_seq_id) in 
	(
		select
			COST_GROUP_ID,ORGANIZATION_ID,max(kca_seq_id)
		from
			bec_ods_stg.CST_COST_GROUP_ACCOUNTS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			COST_GROUP_ID,ORGANIZATION_ID)
);

commit;

-- Soft delete
update bec_ods.CST_COST_GROUP_ACCOUNTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CST_COST_GROUP_ACCOUNTS set IS_DELETED_FLG = 'Y'
where (COST_GROUP_ID,ORGANIZATION_ID)  in
(
select COST_GROUP_ID,ORGANIZATION_ID from bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
where (COST_GROUP_ID,ORGANIZATION_ID,KCA_SEQ_ID)
in 
(
select COST_GROUP_ID,ORGANIZATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
group by COST_GROUP_ID,ORGANIZATION_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'cst_cost_group_accounts';

commit;