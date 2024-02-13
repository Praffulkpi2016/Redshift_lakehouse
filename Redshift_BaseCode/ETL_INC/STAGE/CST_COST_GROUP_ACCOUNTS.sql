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
	table bec_ods_stg.CST_COST_GROUP_ACCOUNTS;

insert
	into
	bec_ods_stg.CST_COST_GROUP_ACCOUNTS
(cost_group_id,
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
	KCA_OPERATION,
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
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
	where
		kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (COST_GROUP_ID,ORGANIZATION_ID,kca_seq_id) in 
(
		select
			COST_GROUP_ID,
		    ORGANIZATION_ID,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
		where
			kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			COST_GROUP_ID,
		    ORGANIZATION_ID)
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cst_cost_group_accounts')
);
end;