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
	table bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS;

insert
	into
	bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS
(layer_id,
	transaction_id,
	organization_id,
	cost_element_id,
	level_type,
	transaction_action_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	inventory_item_id,
	actual_cost,
	prior_cost,
	new_cost,
	insertion_flag,
	variance_amount,
	user_entered,
	transaction_costed_date,
	payback_variance_amount,
	onhand_variance_amount,
	--,	kca_seq_date
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		layer_id,
		transaction_id,
		organization_id,
		cost_element_id,
		level_type,
		transaction_action_id,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		inventory_item_id,
		actual_cost,
		prior_cost,
		new_cost,
		insertion_flag,
		variance_amount,
		user_entered,
		transaction_costed_date,
		payback_variance_amount,
		onhand_variance_amount,
		--,	kca_seq_date
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
	where
		kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
		and (LAYER_ID,
		TRANSACTION_ID,
		ORGANIZATION_ID,
		COST_ELEMENT_ID,
		LEVEL_TYPE,
		TRANSACTION_ACTION_ID,
		kca_seq_id) in 
(
		select
			LAYER_ID,
			TRANSACTION_ID,
			ORGANIZATION_ID,
			COST_ELEMENT_ID,
			LEVEL_TYPE,
			TRANSACTION_ACTION_ID,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
		where
			kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
		group by
			LAYER_ID,
			TRANSACTION_ID,
			ORGANIZATION_ID,
			COST_ELEMENT_ID,
			LEVEL_TYPE,
			TRANSACTION_ACTION_ID)
		   
and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='mtl_cst_actual_cost_details')
);
end;