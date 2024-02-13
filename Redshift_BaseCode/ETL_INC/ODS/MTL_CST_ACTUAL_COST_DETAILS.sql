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
	bec_ods.MTL_CST_ACTUAL_COST_DETAILS
where
	(LAYER_ID,
	TRANSACTION_ID,
	ORGANIZATION_ID,
	COST_ELEMENT_ID,
	LEVEL_TYPE,
	TRANSACTION_ACTION_ID) in (
	select
		stg.LAYER_ID,
		stg.TRANSACTION_ID,
		stg.ORGANIZATION_ID,
		stg.COST_ELEMENT_ID,
		stg.LEVEL_TYPE,
		stg.TRANSACTION_ACTION_ID
	from
		bec_ods.MTL_CST_ACTUAL_COST_DETAILS ods,
		bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS stg
	where
		ods.LAYER_ID = stg.LAYER_ID
		and ods.TRANSACTION_ID = stg.TRANSACTION_ID
		and ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
		and ods.COST_ELEMENT_ID = stg.COST_ELEMENT_ID
		and ods.LEVEL_TYPE = stg.LEVEL_TYPE
		and ods.TRANSACTION_ACTION_ID = stg.TRANSACTION_ACTION_ID
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MTL_CST_ACTUAL_COST_DETAILS
       (
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
	kca_operation,
	is_deleted_flg,
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS
	where
		kca_operation IN ('INSERT','UPDATE')
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
			bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			LAYER_ID,
			TRANSACTION_ID,
			ORGANIZATION_ID,
			COST_ELEMENT_ID,
			LEVEL_TYPE,
			TRANSACTION_ACTION_ID)
);

commit;
 
-- Soft delete
update bec_ods.MTL_CST_ACTUAL_COST_DETAILS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_CST_ACTUAL_COST_DETAILS set IS_DELETED_FLG = 'Y'
where (LAYER_ID,TRANSACTION_ID,ORGANIZATION_ID,COST_ELEMENT_ID,LEVEL_TYPE,TRANSACTION_ACTION_ID)  in
(
select LAYER_ID,TRANSACTION_ID,ORGANIZATION_ID,COST_ELEMENT_ID,LEVEL_TYPE,TRANSACTION_ACTION_ID from bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
where (LAYER_ID,TRANSACTION_ID,ORGANIZATION_ID,COST_ELEMENT_ID,LEVEL_TYPE,TRANSACTION_ACTION_ID,KCA_SEQ_ID)
in 
(
select LAYER_ID,TRANSACTION_ID,ORGANIZATION_ID,COST_ELEMENT_ID,LEVEL_TYPE,TRANSACTION_ACTION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
group by LAYER_ID,TRANSACTION_ID,ORGANIZATION_ID,COST_ELEMENT_ID,LEVEL_TYPE,TRANSACTION_ACTION_ID
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
	ods_table_name = 'mtl_cst_actual_cost_details';

commit;