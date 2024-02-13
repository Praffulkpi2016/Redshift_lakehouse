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
BEGIN;

TRUNCATE TABLE bec_ods_stg.MTL_PHYSICAL_INVENTORIES;

insert into	bec_ods_stg.MTL_PHYSICAL_INVENTORIES
    (physical_inventory_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	physical_inventory_date,
	last_adjustment_date,
	total_adjustment_value,
	description,
	freeze_date,
	physical_inventory_name,
	approval_required,
	all_subinventories_flag,
	next_tag_number,
	tag_number_increments,
	default_gl_adjust_account,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	approval_tolerance_pos,
	approval_tolerance_neg,
	cost_variance_pos,
	cost_variance_neg,
	number_of_skus,
	dynamic_tag_entry_flag,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	attribute_category,
	exclude_zero_balance,
	exclude_negative_balance,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date)
(select
	physical_inventory_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	physical_inventory_date,
	last_adjustment_date,
	total_adjustment_value,
	description,
	freeze_date,
	physical_inventory_name,
	approval_required,
	all_subinventories_flag,
	next_tag_number,
	tag_number_increments,
	default_gl_adjust_account,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	approval_tolerance_pos,
	approval_tolerance_neg,
	cost_variance_pos,
	cost_variance_neg,
	number_of_skus,
	dynamic_tag_entry_flag,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	attribute_category,
	exclude_zero_balance,
	exclude_negative_balance,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date
from
	bec_raw_dl_ext.MTL_PHYSICAL_INVENTORIES 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (NVL(ORGANIZATION_ID,0),NVL(PHYSICAL_INVENTORY_ID,0),KCA_SEQ_ID) in 
	(select NVL(ORGANIZATION_ID,0) AS ORGANIZATION_ID,NVL(PHYSICAL_INVENTORY_ID,0) AS PHYSICAL_INVENTORY_ID,max(KCA_SEQ_ID) from bec_raw_dl_ext.MTL_PHYSICAL_INVENTORIES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by NVL(ORGANIZATION_ID,0),NVL(PHYSICAL_INVENTORY_ID,0))
     and ( kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='mtl_physical_inventories')
	  
            )
	 );
END;