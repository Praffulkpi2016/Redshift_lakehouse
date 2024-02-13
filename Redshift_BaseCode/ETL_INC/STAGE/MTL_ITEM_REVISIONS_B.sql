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

truncate table bec_ods_stg.MTL_ITEM_REVISIONS_B;

insert into	bec_ods_stg.MTL_ITEM_REVISIONS_B
   (inventory_item_id,
	organization_id,
	revision,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	change_notice,
	ecn_initiation_date,
	implementation_date,
	implemented_serial_number,
	effectivity_date,
	attribute_category,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	revised_item_sequence_id,
	description,
	object_version_number,
	revision_id,
	revision_label,
	revision_reason,
	lifecycle_id,
	current_phase_id,
	kca_operation,
	kca_seq_id,
	kca_seq_date)
(
	select
	inventory_item_id,
	organization_id,
	revision,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	change_notice,
	ecn_initiation_date,
	implementation_date,
	implemented_serial_number,
	effectivity_date,
	attribute_category,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	revised_item_sequence_id,
	description,
	object_version_number,
	revision_id,
	revision_label,
	revision_reason,
	lifecycle_id,
	current_phase_id,
	kca_operation,
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.MTL_ITEM_REVISIONS_B
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (INVENTORY_ITEM_ID,ORGANIZATION_ID,REVISION_ID,kca_seq_id) in 
	(select INVENTORY_ITEM_ID,ORGANIZATION_ID,REVISION_ID,max(kca_seq_id) from bec_raw_dl_ext.MTL_ITEM_REVISIONS_B 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by INVENTORY_ITEM_ID,ORGANIZATION_ID,REVISION_ID)
        and	( kca_seq_date > (
		select
			(executebegints-10000)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_item_revisions_b')
		 
            )	
);
end;