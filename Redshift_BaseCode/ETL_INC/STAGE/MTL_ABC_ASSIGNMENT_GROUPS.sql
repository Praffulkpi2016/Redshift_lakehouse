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

truncate table bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS;

insert into	bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS
   (
 assignment_group_id,
	assignment_group_name,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	compile_id,
	secondary_inventory,
	item_scope_type,
	classification_method_type,
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
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
	assignment_group_id,
	assignment_group_name,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	compile_id,
	secondary_inventory,
	item_scope_type,
	classification_method_type,
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
     KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (NVL(ASSIGNMENT_GROUP_ID,0),kca_seq_id) in 
	(select NVL(ASSIGNMENT_GROUP_ID,0) as ASSIGNMENT_GROUP_ID,max(kca_seq_id) from bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by NVL(ASSIGNMENT_GROUP_ID,0))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_abc_assignment_groups')
);
end;