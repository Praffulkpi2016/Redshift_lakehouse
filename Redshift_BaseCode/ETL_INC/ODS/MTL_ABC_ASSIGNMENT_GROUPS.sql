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

delete from bec_ods.MTL_ABC_ASSIGNMENT_GROUPS
where (NVL(ASSIGNMENT_GROUP_ID,0)) in (
select NVL(stg.ASSIGNMENT_GROUP_ID,0) from bec_ods.MTL_ABC_ASSIGNMENT_GROUPS ods, bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS stg
where NVL(ods.ASSIGNMENT_GROUP_ID,0)= NVL(stg.ASSIGNMENT_GROUP_ID,0)and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_ABC_ASSIGNMENT_GROUPS
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
        IS_DELETED_FLG,
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS
	where kca_operation IN ('INSERT','UPDATE') 
	and (NVL(ASSIGNMENT_GROUP_ID,0),kca_seq_id) in 
	(select NVL(ASSIGNMENT_GROUP_ID,0) as ASSIGNMENT_GROUP_ID,max(kca_seq_id) from bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS 
     where kca_operation IN ('INSERT','UPDATE')
     group by NVL(ASSIGNMENT_GROUP_ID,0))
);

commit;

-- Soft delete
update bec_ods.MTL_ABC_ASSIGNMENT_GROUPS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_ABC_ASSIGNMENT_GROUPS set IS_DELETED_FLG = 'Y'
where (ASSIGNMENT_GROUP_ID)  in
(
select ASSIGNMENT_GROUP_ID from bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS
where (ASSIGNMENT_GROUP_ID,KCA_SEQ_ID)
in 
(
select ASSIGNMENT_GROUP_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS
group by ASSIGNMENT_GROUP_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_abc_assignment_groups'; 

commit;