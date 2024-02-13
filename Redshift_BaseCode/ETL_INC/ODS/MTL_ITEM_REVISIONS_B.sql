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

delete from bec_ods.MTL_ITEM_REVISIONS_B
where (REVISION_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID) in (
select stg.REVISION_ID,stg.INVENTORY_ITEM_ID,stg.ORGANIZATION_ID
from bec_ods.MTL_ITEM_REVISIONS_B ods, bec_ods_stg.MTL_ITEM_REVISIONS_B stg
where ods.REVISION_ID = stg.REVISION_ID
and  ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
and ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_ITEM_REVISIONS_B
       (  inventory_item_id,
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
	is_deleted_flg,
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.MTL_ITEM_REVISIONS_B
	where kca_operation IN ('INSERT','UPDATE') 
	and (REVISION_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,kca_seq_id) in 
	(select REVISION_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,max(kca_seq_id) from bec_ods_stg.MTL_ITEM_REVISIONS_B 
     where kca_operation IN ('INSERT','UPDATE')
     group by REVISION_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID)
);

commit;

 

-- Soft delete
update bec_ods.MTL_ITEM_REVISIONS_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_ITEM_REVISIONS_B set IS_DELETED_FLG = 'Y'
where (REVISION_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID)  in
(
select REVISION_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID from bec_raw_dl_ext.MTL_ITEM_REVISIONS_B
where (REVISION_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,KCA_SEQ_ID)
in 
(
select REVISION_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_ITEM_REVISIONS_B
group by REVISION_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_item_revisions_b';

commit;