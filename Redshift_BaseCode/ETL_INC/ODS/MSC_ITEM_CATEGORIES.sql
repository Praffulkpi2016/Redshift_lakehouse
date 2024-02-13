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

delete from bec_ods.MSC_ITEM_CATEGORIES
where (nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0)) in (
select nvl(stg.ORGANIZATION_ID,0),nvl(stg.SR_INSTANCE_ID,0),nvl(stg.INVENTORY_ITEM_ID,0),nvl(stg.CATEGORY_SET_ID,0),nvl(stg.SR_CATEGORY_ID,0) 
 from bec_ods.MSC_ITEM_CATEGORIES ods, bec_ods_stg.MSC_ITEM_CATEGORIES stg
where ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
and  ods.SR_INSTANCE_ID = stg.SR_INSTANCE_ID
and ods.CATEGORY_SET_ID = stg.CATEGORY_SET_ID
and ods.SR_CATEGORY_ID=stg.SR_CATEGORY_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MSC_ITEM_CATEGORIES
       (organization_id,
	inventory_item_id,
	category_set_id,
	category_name,
	description,
	disable_date,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
	sr_instance_id,
	sr_category_id,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)	
(
	select
	organization_id,
	inventory_item_id,
	category_set_id,
	category_name,
	description,
	disable_date,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
	sr_instance_id,
	sr_category_id,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MSC_ITEM_CATEGORIES
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0),kca_seq_id) in 
	(select nvl(ORGANIZATION_ID,0) as ORGANIZATION_ID,nvl(SR_INSTANCE_ID,0) as SR_INSTANCE_ID,nvl(INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,nvl(CATEGORY_SET_ID,0) as CATEGORY_SET_ID,nvl(SR_CATEGORY_ID,0) as SR_CATEGORY_ID ,max(kca_seq_id) from bec_ods_stg.MSC_ITEM_CATEGORIES 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0))
);

commit;

-- Soft delete
update bec_ods.MSC_ITEM_CATEGORIES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MSC_ITEM_CATEGORIES set IS_DELETED_FLG = 'Y'
where (nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0))  in
(
select nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0) from bec_raw_dl_ext.MSC_ITEM_CATEGORIES
where (nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0),KCA_SEQ_ID)
in 
(
select nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MSC_ITEM_CATEGORIES
group by nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'msc_item_categories';

commit;