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

truncate table bec_ods_stg.MSC_ITEM_CATEGORIES;

insert into	bec_ods_stg.MSC_ITEM_CATEGORIES
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
	KCA_OPERATION,
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
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.MSC_ITEM_CATEGORIES
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0),kca_seq_id) in 
	(select  nvl(ORGANIZATION_ID,0) as ORGANIZATION_ID,nvl(SR_INSTANCE_ID,0) as SR_INSTANCE_ID,nvl(INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,nvl(CATEGORY_SET_ID,0) as CATEGORY_SET_ID,nvl(SR_CATEGORY_ID,0) as SR_CATEGORY_ID ,max(kca_seq_id) from bec_raw_dl_ext.MSC_ITEM_CATEGORIES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(ORGANIZATION_ID,0),nvl(SR_INSTANCE_ID,0),nvl(INVENTORY_ITEM_ID,0),nvl(CATEGORY_SET_ID,0),nvl(SR_CATEGORY_ID,0))
	 and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'msc_item_categories')
         
);
end;