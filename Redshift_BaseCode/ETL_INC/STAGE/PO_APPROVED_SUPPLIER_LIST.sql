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

truncate table bec_ods_stg.PO_APPROVED_SUPPLIER_LIST;

insert into	bec_ods_stg.PO_APPROVED_SUPPLIER_LIST
   (asl_id,
	using_organization_id,
	owning_organization_id,
	vendor_business_type,
	asl_status_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	manufacturer_id,
	review_by_date,
	comments,
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
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	vendor_id,
	item_id,
	category_id,
	vendor_site_id,
	primary_vendor_item,
	manufacturer_asl_id,
	disable_flag,	
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
		asl_id,
	using_organization_id,
	owning_organization_id,
	vendor_business_type,
	asl_status_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	manufacturer_id,
	review_by_date,
	comments,
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
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	vendor_id,
	item_id,
	category_id,
	vendor_site_id,
	primary_vendor_item,
	manufacturer_asl_id,
	disable_flag,
	kca_operation,
	kca_seq_id 
	,KCA_SEQ_DATE 
	from bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (ASL_ID,kca_seq_id) in 
	(select ASL_ID,max(kca_seq_id) from bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by ASL_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'po_approved_supplier_list')
            )
);
end;