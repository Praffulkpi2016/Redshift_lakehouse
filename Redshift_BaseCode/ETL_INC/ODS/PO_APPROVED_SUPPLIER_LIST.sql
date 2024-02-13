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

delete from bec_ods.PO_APPROVED_SUPPLIER_LIST
where ASL_ID in (
select stg.ASL_ID
from bec_ods.PO_APPROVED_SUPPLIER_LIST ods, bec_ods_stg.PO_APPROVED_SUPPLIER_LIST stg
where ods.ASL_ID = stg.ASL_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.PO_APPROVED_SUPPLIER_LIST
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
	vendor_id,
	item_id,
	category_id,
	vendor_site_id,
	primary_vendor_item,
	manufacturer_asl_id,
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
	disable_flag,	
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
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
	vendor_id,
	item_id,
	category_id,
	vendor_site_id,
	primary_vendor_item,
	manufacturer_asl_id,
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
	disable_flag,	
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,  
		KCA_SEQ_DATE
	from bec_ods_stg.PO_APPROVED_SUPPLIER_LIST
	where kca_operation in ('INSERT','UPDATE') 
	and (ASL_ID,kca_seq_id) in 
	(select ASL_ID,max(kca_seq_id) from bec_ods_stg.PO_APPROVED_SUPPLIER_LIST 
     where kca_operation in ('INSERT','UPDATE')
     group by ASL_ID)
);

commit;


-- Soft delete
update bec_ods.PO_APPROVED_SUPPLIER_LIST set IS_DELETED_FLG = 'N';
commit;
update bec_ods.PO_APPROVED_SUPPLIER_LIST set IS_DELETED_FLG = 'Y'
where (ASL_ID)  in
(
select ASL_ID from bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST
where (ASL_ID,KCA_SEQ_ID)
in 
(
select ASL_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST
group by ASL_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'po_approved_supplier_list';

commit;