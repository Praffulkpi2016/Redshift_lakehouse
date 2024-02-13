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

delete from bec_ods.AR_RECEIPT_CLASSES
where receipt_class_id in (
select stg.receipt_class_id from bec_ods.AR_RECEIPT_CLASSES ods, bec_ods_stg.AR_RECEIPT_CLASSES stg
where ods.receipt_class_id = stg.receipt_class_id and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.AR_RECEIPT_CLASSES
       (
    receipt_class_id,
	confirm_flag,
	created_by,
	creation_date,
	creation_method_code,
	last_updated_by,
	last_update_date,
	clear_flag,
	"name",
	remit_flag,
	creation_status,
	last_update_login,
	remit_method_code,
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
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	global_attribute_category,
	notes_receivable,
	bill_of_exchange_flag, 
	zd_edition_name,
	zd_sync,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
(
	SELECT
	receipt_class_id,
	confirm_flag,
	created_by,
	creation_date,
	creation_method_code,
	last_updated_by,
	last_update_date,
	clear_flag,
	"name",
	remit_flag,
	creation_status,
	last_update_login,
	remit_method_code,
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
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	global_attribute_category,
	notes_receivable,
	bill_of_exchange_flag, 
	zd_edition_name,
	zd_sync,
	kca_operation,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.AR_RECEIPT_CLASSES
	where kca_operation IN ('INSERT','UPDATE') 
	and (receipt_class_id,kca_seq_id) in 
	(select receipt_class_id,max(kca_seq_id) from bec_ods_stg.AR_RECEIPT_CLASSES 
     where kca_operation IN ('INSERT','UPDATE')
     group by receipt_class_id)
);

commit;

-- Soft delete
update bec_ods.AR_RECEIPT_CLASSES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AR_RECEIPT_CLASSES set IS_DELETED_FLG = 'Y'
where (receipt_class_id)  in
(
select receipt_class_id from bec_raw_dl_ext.AR_RECEIPT_CLASSES
where (receipt_class_id,KCA_SEQ_ID)
in 
(
select receipt_class_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AR_RECEIPT_CLASSES
group by receipt_class_id
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ar_receipt_classes';

commit;