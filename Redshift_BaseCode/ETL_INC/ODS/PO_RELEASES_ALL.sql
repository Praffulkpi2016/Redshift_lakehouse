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

delete from bec_ods.po_releases_all
where PO_RELEASE_ID in (
select stg.PO_RELEASE_ID from bec_ods.po_releases_all ods, bec_ods_stg.po_releases_all stg
where ods.PO_RELEASE_ID = stg.PO_RELEASE_ID and stg.kca_operation in ('INSERT','UPDATE'));

commit;

-- Insert records
insert
	into
	bec_ods.po_releases_all
(po_release_id,
	last_update_date,
	last_updated_by,
	po_header_id,
	release_num,
	agent_id,
	release_date,
	last_update_login,
	creation_date,
	created_by,
	revision_num,
	revised_date,
	approved_flag,
	approved_date,
	print_count,
	printed_date,
	acceptance_required_flag,
	acceptance_due_date,
	hold_by,
	hold_date,
	hold_reason,
	hold_flag,
	cancel_flag,
	cancelled_by,
	cancel_date,
	cancel_reason,
	firm_status_lookup_code,
	firm_date,
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
	authorization_status,
	ussgl_transaction_code,
	government_context,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	closed_code,
	frozen_flag,
	release_type,
	note_to_vendor,
	org_id,
	edi_processed_flag,
	global_attribute_category,
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
	wf_item_type,
	wf_item_key,
	pcard_id,
	pay_on_code,
	xml_flag,
	xml_send_date,
	xml_change_send_date,
	consigned_consumption_flag,
	cbc_accounting_date,
	change_requested_by,
	shipping_control,
	change_summary,
	vendor_order_num,
	document_creation_method,
	submit_date,
	tax_attribute_update_code,
	comm_rev_num, 
	otm_status_code, 
	otm_recovery_flag, 
	"cancel_reason#1",	
KCA_OPERATION,
IS_DELETED_FLG
,kca_seq_id 
,kca_seq_date
)	
(
select
	po_release_id,
	last_update_date,
	last_updated_by,
	po_header_id,
	release_num,
	agent_id,
	release_date,
	last_update_login,
	creation_date,
	created_by,
	revision_num,
	revised_date,
	approved_flag,
	approved_date,
	print_count,
	printed_date,
	acceptance_required_flag,
	acceptance_due_date,
	hold_by,
	hold_date,
	hold_reason,
	hold_flag,
	cancel_flag,
	cancelled_by,
	cancel_date,
	cancel_reason,
	firm_status_lookup_code,
	firm_date,
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
	authorization_status,
	ussgl_transaction_code,
	government_context,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	closed_code,
	frozen_flag,
	release_type,
	note_to_vendor,
	org_id,
	edi_processed_flag,
	global_attribute_category,
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
	wf_item_type,
	wf_item_key,
	pcard_id,
	pay_on_code,
	xml_flag,
	xml_send_date,
	xml_change_send_date,
	consigned_consumption_flag,
	cbc_accounting_date,
	change_requested_by,
	shipping_control,
	change_summary,
	vendor_order_num,
	document_creation_method,
	submit_date,
	tax_attribute_update_code,
	comm_rev_num, 
	otm_status_code, 
	otm_recovery_flag, 
	"cancel_reason#1",	
	KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
  KCA_SEQ_DATE
from
	bec_ods_stg.po_releases_all
where kca_operation in ('INSERT','UPDATE') and (PO_RELEASE_ID,kca_seq_id) in (select PO_RELEASE_ID,max(kca_seq_id) from bec_ods_stg.po_releases_all 
where kca_operation in ('INSERT','UPDATE')
group by PO_RELEASE_ID)
);

commit;




-- Soft delete
update bec_ods.po_releases_all set IS_DELETED_FLG = 'N';
commit;
update bec_ods.po_releases_all set IS_DELETED_FLG = 'Y'
where (PO_RELEASE_ID)  in
(
select PO_RELEASE_ID from bec_raw_dl_ext.po_releases_all
where (PO_RELEASE_ID,KCA_SEQ_ID)
in 
(
select PO_RELEASE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.po_releases_all
group by PO_RELEASE_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'po_releases_all';

commit;