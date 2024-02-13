/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/
begin;

DROP TABLE if exists bec_ods.PO_RELEASES_ALL;
CREATE TABLE IF NOT EXISTS bec_ods.po_releases_all
(
	po_release_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,po_header_id NUMERIC(15,0)   ENCODE az64
	,release_num NUMERIC(15,0)   ENCODE az64
	,agent_id NUMERIC(9,0)   ENCODE az64
	,release_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,revision_num NUMERIC(15,0)   ENCODE az64
	,revised_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,approved_flag VARCHAR(1)   ENCODE lzo
	,approved_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,print_count NUMERIC(15,0)   ENCODE az64
	,printed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,acceptance_required_flag VARCHAR(1)   ENCODE lzo
	,acceptance_due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,hold_by NUMERIC(9,0)   ENCODE az64
	,hold_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,hold_reason VARCHAR(240)   ENCODE lzo
	,hold_flag VARCHAR(1)   ENCODE lzo
	,cancel_flag VARCHAR(1)   ENCODE lzo
	,cancelled_by NUMERIC(9,0)   ENCODE az64
	,cancel_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cancel_reason VARCHAR(240)   ENCODE lzo
	,firm_status_lookup_code VARCHAR(25)   ENCODE lzo
	,firm_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,authorization_status VARCHAR(25)   ENCODE lzo
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,government_context VARCHAR(30)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,closed_code VARCHAR(25)   ENCODE lzo
	,frozen_flag VARCHAR(1)   ENCODE lzo
	,release_type VARCHAR(25)   ENCODE lzo
	,note_to_vendor VARCHAR(480)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,edi_processed_flag VARCHAR(1)   ENCODE lzo
	,global_attribute_category VARCHAR(150)   ENCODE lzo
	,global_attribute1 VARCHAR(150)   ENCODE lzo
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,global_attribute10 VARCHAR(150)   ENCODE lzo
	,global_attribute11 VARCHAR(150)   ENCODE lzo
	,global_attribute12 VARCHAR(150)   ENCODE lzo
	,global_attribute13 VARCHAR(150)   ENCODE lzo
	,global_attribute14 VARCHAR(150)   ENCODE lzo
	,global_attribute15 VARCHAR(150)   ENCODE lzo
	,global_attribute16 VARCHAR(150)   ENCODE lzo
	,global_attribute17 VARCHAR(150)   ENCODE lzo
	,global_attribute18 VARCHAR(150)   ENCODE lzo
	,global_attribute19 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,wf_item_type VARCHAR(8)   ENCODE lzo
	,wf_item_key VARCHAR(240)   ENCODE lzo
	,pcard_id NUMERIC(15,0)   ENCODE az64
	,pay_on_code VARCHAR(25)   ENCODE lzo
	,xml_flag VARCHAR(3)   ENCODE lzo
	,xml_send_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,xml_change_send_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,consigned_consumption_flag VARCHAR(1)   ENCODE lzo
	,cbc_accounting_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,change_requested_by VARCHAR(20)   ENCODE lzo
	,shipping_control VARCHAR(30)   ENCODE lzo
	,change_summary VARCHAR(2000)   ENCODE lzo
	,vendor_order_num VARCHAR(25)   ENCODE lzo
	,document_creation_method VARCHAR(30)   ENCODE lzo
	,submit_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tax_attribute_update_code VARCHAR(15)   ENCODE lzo
	,comm_rev_num NUMERIC(15,0)   ENCODE az64
	,otm_status_code VARCHAR(40)   ENCODE lzo
	,otm_recovery_flag VARCHAR(1)   ENCODE lzo
	,"cancel_reason#1" VARCHAR(2000)   ENCODE lzo	
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO;
insert
	into
	bec_ods.po_releases_all
(	po_release_id,
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
	"cancel_reason#1"
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID  
,kca_seq_date
from
	bec_ods_stg.po_releases_all;
end;		


UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_releases_all'; 
	
commit;