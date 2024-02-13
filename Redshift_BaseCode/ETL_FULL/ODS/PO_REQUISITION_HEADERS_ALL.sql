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

DROP TABLE if exists bec_ods.PO_REQUISITION_HEADERS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.PO_REQUISITION_HEADERS_ALL
(
	requisition_header_id NUMERIC(15,0)   ENCODE az64
	,preparer_id NUMERIC(9,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,segment1 VARCHAR(20)   ENCODE lzo
	,summary_flag VARCHAR(1)   ENCODE lzo
	,enabled_flag VARCHAR(1)   ENCODE lzo
	,segment2 VARCHAR(20)   ENCODE lzo
	,segment3 VARCHAR(20)   ENCODE lzo
	,segment4 VARCHAR(20)   ENCODE lzo
	,segment5 VARCHAR(20)   ENCODE lzo
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,authorization_status VARCHAR(25)   ENCODE lzo
	,note_to_authorizer VARCHAR(4000)   ENCODE lzo
	,type_lookup_code VARCHAR(25)   ENCODE lzo
	,transferred_to_oe_flag VARCHAR(1)   ENCODE lzo
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,on_line_flag VARCHAR(1)   ENCODE lzo
	,preliminary_research_flag VARCHAR(1)   ENCODE lzo
	,research_complete_flag VARCHAR(1)   ENCODE lzo
	,preparer_finished_flag VARCHAR(1)   ENCODE lzo
	,preparer_finished_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,agent_return_flag VARCHAR(1)   ENCODE lzo
	,agent_return_note VARCHAR(240)   ENCODE lzo
	,cancel_flag VARCHAR(1)   ENCODE lzo
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
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,government_context VARCHAR(30)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,interface_source_code VARCHAR(25)   ENCODE lzo
	,interface_source_line_id NUMERIC(15,0)   ENCODE az64
	,closed_code VARCHAR(25)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,wf_item_type VARCHAR(8)   ENCODE lzo
	,wf_item_key VARCHAR(240)   ENCODE lzo
	,emergency_po_num VARCHAR(20)   ENCODE lzo
	,pcard_id NUMERIC(15,0)   ENCODE az64
	,apps_source_code VARCHAR(25)   ENCODE lzo
	,cbc_accounting_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,change_pending_flag VARCHAR(1)   ENCODE lzo
	,active_shopping_cart_flag VARCHAR(1)   ENCODE lzo
	,contractor_status VARCHAR(25)   ENCODE lzo
	,contractor_requisition_flag VARCHAR(1)   ENCODE lzo
	,supplier_notified_flag VARCHAR(1)   ENCODE lzo
	,emergency_po_org_id NUMERIC(15,0)   ENCODE az64
	,approved_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tax_attribute_update_code VARCHAR(15)   ENCODE lzo
	,first_approver_id NUMERIC(15,0)   ENCODE az64
	,first_position_id NUMERIC(15,0)   ENCODE az64
	,AMENDMENT_REASON VARCHAR(4000)   ENCODE lzo
	,AMENDMENT_STATUS VARCHAR(3)   ENCODE lzo
	,AMENDMENT_TYPE VARCHAR(30)   ENCODE lzo
	,CLM_ASSIST_CONTACT VARCHAR(240)   ENCODE lzo
	,CLM_ASSIST_OFFICE  NUMERIC(15,0)   ENCODE az64
	,CLM_COTR_CONTACT VARCHAR(400)   ENCODE lzo
	,CLM_COTR_OFFICE VARCHAR(1000)   ENCODE lzo
	,CLM_ISSUING_OFFICE VARCHAR(1000)   ENCODE lzo
	,CLM_MIPR_ACKNOWLEDGED_FLAG VARCHAR(1)   ENCODE lzo
	,CLM_MIPR_PREPARED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CLM_MIPR_REF_NUM VARCHAR(30)   ENCODE lzo
	,CLM_MIPR_TYPE VARCHAR(30)   ENCODE lzo
	,CLM_PRIORITY_CODE VARCHAR(400)   ENCODE lzo
	,CLM_REQ_CONTACT VARCHAR(240)   ENCODE lzo
	,CLM_REQ_OFFICE  NUMERIC(15,0)   ENCODE az64
	,CONFORMED_HEADER_ID  NUMERIC(15,0)   ENCODE az64
	,FEDERAL_FLAG VARCHAR(1)   ENCODE lzo
	--,IGT_DOCUMENT_FLAG VARCHAR(1)   ENCODE lzo
	,NOTIFY_REQUESTER_FLAG VARCHAR(1)   ENCODE lzo
	,PAR_DRAFT_ID NUMERIC(15,0)   ENCODE az64
	,PAR_FLAG VARCHAR(1)   ENCODE lzo
	,REVISION_NUM NUMERIC(15,0)   ENCODE az64
	,SUGGESTED_AWARD_NO VARCHAR(240)   ENCODE lzo
	,UDA_TEMPLATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,UDA_TEMPLATE_ID NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.PO_REQUISITION_HEADERS_ALL (
    requisition_header_id,
	preparer_id,
	last_update_date,
	last_updated_by,
	segment1,
	summary_flag,
	enabled_flag,
	segment2,
	segment3,
	segment4,
	segment5,
	start_date_active,
	end_date_active,
	last_update_login,
	creation_date,
	created_by,
	description,
	authorization_status,
	note_to_authorizer,
	type_lookup_code,
	transferred_to_oe_flag,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	on_line_flag,
	preliminary_research_flag,
	research_complete_flag,
	preparer_finished_flag,
	preparer_finished_date,
	agent_return_flag,
	agent_return_note,
	cancel_flag,
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
	ussgl_transaction_code,
	government_context,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	interface_source_code,
	interface_source_line_id,
	closed_code,
	org_id,
	wf_item_type,
	wf_item_key,
	emergency_po_num,
	pcard_id,
	apps_source_code,
	cbc_accounting_date,
	change_pending_flag,
	active_shopping_cart_flag,
	contractor_status,
	contractor_requisition_flag,
	supplier_notified_flag,
	emergency_po_org_id,
	approved_date,
	tax_attribute_update_code,
	first_approver_id,
	first_position_id,
	amendment_reason,
	amendment_status,
	amendment_type,
	clm_assist_contact,
	clm_assist_office,
	clm_cotr_contact,
	clm_cotr_office,
	clm_issuing_office,
	clm_mipr_acknowledged_flag,
	clm_mipr_prepared_date,
	clm_mipr_ref_num,
	clm_mipr_type,
	clm_priority_code,
	clm_req_contact,
	clm_req_office,
	conformed_header_id,
	federal_flag,
	--igt_document_flag,
	notify_requester_flag,
	par_draft_id,
	par_flag,
	revision_num,
	suggested_award_no,
	uda_template_date,
	uda_template_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
	)
    SELECT
    requisition_header_id,
	preparer_id,
	last_update_date,
	last_updated_by,
	segment1,
	summary_flag,
	enabled_flag,
	segment2,
	segment3,
	segment4,
	segment5,
	start_date_active,
	end_date_active,
	last_update_login,
	creation_date,
	created_by,
	description,
	authorization_status,
	note_to_authorizer,
	type_lookup_code,
	transferred_to_oe_flag,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	on_line_flag,
	preliminary_research_flag,
	research_complete_flag,
	preparer_finished_flag,
	preparer_finished_date,
	agent_return_flag,
	agent_return_note,
	cancel_flag,
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
	ussgl_transaction_code,
	government_context,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	interface_source_code,
	interface_source_line_id,
	closed_code,
	org_id,
	wf_item_type,
	wf_item_key,
	emergency_po_num,
	pcard_id,
	apps_source_code,
	cbc_accounting_date,
	change_pending_flag,
	active_shopping_cart_flag,
	contractor_status,
	contractor_requisition_flag,
	supplier_notified_flag,
	emergency_po_org_id,
	approved_date,
	tax_attribute_update_code,
	first_approver_id,
	first_position_id,
	amendment_reason,
	amendment_status,
	amendment_type,
	clm_assist_contact,
	clm_assist_office,
	clm_cotr_contact,
	clm_cotr_office,
	clm_issuing_office,
	clm_mipr_acknowledged_flag,
	clm_mipr_prepared_date,
	clm_mipr_ref_num,
	clm_mipr_type,
	clm_priority_code,
	clm_req_contact,
	clm_req_office,
	conformed_header_id,
	federal_flag,
	--igt_document_flag,
	notify_requester_flag,
	par_draft_id,
	par_flag,
	revision_num,
	suggested_award_no,
	uda_template_date,
	uda_template_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.PO_REQUISITION_HEADERS_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_requisition_headers_all';
	
commit;