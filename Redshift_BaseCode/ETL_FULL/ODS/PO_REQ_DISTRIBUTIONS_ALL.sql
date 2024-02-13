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

DROP TABLE if exists bec_ods.PO_REQ_DISTRIBUTIONS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.PO_REQ_DISTRIBUTIONS_ALL
(
	distribution_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,requisition_line_id NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,code_combination_id NUMERIC(15,0)   ENCODE az64
	,req_line_quantity NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,encumbered_flag VARCHAR(1)   ENCODE lzo
	,gl_encumbered_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,gl_encumbered_period_name VARCHAR(15)   ENCODE lzo
	,gl_cancelled_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,failed_funds_lookup_code VARCHAR(25)   ENCODE lzo
	,encumbered_amount NUMERIC(28,10)   ENCODE az64
	,budget_account_id NUMERIC(15,0)   ENCODE az64
	,accrual_account_id NUMERIC(15,0)   ENCODE az64
	,variance_account_id NUMERIC(15,0)   ENCODE az64
	,prevent_encumbrance_flag VARCHAR(1)   ENCODE lzo
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
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,government_context VARCHAR(30)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,expenditure_type VARCHAR(30)   ENCODE lzo
	,project_accounting_context VARCHAR(30)   ENCODE lzo
	,expenditure_organization_id NUMERIC(15,0)   ENCODE az64
	,gl_closed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,source_req_distribution_id NUMERIC(15,0)   ENCODE az64
	,distribution_num NUMERIC(15,0)   ENCODE az64
	,project_related_flag VARCHAR(1)   ENCODE lzo
	,expenditure_item_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,allocation_type VARCHAR(25)   ENCODE lzo
	,allocation_value NUMERIC(28,10)   ENCODE az64
	,award_id NUMERIC(15,0)   ENCODE az64
	,end_item_unit_number VARCHAR(30)   ENCODE lzo
	,recoverable_tax NUMERIC(28,10)   ENCODE az64
	,nonrecoverable_tax NUMERIC(28,10)   ENCODE az64
	,recovery_rate NUMERIC(28,10)   ENCODE az64
	,tax_recovery_override_flag VARCHAR(1)   ENCODE lzo
	,oke_contract_line_id NUMERIC(15,0)   ENCODE az64
	,oke_contract_deliverable_id NUMERIC(15,0)   ENCODE az64
	,req_line_amount NUMERIC(28,10)   ENCODE az64
	,req_line_currency_amount NUMERIC(28,10)   ENCODE az64
	,req_award_id NUMERIC(15,0)   ENCODE az64
	,event_id NUMERIC(15,0)   ENCODE az64
	,ACRN VARCHAR(2)   ENCODE lzo
	,AMENDMENT_STATUS VARCHAR(3)   ENCODE lzo
	,AMENDMENT_TYPE VARCHAR(30)   ENCODE lzo
	,AMOUNT_FUNDED NUMERIC(28,10)   ENCODE az64
	,CHANGE_IN_FUNDED_VALUE NUMERIC(15,0)   ENCODE az64
	,CLM_AGENCY_ACCT_IDENTIFIER VARCHAR(1000)   ENCODE lzo
	,CLM_DEFENCE_FUNDING VARCHAR(10)   ENCODE lzo
	,CLM_FMS_CASE_NUMBER VARCHAR(240)   ENCODE lzo
	,CLM_MISC_LOA VARCHAR(200)   ENCODE lzo
	,CONFORMED_DIST_ID NUMERIC(15,0)   ENCODE az64
	,FUNDED_VALUE NUMERIC(28,10)   ENCODE az64
	,FUNDS_LIQUIDATED NUMERIC(28,10)   ENCODE az64
	,INFO_LINE_ID NUMERIC(15,0)   ENCODE az64
	,PAR_DISTRIBUTION_ID NUMERIC(15,0)   ENCODE az64
	,PAR_DRAFT_ID NUMERIC(15,0)   ENCODE az64
	,PARTIAL_FUNDED_FLAG VARCHAR(1)   ENCODE lzo
	,QUANTITY_FUNDED NUMERIC(15,0)   ENCODE az64
	,UDA_TEMPLATE_ID NUMERIC(15,0)   ENCODE az64
	,UNENCUMBERED_AMOUNT NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.PO_REQ_DISTRIBUTIONS_ALL (
    distribution_id,
	last_update_date,
	last_updated_by,
	requisition_line_id,
	set_of_books_id,
	code_combination_id,
	req_line_quantity,
	last_update_login,
	creation_date,
	created_by,
	encumbered_flag,
	gl_encumbered_date,
	gl_encumbered_period_name,
	gl_cancelled_date,
	failed_funds_lookup_code,
	encumbered_amount,
	budget_account_id,
	accrual_account_id,
	variance_account_id,
	prevent_encumbrance_flag,
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
	ussgl_transaction_code,
	government_context,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	project_id,
	task_id,
	expenditure_type,
	project_accounting_context,
	expenditure_organization_id,
	gl_closed_date,
	source_req_distribution_id,
	distribution_num,
	project_related_flag,
	expenditure_item_date,
	org_id,
	allocation_type,
	allocation_value,
	award_id,
	end_item_unit_number,
	recoverable_tax,
	nonrecoverable_tax,
	recovery_rate,
	tax_recovery_override_flag,
	oke_contract_line_id,
	oke_contract_deliverable_id,
	req_line_amount,
	req_line_currency_amount,
	req_award_id,
	event_id,
	acrn,
	amendment_status,
	amendment_type,
	amount_funded,
	change_in_funded_value,
	clm_agency_acct_identifier,
	clm_defence_funding,
	clm_fms_case_number,
	clm_misc_loa,
	conformed_dist_id,
	funded_value,
	funds_liquidated,
	info_line_id,
	par_distribution_id,
	par_draft_id,
	partial_funded_flag,
	quantity_funded,
	uda_template_id,
	unencumbered_amount,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date 
	)
    SELECT
    distribution_id,
	last_update_date,
	last_updated_by,
	requisition_line_id,
	set_of_books_id,
	code_combination_id,
	req_line_quantity,
	last_update_login,
	creation_date,
	created_by,
	encumbered_flag,
	gl_encumbered_date,
	gl_encumbered_period_name,
	gl_cancelled_date,
	failed_funds_lookup_code,
	encumbered_amount,
	budget_account_id,
	accrual_account_id,
	variance_account_id,
	prevent_encumbrance_flag,
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
	ussgl_transaction_code,
	government_context,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	project_id,
	task_id,
	expenditure_type,
	project_accounting_context,
	expenditure_organization_id,
	gl_closed_date,
	source_req_distribution_id,
	distribution_num,
	project_related_flag,
	expenditure_item_date,
	org_id,
	allocation_type,
	allocation_value,
	award_id,
	end_item_unit_number,
	recoverable_tax,
	nonrecoverable_tax,
	recovery_rate,
	tax_recovery_override_flag,
	oke_contract_line_id,
	oke_contract_deliverable_id,
	req_line_amount,
	req_line_currency_amount,
	req_award_id,
	event_id,
	acrn,
	amendment_status,
	amendment_type,
	amount_funded,
	change_in_funded_value,
	clm_agency_acct_identifier,
	clm_defence_funding,
	clm_fms_case_number,
	clm_misc_loa,
	conformed_dist_id,
	funded_value,
	funds_liquidated,
	info_line_id,
	par_distribution_id,
	par_draft_id,
	partial_funded_flag,
	quantity_funded,
	uda_template_id,
	unencumbered_amount,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.PO_REQ_DISTRIBUTIONS_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_req_distributions_all';
	
commit;