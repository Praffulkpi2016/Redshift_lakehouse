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
drop table if exists bec_ods.PO_DISTRIBUTIONS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.PO_DISTRIBUTIONS_ALL
(
PO_DISTRIBUTION_ID	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,PO_HEADER_ID	NUMERIC(15,0)   ENCODE az64
,PO_LINE_ID	NUMERIC(15,0)   ENCODE az64
,LINE_LOCATION_ID	NUMERIC(15,0)   ENCODE az64
,SET_OF_BOOKS_ID	NUMERIC(15,0)   ENCODE az64
,CODE_COMBINATION_ID	NUMERIC(15,0)   ENCODE az64
,QUANTITY_ORDERED	NUMERIC(28,10)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,PO_RELEASE_ID	NUMERIC(15,0)   ENCODE az64
,QUANTITY_DELIVERED	NUMERIC(28,10)   ENCODE az64
,QUANTITY_BILLED	NUMERIC(28,10)   ENCODE az64
,QUANTITY_CANCELLED	NUMERIC(28,10)  ENCODE az64
,REQ_HEADER_REFERENCE_NUM	VARCHAR(25)   ENCODE lzo
,REQ_LINE_REFERENCE_NUM	VARCHAR(25)   ENCODE lzo
,REQ_DISTRIBUTION_ID	NUMERIC(15,0)   ENCODE az64
,DELIVER_TO_LOCATION_ID	NUMERIC(15,0)   ENCODE az64
,DELIVER_TO_PERSON_ID	NUMERIC(9,0)   ENCODE az64
,RATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,RATE	NUMERIC(28,10)   ENCODE az64
,AMOUNT_BILLED	NUMERIC(28,10)   ENCODE az64
,ACCRUED_FLAG	VARCHAR(1)   ENCODE lzo
,ENCUMBERED_FLAG	VARCHAR(1)   ENCODE lzo
,ENCUMBERED_AMOUNT	NUMERIC(28,10)   ENCODE az64
,UNENCUMBERED_QUANTITY	NUMERIC(28,10)   ENCODE az64
,UNENCUMBERED_AMOUNT	NUMERIC(28,10)   ENCODE az64
,FAILED_FUNDS_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,GL_ENCUMBERED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,GL_ENCUMBERED_PERIOD_NAME	VARCHAR(15)   ENCODE lzo
,GL_CANCELLED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,DESTINATION_TYPE_CODE	VARCHAR(25)   ENCODE lzo
,DESTINATION_ORGANIZATION_ID	NUMERIC(15,0)   ENCODE az64
,DESTINATION_SUBINVENTORY	VARCHAR(10)   ENCODE lzo
,ATTRIBUTE_CATEGORY	VARCHAR(30)   ENCODE lzo
,ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE11	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE12	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE13	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE14	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE15	VARCHAR(150)   ENCODE lzo
,WIP_ENTITY_ID	NUMERIC(15,0)   ENCODE az64
,WIP_OPERATION_SEQ_NUM	NUMERIC(15,0)   ENCODE az64
,WIP_RESOURCE_SEQ_NUM	NUMERIC(15,0)   ENCODE az64
,WIP_REPETITIVE_SCHEDULE_ID	NUMERIC(15,0)   ENCODE az64
,WIP_LINE_ID	NUMERIC(15,0)   ENCODE az64
,BOM_RESOURCE_ID	NUMERIC(15,0)   ENCODE az64
,BUDGET_ACCOUNT_ID	NUMERIC(15,0)   ENCODE az64
,ACCRUAL_ACCOUNT_ID	NUMERIC(15,0)   ENCODE az64
,VARIANCE_ACCOUNT_ID	NUMERIC(15,0)   ENCODE az64
,PREVENT_ENCUMBRANCE_FLAG	VARCHAR(1)   ENCODE lzo
,USSGL_TRANSACTION_CODE	VARCHAR(30)   ENCODE lzo
,GOVERNMENT_CONTEXT	VARCHAR(30)   ENCODE lzo
,DESTINATION_CONTEXT	VARCHAR(30)   ENCODE lzo
,DISTRIBUTION_NUM	NUMERIC(15,0)   ENCODE az64
,SOURCE_DISTRIBUTION_ID	NUMERIC(15,0)   ENCODE az64
,REQUEST_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,PROJECT_ID	NUMERIC(15,0)   ENCODE az64
,TASK_ID	NUMERIC(15,0)   ENCODE az64
,EXPENDITURE_TYPE	VARCHAR(30)   ENCODE lzo
,PROJECT_ACCOUNTING_CONTEXT	VARCHAR(30)   ENCODE lzo
,EXPENDITURE_ORGANIZATION_ID	NUMERIC(15,0)   ENCODE az64
,GL_CLOSED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ACCRUE_ON_RECEIPT_FLAG	VARCHAR(1)   ENCODE lzo
,EXPENDITURE_ITEM_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ORG_ID	NUMERIC(15,0)   ENCODE az64
,KANBAN_CARD_ID	NUMERIC(15,0)   ENCODE az64
,AWARD_ID	NUMERIC(15,0)   ENCODE az64
,MRC_RATE_DATE	VARCHAR(2000)   ENCODE lzo
,MRC_RATE	VARCHAR(2000)   ENCODE lzo
,MRC_ENCUMBERED_AMOUNT	VARCHAR(2000)   ENCODE lzo
,MRC_UNENCUMBERED_AMOUNT	VARCHAR(2000)   ENCODE lzo
,END_ITEM_UNIT_NUMBER	VARCHAR(30)   ENCODE lzo
,TAX_RECOVERY_OVERRIDE_FLAG	VARCHAR(1)   ENCODE lzo
,RECOVERABLE_TAX	NUMERIC(28,10)   ENCODE az64
,NONRECOVERABLE_TAX	NUMERIC(28,10)   ENCODE az64
,RECOVERY_RATE	NUMERIC(28,10)   ENCODE az64
,OKE_CONTRACT_LINE_ID	NUMERIC(15,0)   ENCODE az64
,OKE_CONTRACT_DELIVERABLE_ID	NUMERIC(15,0)   ENCODE az64
,AMOUNT_ORDERED	NUMERIC(28,10)   ENCODE az64
,AMOUNT_DELIVERED	NUMERIC(28,10)   ENCODE az64
,AMOUNT_CANCELLED	NUMERIC(28,10)   ENCODE az64
,DISTRIBUTION_TYPE	VARCHAR(25)   ENCODE lzo
,AMOUNT_TO_ENCUMBER	NUMERIC(28,10)   ENCODE az64
,INVOICE_ADJUSTMENT_FLAG	VARCHAR(1)   ENCODE lzo
,DEST_CHARGE_ACCOUNT_ID	NUMERIC(15,0)   ENCODE az64
,DEST_VARIANCE_ACCOUNT_ID	NUMERIC(15,0)   ENCODE az64
,QUANTITY_FINANCED	NUMERIC(28,10)   ENCODE az64
,AMOUNT_FINANCED	NUMERIC(28,10)   ENCODE az64
,QUANTITY_RECOUPED	NUMERIC(28,10)   ENCODE az64
,AMOUNT_RECOUPED	NUMERIC(28,10)   ENCODE az64
,RETAINAGE_WITHHELD_AMOUNT	NUMERIC(28,10)   ENCODE az64
,RETAINAGE_RELEASED_AMOUNT	NUMERIC(28,10)   ENCODE az64
,WF_ITEM_KEY	VARCHAR(240)   ENCODE lzo
,INVOICED_VAL_IN_NTFN	NUMERIC(28,10)   ENCODE az64
,TAX_ATTRIBUTE_UPDATE_CODE	VARCHAR(15)   ENCODE lzo
,EVENT_ID	NUMERIC(15,0)   ENCODE az64
,INTERFACE_DISTRIBUTION_REF	VARCHAR(240)   ENCODE lzo
,LCM_FLAG	VARCHAR(1)   ENCODE lzo
,group_line_id NUMERIC(15,0)   ENCODE az64
,uda_template_id NUMERIC(15,0)   ENCODE az64
,draft_id NUMERIC(15,0)   ENCODE az64
	,amount_funded NUMERIC(28,10)   ENCODE az64
	,funded_value NUMERIC(28,10)   ENCODE az64
	,partial_funded_flag VARCHAR(1)   ENCODE lzo
	,quantity_funded NUMERIC(28,10)   ENCODE az64
	,clm_misc_loa VARCHAR(200)   ENCODE lzo
	,clm_defence_funding VARCHAR(10)   ENCODE lzo
	,clm_fms_case_number VARCHAR(200)   ENCODE lzo
	,clm_agency_acct_identifier VARCHAR(100)   ENCODE lzo
	,change_in_funded_value NUMERIC(28,10)   ENCODE az64
	,acrn VARCHAR(2)   ENCODE lzo
	,revision_num NUMERIC(28,10)   ENCODE az64
	,amount_reversed NUMERIC(28,10)   ENCODE az64
	,clm_payment_sequence_num NUMERIC(28,10)   ENCODE az64
	,amount_changed_flag VARCHAR(1)   ENCODE lzo
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
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.PO_DISTRIBUTIONS_ALL
(
PO_DISTRIBUTION_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,PO_HEADER_ID
,PO_LINE_ID
,LINE_LOCATION_ID
,SET_OF_BOOKS_ID
,CODE_COMBINATION_ID
,QUANTITY_ORDERED
,LAST_UPDATE_LOGIN
,CREATION_DATE
,CREATED_BY
,PO_RELEASE_ID
,QUANTITY_DELIVERED
,QUANTITY_BILLED
,QUANTITY_CANCELLED
,REQ_HEADER_REFERENCE_NUM
,REQ_LINE_REFERENCE_NUM
,REQ_DISTRIBUTION_ID
,DELIVER_TO_LOCATION_ID
,DELIVER_TO_PERSON_ID
,RATE_DATE
,RATE
,AMOUNT_BILLED
,ACCRUED_FLAG
,ENCUMBERED_FLAG
,ENCUMBERED_AMOUNT
,UNENCUMBERED_QUANTITY
,UNENCUMBERED_AMOUNT
,FAILED_FUNDS_LOOKUP_CODE
,GL_ENCUMBERED_DATE
,GL_ENCUMBERED_PERIOD_NAME
,GL_CANCELLED_DATE
,DESTINATION_TYPE_CODE
,DESTINATION_ORGANIZATION_ID
,DESTINATION_SUBINVENTORY
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,WIP_ENTITY_ID
,WIP_OPERATION_SEQ_NUM
,WIP_RESOURCE_SEQ_NUM
,WIP_REPETITIVE_SCHEDULE_ID
,WIP_LINE_ID
,BOM_RESOURCE_ID
,BUDGET_ACCOUNT_ID
,ACCRUAL_ACCOUNT_ID
,VARIANCE_ACCOUNT_ID
,PREVENT_ENCUMBRANCE_FLAG
,USSGL_TRANSACTION_CODE
,GOVERNMENT_CONTEXT
,DESTINATION_CONTEXT
,DISTRIBUTION_NUM
,SOURCE_DISTRIBUTION_ID
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,PROJECT_ID
,TASK_ID
,EXPENDITURE_TYPE
,PROJECT_ACCOUNTING_CONTEXT
,EXPENDITURE_ORGANIZATION_ID
,GL_CLOSED_DATE
,ACCRUE_ON_RECEIPT_FLAG
,EXPENDITURE_ITEM_DATE
,ORG_ID
,KANBAN_CARD_ID
,AWARD_ID
,MRC_RATE_DATE
,MRC_RATE
,MRC_ENCUMBERED_AMOUNT
,MRC_UNENCUMBERED_AMOUNT
,END_ITEM_UNIT_NUMBER
,TAX_RECOVERY_OVERRIDE_FLAG
,RECOVERABLE_TAX
,NONRECOVERABLE_TAX
,RECOVERY_RATE
,OKE_CONTRACT_LINE_ID
,OKE_CONTRACT_DELIVERABLE_ID
,AMOUNT_ORDERED
,AMOUNT_DELIVERED
,AMOUNT_CANCELLED
,DISTRIBUTION_TYPE
,AMOUNT_TO_ENCUMBER
,INVOICE_ADJUSTMENT_FLAG
,DEST_CHARGE_ACCOUNT_ID
,DEST_VARIANCE_ACCOUNT_ID
,QUANTITY_FINANCED
,AMOUNT_FINANCED
,QUANTITY_RECOUPED
,AMOUNT_RECOUPED
,RETAINAGE_WITHHELD_AMOUNT
,RETAINAGE_RELEASED_AMOUNT
,WF_ITEM_KEY
,INVOICED_VAL_IN_NTFN
,TAX_ATTRIBUTE_UPDATE_CODE
,INTERFACE_DISTRIBUTION_REF
,LCM_FLAG,
	group_line_id,
	uda_template_id,
	draft_id,
	amount_funded,
	funded_value,
	partial_funded_flag,
	quantity_funded,
	clm_misc_loa,
	clm_defence_funding,
	clm_fms_case_number,
	clm_agency_acct_identifier,
	change_in_funded_value,
	acrn,
	revision_num,
	amount_reversed,
	clm_payment_sequence_num,
	amount_changed_flag,
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
	global_attribute20
,KCA_OPERATION
,IS_DELETED_FLG
,kca_seq_id
,kca_seq_date
)
(
select

PO_DISTRIBUTION_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,PO_HEADER_ID
,PO_LINE_ID
,LINE_LOCATION_ID
,SET_OF_BOOKS_ID
,CODE_COMBINATION_ID
,QUANTITY_ORDERED
,LAST_UPDATE_LOGIN
,CREATION_DATE
,CREATED_BY
,PO_RELEASE_ID
,QUANTITY_DELIVERED
,QUANTITY_BILLED
,QUANTITY_CANCELLED
,REQ_HEADER_REFERENCE_NUM
,REQ_LINE_REFERENCE_NUM
,REQ_DISTRIBUTION_ID
,DELIVER_TO_LOCATION_ID
,DELIVER_TO_PERSON_ID
,RATE_DATE
,RATE
,AMOUNT_BILLED
,ACCRUED_FLAG
,ENCUMBERED_FLAG
,ENCUMBERED_AMOUNT
,UNENCUMBERED_QUANTITY
,UNENCUMBERED_AMOUNT
,FAILED_FUNDS_LOOKUP_CODE
,GL_ENCUMBERED_DATE
,GL_ENCUMBERED_PERIOD_NAME
,GL_CANCELLED_DATE
,DESTINATION_TYPE_CODE
,DESTINATION_ORGANIZATION_ID
,DESTINATION_SUBINVENTORY
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,WIP_ENTITY_ID
,WIP_OPERATION_SEQ_NUM
,WIP_RESOURCE_SEQ_NUM
,WIP_REPETITIVE_SCHEDULE_ID
,WIP_LINE_ID
,BOM_RESOURCE_ID
,BUDGET_ACCOUNT_ID
,ACCRUAL_ACCOUNT_ID
,VARIANCE_ACCOUNT_ID
,PREVENT_ENCUMBRANCE_FLAG
,USSGL_TRANSACTION_CODE
,GOVERNMENT_CONTEXT
,DESTINATION_CONTEXT
,DISTRIBUTION_NUM
,SOURCE_DISTRIBUTION_ID
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,PROJECT_ID
,TASK_ID
,EXPENDITURE_TYPE
,PROJECT_ACCOUNTING_CONTEXT
,EXPENDITURE_ORGANIZATION_ID
,GL_CLOSED_DATE
,ACCRUE_ON_RECEIPT_FLAG
,EXPENDITURE_ITEM_DATE
,ORG_ID
,KANBAN_CARD_ID
,AWARD_ID
,MRC_RATE_DATE
,MRC_RATE
,MRC_ENCUMBERED_AMOUNT
,MRC_UNENCUMBERED_AMOUNT
,END_ITEM_UNIT_NUMBER
,TAX_RECOVERY_OVERRIDE_FLAG
,RECOVERABLE_TAX
,NONRECOVERABLE_TAX
,RECOVERY_RATE
,OKE_CONTRACT_LINE_ID
,OKE_CONTRACT_DELIVERABLE_ID
,AMOUNT_ORDERED
,AMOUNT_DELIVERED
,AMOUNT_CANCELLED
,DISTRIBUTION_TYPE
,AMOUNT_TO_ENCUMBER
,INVOICE_ADJUSTMENT_FLAG
,DEST_CHARGE_ACCOUNT_ID
,DEST_VARIANCE_ACCOUNT_ID
,QUANTITY_FINANCED
,AMOUNT_FINANCED
,QUANTITY_RECOUPED
,AMOUNT_RECOUPED
,RETAINAGE_WITHHELD_AMOUNT
,RETAINAGE_RELEASED_AMOUNT
,WF_ITEM_KEY
,INVOICED_VAL_IN_NTFN
,TAX_ATTRIBUTE_UPDATE_CODE
,INTERFACE_DISTRIBUTION_REF
,LCM_FLAG,
	group_line_id,
	uda_template_id,
	draft_id,
	amount_funded,
	funded_value,
	partial_funded_flag,
	quantity_funded,
	clm_misc_loa,
	clm_defence_funding,
	clm_fms_case_number,
	clm_agency_acct_identifier,
	change_in_funded_value,
	acrn,
	revision_num,
	amount_reversed,
	clm_payment_sequence_num,
	amount_changed_flag,
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
	global_attribute20
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID 
,kca_seq_date
from bec_ods_stg.PO_DISTRIBUTIONS_ALL);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='po_distributions_all'; 

commit;
