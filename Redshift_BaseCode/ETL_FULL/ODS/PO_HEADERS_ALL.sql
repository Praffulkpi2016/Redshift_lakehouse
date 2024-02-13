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
drop table if exists bec_ods.PO_HEADERS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.PO_HEADERS_ALL
(
PO_HEADER_ID	NUMERIC(15,0)   ENCODE az64
,AGENT_ID	NUMERIC(9,0)   ENCODE az64
,TYPE_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,SEGMENT1	VARCHAR(20)   ENCODE lzo
,SUMMARY_FLAG	VARCHAR(1)   ENCODE lzo
,ENABLED_FLAG	VARCHAR(1)   ENCODE lzo
,SEGMENT2	VARCHAR(20)   ENCODE lzo
,SEGMENT3	VARCHAR(20)   ENCODE lzo
,SEGMENT4	VARCHAR(20)   ENCODE lzo
,SEGMENT5	VARCHAR(20)   ENCODE lzo
,START_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,END_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,VENDOR_ID	NUMERIC(15,0)   ENCODE az64
,VENDOR_SITE_ID	NUMERIC(15,0)   ENCODE az64
,VENDOR_CONTACT_ID	NUMERIC(15,0)   ENCODE az64
,SHIP_TO_LOCATION_ID	NUMERIC(15,0)   ENCODE az64
,BILL_TO_LOCATION_ID	NUMERIC(15,0)   ENCODE az64
,TERMS_ID	NUMERIC(15,0)   ENCODE az64
,SHIP_VIA_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,FOB_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,FREIGHT_TERMS_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,STATUS_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,CURRENCY_CODE	VARCHAR(15)   ENCODE lzo
,RATE_TYPE	VARCHAR(30)   ENCODE lzo
,RATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,RATE	NUMERIC(28,10)   ENCODE az64
,FROM_HEADER_ID	NUMERIC(15,0)   ENCODE az64
,FROM_TYPE_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,END_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,BLANKET_TOTAL_AMOUNT	NUMERIC(28,10)   ENCODE az64
,AUTHORIZATION_STATUS	VARCHAR(25)   ENCODE lzo
,REVISION_NUM	NUMERIC(28,10)   ENCODE az64
,REVISED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,APPROVED_FLAG	VARCHAR(1)   ENCODE lzo
,APPROVED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,AMOUNT_LIMIT	NUMERIC(28,10)   ENCODE az64
,MIN_RELEASE_AMOUNT	NUMERIC(28,10)   ENCODE az64
,NOTE_TO_AUTHORIZER	VARCHAR(240)   ENCODE lzo
,NOTE_TO_VENDOR	VARCHAR(480)   ENCODE lzo
,NOTE_TO_RECEIVER	VARCHAR(480)   ENCODE lzo
,PRINT_COUNT	NUMERIC(28,10)   ENCODE az64
,PRINTED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,VENDOR_ORDER_NUM	VARCHAR(25)   ENCODE lzo
,CONFIRMING_ORDER_FLAG	VARCHAR(1)   ENCODE lzo
,COMMENTS	VARCHAR(240)   ENCODE lzo
,REPLY_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,REPLY_METHOD_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,RFQ_CLOSE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,QUOTE_TYPE_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,QUOTATION_CLASS_CODE	VARCHAR(25)   ENCODE lzo
,QUOTE_WARNING_DELAY_UNIT	VARCHAR(25)   ENCODE lzo
,QUOTE_WARNING_DELAY	NUMERIC(28,10)   ENCODE az64
,QUOTE_VENDOR_QUOTE_NUMBER	VARCHAR(25)   ENCODE lzo
,ACCEPTANCE_REQUIRED_FLAG	VARCHAR(1)   ENCODE lzo
,ACCEPTANCE_DUE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CLOSED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,USER_HOLD_FLAG	VARCHAR(1)   ENCODE lzo
,APPROVAL_REQUIRED_FLAG	VARCHAR(1)   ENCODE lzo
,CANCEL_FLAG	VARCHAR(1)   ENCODE lzo
,FIRM_STATUS_LOOKUP_CODE	VARCHAR(30)   ENCODE lzo
,FIRM_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,FROZEN_FLAG	VARCHAR(1)   ENCODE lzo
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
,CLOSED_CODE	VARCHAR(150)   ENCODE lzo
,USSGL_TRANSACTION_CODE	VARCHAR(150)   ENCODE lzo
,GOVERNMENT_CONTEXT	VARCHAR(150)   ENCODE lzo
,REQUEST_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ORG_ID	NUMERIC(15,0)   ENCODE az64
,SUPPLY_AGREEMENT_FLAG	VARCHAR(1)   ENCODE lzo
,EDI_PROCESSED_FLAG	VARCHAR(1)   ENCODE lzo
,EDI_PROCESSED_STATUS	VARCHAR(10)   ENCODE lzo
,GLOBAL_ATTRIBUTE_CATEGORY	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE11	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE12	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE13	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE14	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE15	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE16	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE17	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE18	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE19	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE20	VARCHAR(150)   ENCODE lzo
,INTERFACE_SOURCE_CODE	VARCHAR(25)   ENCODE lzo
,REFERENCE_NUM	VARCHAR(25)   ENCODE lzo
,WF_ITEM_TYPE	VARCHAR(8)   ENCODE lzo
,WF_ITEM_KEY	VARCHAR(240)   ENCODE lzo
,MRC_RATE_TYPE	VARCHAR(2000)   ENCODE lzo
,MRC_RATE_DATE	VARCHAR(2000)   ENCODE lzo
,MRC_RATE	VARCHAR(2000)   ENCODE lzo
,PCARD_ID	NUMERIC(15,0)   ENCODE az64
,PRICE_UPDATE_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,PAY_ON_CODE	VARCHAR(25)   ENCODE lzo
,XML_FLAG	VARCHAR(3)   ENCODE lzo
,XML_SEND_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,XML_CHANGE_SEND_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,GLOBAL_AGREEMENT_FLAG	VARCHAR(1)   ENCODE lzo
,CONSIGNED_CONSUMPTION_FLAG	VARCHAR(1)   ENCODE lzo
,CBC_ACCOUNTING_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CONSUME_REQ_DEMAND_FLAG	VARCHAR(1)   ENCODE lzo
,CHANGE_REQUESTED_BY	VARCHAR(20)   ENCODE lzo
,SHIPPING_CONTROL	VARCHAR(30)   ENCODE lzo
,CONTERMS_EXIST_FLAG	VARCHAR(1)   ENCODE lzo
,CONTERMS_ARTICLES_UPD_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CONTERMS_DELIV_UPD_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ENCUMBRANCE_REQUIRED	VARCHAR(1)   ENCODE lzo
,PENDING_SIGNATURE_FLAG	VARCHAR(1)   ENCODE lzo
,CHANGE_SUMMARY	VARCHAR(2000)   ENCODE lzo
,ENCUMBRANCE_REQUIRED_FLAG	VARCHAR(1)   ENCODE lzo
,DOCUMENT_CREATION_METHOD	VARCHAR(30)   ENCODE lzo
,SUBMIT_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,SUPPLIER_NOTIF_METHOD	VARCHAR(25)   ENCODE lzo
,FAX	VARCHAR(30)   ENCODE lzo
,EMAIL_ADDRESS	VARCHAR(2000)   ENCODE lzo
,RETRO_PRICE_COMM_UPDATES_FLAG	VARCHAR(1)   ENCODE lzo
,RETRO_PRICE_APPLY_UPDATES_FLAG	VARCHAR(1)   ENCODE lzo
,UPDATE_SOURCING_RULES_FLAG	VARCHAR(1)   ENCODE lzo
,AUTO_SOURCING_FLAG	VARCHAR(1)   ENCODE lzo
,CREATED_LANGUAGE	VARCHAR(4)   ENCODE lzo
,CPA_REFERENCE	NUMERIC(28,10)   ENCODE az64
,LOCK_OWNER_ROLE	VARCHAR(10)   ENCODE lzo
,LOCK_OWNER_USER_ID	NUMERIC(15,0)   ENCODE az64
,SUPPLIER_AUTH_ENABLED_FLAG	VARCHAR(1)   ENCODE lzo
,CAT_ADMIN_AUTH_ENABLED_FLAG	VARCHAR(1)   ENCODE lzo
,STYLE_ID	NUMERIC(15,0)   ENCODE az64
,TAX_ATTRIBUTE_UPDATE_CODE	VARCHAR(15)   ENCODE lzo
,LAST_UPDATED_PROGRAM	VARCHAR(255)   ENCODE lzo
,ENABLE_ALL_SITES	VARCHAR(1)   ENCODE lzo
,PAY_WHEN_PAID	VARCHAR(1)   ENCODE lzo
	,comm_rev_num NUMERIC(15,0)   ENCODE az64
	,clm_document_number VARCHAR(50)   ENCODE lzo
	,otm_status_code VARCHAR(80)   ENCODE lzo
	,otm_recovery_flag VARCHAR(1)   ENCODE lzo
	,uda_template_id NUMERIC(15,0)   ENCODE az64
	,uda_template_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,user_document_status VARCHAR(30)   ENCODE lzo
	,ame_approval_id NUMERIC(15,0)   ENCODE az64
	,draft_id NUMERIC(15,0)   ENCODE az64
	,clm_effective_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,clm_vendor_offer_number VARCHAR(400)   ENCODE lzo
	,clm_award_administrator VARCHAR(400)   ENCODE lzo
	,clm_no_signed_copies_to_return NUMERIC(15,0)   ENCODE az64
	,clm_min_guarantee_award_amt NUMERIC(28,10)   ENCODE az64
	,clm_min_guar_award_amt_percent NUMERIC(28,10)   ENCODE az64
	,clm_min_order_amount NUMERIC(28,10)   ENCODE az64
	,clm_max_order_amount NUMERIC(28,10)   ENCODE az64
	,clm_amt_synced_to_agreement VARCHAR(1)   ENCODE lzo
	,clm_amount_released NUMERIC(38,10)   ENCODE az64
	,clm_external_idv VARCHAR(2000)   ENCODE lzo
	,clm_supplier_name VARCHAR(400)   ENCODE lzo
	,clm_supplier_site_name VARCHAR(400)   ENCODE lzo
	,clm_source_document_id NUMERIC(15,0)   ENCODE az64
	,clm_issuing_office VARCHAR(1000)   ENCODE lzo
	,clm_cotr_office VARCHAR(2000)   ENCODE lzo
	,clm_cotr_contact VARCHAR(200)   ENCODE lzo
	,clm_priority_code VARCHAR(200)   ENCODE lzo
	,clm_mod_issuing_office VARCHAR(1000)   ENCODE lzo
	,clm_standard_form VARCHAR(200)   ENCODE lzo
	,clm_document_format VARCHAR(200)   ENCODE lzo
	,ame_transaction_type VARCHAR(50)   ENCODE lzo
	,clm_award_type VARCHAR(100)   ENCODE lzo
	,clm_contract_officer NUMERIC(38,10)   ENCODE az64
	,clm_closeout_status VARCHAR(25)   ENCODE lzo
	,umbrella_program_id NUMERIC(15,0)   ENCODE az64
	,fon_ref_id NUMERIC(15,0)   ENCODE az64
	,clm_default_dist_flag VARCHAR(1)   ENCODE lzo
	,clm_edagen_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,clm_contract_finance_code VARCHAR(30)   ENCODE lzo
	,clm_payment_instr_code VARCHAR(30)   ENCODE lzo
	,clm_special_contract_type VARCHAR(30)   ENCODE lzo
	--,igt_document_number VARCHAR(30)   ENCODE lzo
	--,igt_gtnc_number VARCHAR(30)   ENCODE lzo
	--,igt_status VARCHAR(20)   ENCODE lzo
	--,agreement_type VARCHAR(10)   ENCODE lzo
	--,assisted_acquisition_ind VARCHAR(1)   ENCODE lzo
	--,advance_payment_ind VARCHAR(1)   ENCODE lzo
	--,enforce_total_amt_ind VARCHAR(1)   ENCODE lzo
	--,termination_days NUMERIC(28,10)   ENCODE az64
	--,total_remaining_amt NUMERIC(28,10)   ENCODE az64
	--,igt_business_txn_id VARCHAR(50)   ENCODE lzo
	--,order_originating_ind VARCHAR(1)   ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.po_headers_all
(
PO_HEADER_ID
,AGENT_ID
,TYPE_LOOKUP_CODE
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,SEGMENT1
,SUMMARY_FLAG
,ENABLED_FLAG
,SEGMENT2
,SEGMENT3
,SEGMENT4
,SEGMENT5
,START_DATE_ACTIVE
,END_DATE_ACTIVE
,LAST_UPDATE_LOGIN
,CREATION_DATE
,CREATED_BY
,VENDOR_ID
,VENDOR_SITE_ID
,VENDOR_CONTACT_ID
,SHIP_TO_LOCATION_ID
,BILL_TO_LOCATION_ID
,TERMS_ID
,SHIP_VIA_LOOKUP_CODE
,FOB_LOOKUP_CODE
,FREIGHT_TERMS_LOOKUP_CODE
,STATUS_LOOKUP_CODE
,CURRENCY_CODE
,RATE_TYPE
,RATE_DATE
,RATE
,FROM_HEADER_ID
,FROM_TYPE_LOOKUP_CODE
,START_DATE
,END_DATE
,BLANKET_TOTAL_AMOUNT
,AUTHORIZATION_STATUS
,REVISION_NUM
,REVISED_DATE
,APPROVED_FLAG
,APPROVED_DATE
,AMOUNT_LIMIT
,MIN_RELEASE_AMOUNT
,NOTE_TO_AUTHORIZER
,NOTE_TO_VENDOR
,NOTE_TO_RECEIVER
,PRINT_COUNT
,PRINTED_DATE
,VENDOR_ORDER_NUM
,CONFIRMING_ORDER_FLAG
,COMMENTS
,REPLY_DATE
,REPLY_METHOD_LOOKUP_CODE
,RFQ_CLOSE_DATE
,QUOTE_TYPE_LOOKUP_CODE
,QUOTATION_CLASS_CODE
,QUOTE_WARNING_DELAY_UNIT
,QUOTE_WARNING_DELAY
,QUOTE_VENDOR_QUOTE_NUMBER
,ACCEPTANCE_REQUIRED_FLAG
,ACCEPTANCE_DUE_DATE
,CLOSED_DATE
,USER_HOLD_FLAG
,APPROVAL_REQUIRED_FLAG
,CANCEL_FLAG
,FIRM_STATUS_LOOKUP_CODE
,FIRM_DATE
,FROZEN_FLAG
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
,CLOSED_CODE
,USSGL_TRANSACTION_CODE
,GOVERNMENT_CONTEXT
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,ORG_ID
,SUPPLY_AGREEMENT_FLAG
,EDI_PROCESSED_FLAG
,EDI_PROCESSED_STATUS
,GLOBAL_ATTRIBUTE_CATEGORY
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
,GLOBAL_ATTRIBUTE10
,GLOBAL_ATTRIBUTE11
,GLOBAL_ATTRIBUTE12
,GLOBAL_ATTRIBUTE13
,GLOBAL_ATTRIBUTE14
,GLOBAL_ATTRIBUTE15
,GLOBAL_ATTRIBUTE16
,GLOBAL_ATTRIBUTE17
,GLOBAL_ATTRIBUTE18
,GLOBAL_ATTRIBUTE19
,GLOBAL_ATTRIBUTE20
,INTERFACE_SOURCE_CODE
,REFERENCE_NUM
,WF_ITEM_TYPE
,WF_ITEM_KEY
,MRC_RATE_TYPE
,MRC_RATE_DATE
,MRC_RATE
,PCARD_ID
,PRICE_UPDATE_TOLERANCE
,PAY_ON_CODE
,XML_FLAG
,XML_SEND_DATE
,XML_CHANGE_SEND_DATE
,GLOBAL_AGREEMENT_FLAG
,CONSIGNED_CONSUMPTION_FLAG
,CBC_ACCOUNTING_DATE
,CONSUME_REQ_DEMAND_FLAG
,CHANGE_REQUESTED_BY
,SHIPPING_CONTROL
,CONTERMS_EXIST_FLAG
,CONTERMS_ARTICLES_UPD_DATE
,CONTERMS_DELIV_UPD_DATE
--,ENCUMBRANCE_REQUIRED
,PENDING_SIGNATURE_FLAG
,CHANGE_SUMMARY
,ENCUMBRANCE_REQUIRED_FLAG
,DOCUMENT_CREATION_METHOD
,SUBMIT_DATE
,SUPPLIER_NOTIF_METHOD
,FAX
,EMAIL_ADDRESS
,RETRO_PRICE_COMM_UPDATES_FLAG
,RETRO_PRICE_APPLY_UPDATES_FLAG
,UPDATE_SOURCING_RULES_FLAG
,AUTO_SOURCING_FLAG
,CREATED_LANGUAGE
,CPA_REFERENCE
,LOCK_OWNER_ROLE
,LOCK_OWNER_USER_ID
,SUPPLIER_AUTH_ENABLED_FLAG
,CAT_ADMIN_AUTH_ENABLED_FLAG
,STYLE_ID
,TAX_ATTRIBUTE_UPDATE_CODE
,LAST_UPDATED_PROGRAM
,ENABLE_ALL_SITES
,PAY_WHEN_PAID,
	comm_rev_num,
	clm_document_number,
	otm_status_code,
	otm_recovery_flag,
	uda_template_id,
	uda_template_date,
	user_document_status,
	ame_approval_id,
	draft_id,
	clm_effective_date,
	clm_vendor_offer_number,
	clm_award_administrator,
	clm_no_signed_copies_to_return,
	clm_min_guarantee_award_amt,
	clm_min_guar_award_amt_percent,
	clm_min_order_amount,
	clm_max_order_amount,
	clm_amt_synced_to_agreement,
	clm_amount_released,
	clm_external_idv,
	clm_supplier_name,
	clm_supplier_site_name,
	clm_source_document_id,
	clm_issuing_office,
	clm_cotr_office,
	clm_cotr_contact,
	clm_priority_code,
	clm_mod_issuing_office,
	clm_standard_form,
	clm_document_format,
	ame_transaction_type,
	clm_award_type,
	clm_contract_officer,
	clm_closeout_status,
	umbrella_program_id,
	fon_ref_id,
	clm_default_dist_flag,
	clm_edagen_date,
	clm_contract_finance_code,
	clm_payment_instr_code,
	clm_special_contract_type,
	--igt_document_number,
	--igt_gtnc_number,
	--igt_status,
	--agreement_type,
	--assisted_acquisition_ind,
	--advance_payment_ind,
	--enforce_total_amt_ind,
	--termination_days,
	--total_remaining_amt,
	--igt_business_txn_id,
	--order_originating_ind
KCA_OPERATION
,IS_DELETED_FLG
,kca_seq_id
,kca_seq_date
)
(
select
PO_HEADER_ID
,AGENT_ID
,TYPE_LOOKUP_CODE
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,SEGMENT1
,SUMMARY_FLAG
,ENABLED_FLAG
,SEGMENT2
,SEGMENT3
,SEGMENT4
,SEGMENT5
,START_DATE_ACTIVE
,END_DATE_ACTIVE
,LAST_UPDATE_LOGIN
,CREATION_DATE
,CREATED_BY
,VENDOR_ID
,VENDOR_SITE_ID
,VENDOR_CONTACT_ID
,SHIP_TO_LOCATION_ID
,BILL_TO_LOCATION_ID
,TERMS_ID
,SHIP_VIA_LOOKUP_CODE
,FOB_LOOKUP_CODE
,FREIGHT_TERMS_LOOKUP_CODE
,STATUS_LOOKUP_CODE
,CURRENCY_CODE
,RATE_TYPE
,RATE_DATE
,RATE
,FROM_HEADER_ID
,FROM_TYPE_LOOKUP_CODE
,START_DATE
,END_DATE
,BLANKET_TOTAL_AMOUNT
,AUTHORIZATION_STATUS
,REVISION_NUM
,REVISED_DATE
,APPROVED_FLAG
,APPROVED_DATE
,AMOUNT_LIMIT
,MIN_RELEASE_AMOUNT
,NOTE_TO_AUTHORIZER
,NOTE_TO_VENDOR
,NOTE_TO_RECEIVER
,PRINT_COUNT
,PRINTED_DATE
,VENDOR_ORDER_NUM
,CONFIRMING_ORDER_FLAG
,COMMENTS
,REPLY_DATE
,REPLY_METHOD_LOOKUP_CODE
,RFQ_CLOSE_DATE
,QUOTE_TYPE_LOOKUP_CODE
,QUOTATION_CLASS_CODE
,QUOTE_WARNING_DELAY_UNIT
,QUOTE_WARNING_DELAY
,QUOTE_VENDOR_QUOTE_NUMBER
,ACCEPTANCE_REQUIRED_FLAG
,ACCEPTANCE_DUE_DATE
,CLOSED_DATE
,USER_HOLD_FLAG
,APPROVAL_REQUIRED_FLAG
,CANCEL_FLAG
,FIRM_STATUS_LOOKUP_CODE
,FIRM_DATE
,FROZEN_FLAG
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
,CLOSED_CODE
,USSGL_TRANSACTION_CODE
,GOVERNMENT_CONTEXT
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,ORG_ID
,SUPPLY_AGREEMENT_FLAG
,EDI_PROCESSED_FLAG
,EDI_PROCESSED_STATUS
,GLOBAL_ATTRIBUTE_CATEGORY
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
,GLOBAL_ATTRIBUTE10
,GLOBAL_ATTRIBUTE11
,GLOBAL_ATTRIBUTE12
,GLOBAL_ATTRIBUTE13
,GLOBAL_ATTRIBUTE14
,GLOBAL_ATTRIBUTE15
,GLOBAL_ATTRIBUTE16
,GLOBAL_ATTRIBUTE17
,GLOBAL_ATTRIBUTE18
,GLOBAL_ATTRIBUTE19
,GLOBAL_ATTRIBUTE20
,INTERFACE_SOURCE_CODE
,REFERENCE_NUM
,WF_ITEM_TYPE
,WF_ITEM_KEY
,MRC_RATE_TYPE
,MRC_RATE_DATE
,MRC_RATE
,PCARD_ID
,PRICE_UPDATE_TOLERANCE
,PAY_ON_CODE
,XML_FLAG
,XML_SEND_DATE
,XML_CHANGE_SEND_DATE
,GLOBAL_AGREEMENT_FLAG
,CONSIGNED_CONSUMPTION_FLAG
,CBC_ACCOUNTING_DATE
,CONSUME_REQ_DEMAND_FLAG
,CHANGE_REQUESTED_BY
,SHIPPING_CONTROL
,CONTERMS_EXIST_FLAG
,CONTERMS_ARTICLES_UPD_DATE
,CONTERMS_DELIV_UPD_DATE
--,ENCUMBRANCE_REQUIRED
,PENDING_SIGNATURE_FLAG
,CHANGE_SUMMARY
,ENCUMBRANCE_REQUIRED_FLAG
,DOCUMENT_CREATION_METHOD
,SUBMIT_DATE
,SUPPLIER_NOTIF_METHOD
,FAX
,EMAIL_ADDRESS
,RETRO_PRICE_COMM_UPDATES_FLAG
,RETRO_PRICE_APPLY_UPDATES_FLAG
,UPDATE_SOURCING_RULES_FLAG
,AUTO_SOURCING_FLAG
,CREATED_LANGUAGE
,CPA_REFERENCE
,LOCK_OWNER_ROLE
,LOCK_OWNER_USER_ID
,SUPPLIER_AUTH_ENABLED_FLAG
,CAT_ADMIN_AUTH_ENABLED_FLAG
,STYLE_ID
,TAX_ATTRIBUTE_UPDATE_CODE
,LAST_UPDATED_PROGRAM
,ENABLE_ALL_SITES
,PAY_WHEN_PAID,
	comm_rev_num,
	clm_document_number,
	otm_status_code,
	otm_recovery_flag,
	uda_template_id,
	uda_template_date,
	user_document_status,
	ame_approval_id,
	draft_id,
	clm_effective_date,
	clm_vendor_offer_number,
	clm_award_administrator,
	clm_no_signed_copies_to_return,
	clm_min_guarantee_award_amt,
	clm_min_guar_award_amt_percent,
	clm_min_order_amount,
	clm_max_order_amount,
	clm_amt_synced_to_agreement,
	clm_amount_released,
	clm_external_idv,
	clm_supplier_name,
	clm_supplier_site_name,
	clm_source_document_id,
	clm_issuing_office,
	clm_cotr_office,
	clm_cotr_contact,
	clm_priority_code,
	clm_mod_issuing_office,
	clm_standard_form,
	clm_document_format,
	ame_transaction_type,
	clm_award_type,
	clm_contract_officer,
	clm_closeout_status,
	umbrella_program_id,
	fon_ref_id,
	clm_default_dist_flag,
	clm_edagen_date,
	clm_contract_finance_code,
	clm_payment_instr_code,
	clm_special_contract_type,
	--igt_document_number,
	--igt_gtnc_number,
	--igt_status,
	--agreement_type,
	--assisted_acquisition_ind,
	--advance_payment_ind,
	--enforce_total_amt_ind,
	--termination_days,
	--total_remaining_amt,
	--igt_business_txn_id,
	--order_originating_ind
KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID 
,kca_seq_date
from bec_ods_stg.po_headers_all
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='po_headers_all'; 

commit;