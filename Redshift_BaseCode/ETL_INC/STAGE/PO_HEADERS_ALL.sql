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

truncate
	table bec_ods_stg.po_headers_all;

insert
	into
	bec_ods_stg.po_headers_all
(PO_HEADER_ID,
AGENT_ID,
TYPE_LOOKUP_CODE,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
SEGMENT1,
SUMMARY_FLAG,
ENABLED_FLAG,
SEGMENT2,
SEGMENT3,
SEGMENT4,
SEGMENT5,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
LAST_UPDATE_LOGIN,
CREATION_DATE,
CREATED_BY,
VENDOR_ID,
VENDOR_SITE_ID,
VENDOR_CONTACT_ID,
SHIP_TO_LOCATION_ID,
BILL_TO_LOCATION_ID,
TERMS_ID,
SHIP_VIA_LOOKUP_CODE,
FOB_LOOKUP_CODE,
FREIGHT_TERMS_LOOKUP_CODE,
STATUS_LOOKUP_CODE,
CURRENCY_CODE,
RATE_TYPE,
RATE_DATE,
RATE,
FROM_HEADER_ID,
FROM_TYPE_LOOKUP_CODE,
START_DATE,
END_DATE,
BLANKET_TOTAL_AMOUNT,
AUTHORIZATION_STATUS,
REVISION_NUM,
REVISED_DATE,
APPROVED_FLAG,
APPROVED_DATE,
AMOUNT_LIMIT,
MIN_RELEASE_AMOUNT,
NOTE_TO_AUTHORIZER,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
PRINT_COUNT,
PRINTED_DATE,
VENDOR_ORDER_NUM,
CONFIRMING_ORDER_FLAG,
COMMENTS,
REPLY_DATE,
REPLY_METHOD_LOOKUP_CODE,
RFQ_CLOSE_DATE,
QUOTE_TYPE_LOOKUP_CODE,
QUOTATION_CLASS_CODE,
QUOTE_WARNING_DELAY_UNIT,
QUOTE_WARNING_DELAY,
QUOTE_VENDOR_QUOTE_NUMBER,
ACCEPTANCE_REQUIRED_FLAG,
ACCEPTANCE_DUE_DATE,
CLOSED_DATE,
USER_HOLD_FLAG,
APPROVAL_REQUIRED_FLAG,
CANCEL_FLAG,
FIRM_STATUS_LOOKUP_CODE,
FIRM_DATE,
FROZEN_FLAG,
ATTRIBUTE_CATEGORY,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
ATTRIBUTE9,
ATTRIBUTE10,
ATTRIBUTE11,
ATTRIBUTE12,
ATTRIBUTE13,
ATTRIBUTE14,
ATTRIBUTE15,
CLOSED_CODE,
USSGL_TRANSACTION_CODE,
GOVERNMENT_CONTEXT,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
ORG_ID,
SUPPLY_AGREEMENT_FLAG,
EDI_PROCESSED_FLAG,
EDI_PROCESSED_STATUS,
GLOBAL_ATTRIBUTE_CATEGORY,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE3,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE5,
GLOBAL_ATTRIBUTE6,
GLOBAL_ATTRIBUTE7,
GLOBAL_ATTRIBUTE8,
GLOBAL_ATTRIBUTE9,
GLOBAL_ATTRIBUTE10,
GLOBAL_ATTRIBUTE11,
GLOBAL_ATTRIBUTE12,
GLOBAL_ATTRIBUTE13,
GLOBAL_ATTRIBUTE14,
GLOBAL_ATTRIBUTE15,
GLOBAL_ATTRIBUTE16,
GLOBAL_ATTRIBUTE17,
GLOBAL_ATTRIBUTE18,
GLOBAL_ATTRIBUTE19,
GLOBAL_ATTRIBUTE20,
INTERFACE_SOURCE_CODE,
REFERENCE_NUM,
WF_ITEM_TYPE,
WF_ITEM_KEY,
MRC_RATE_TYPE,
MRC_RATE_DATE,
MRC_RATE,
PCARD_ID,
PRICE_UPDATE_TOLERANCE,
PAY_ON_CODE,
XML_FLAG,
XML_SEND_DATE,
XML_CHANGE_SEND_DATE,
GLOBAL_AGREEMENT_FLAG,
CONSIGNED_CONSUMPTION_FLAG,
CBC_ACCOUNTING_DATE,
CONSUME_REQ_DEMAND_FLAG,
CHANGE_REQUESTED_BY,
SHIPPING_CONTROL,
CONTERMS_EXIST_FLAG,
CONTERMS_ARTICLES_UPD_DATE,
CONTERMS_DELIV_UPD_DATE,
--ENCUMBRANCE_REQUIRED,
PENDING_SIGNATURE_FLAG,
CHANGE_SUMMARY,
ENCUMBRANCE_REQUIRED_FLAG,
DOCUMENT_CREATION_METHOD,
SUBMIT_DATE,
SUPPLIER_NOTIF_METHOD,
FAX,
EMAIL_ADDRESS,
RETRO_PRICE_COMM_UPDATES_FLAG,
RETRO_PRICE_APPLY_UPDATES_FLAG,
UPDATE_SOURCING_RULES_FLAG,
AUTO_SOURCING_FLAG,
CREATED_LANGUAGE,
CPA_REFERENCE,
LOCK_OWNER_ROLE,
LOCK_OWNER_USER_ID,
SUPPLIER_AUTH_ENABLED_FLAG,
CAT_ADMIN_AUTH_ENABLED_FLAG,
STYLE_ID,
TAX_ATTRIBUTE_UPDATE_CODE,
LAST_UPDATED_PROGRAM,
ENABLE_ALL_SITES,
PAY_WHEN_PAID,
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
	--order_originating_ind,
KCA_OPERATION
,kca_seq_id
,KCA_SEQ_DATE)
(
	select
	PO_HEADER_ID,
AGENT_ID,
TYPE_LOOKUP_CODE,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
SEGMENT1,
SUMMARY_FLAG,
ENABLED_FLAG,
SEGMENT2,
SEGMENT3,
SEGMENT4,
SEGMENT5,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
LAST_UPDATE_LOGIN,
CREATION_DATE,
CREATED_BY,
VENDOR_ID,
VENDOR_SITE_ID,
VENDOR_CONTACT_ID,
SHIP_TO_LOCATION_ID,
BILL_TO_LOCATION_ID,
TERMS_ID,
SHIP_VIA_LOOKUP_CODE,
FOB_LOOKUP_CODE,
FREIGHT_TERMS_LOOKUP_CODE,
STATUS_LOOKUP_CODE,
CURRENCY_CODE,
RATE_TYPE,
RATE_DATE,
RATE,
FROM_HEADER_ID,
FROM_TYPE_LOOKUP_CODE,
START_DATE,
END_DATE,
BLANKET_TOTAL_AMOUNT,
AUTHORIZATION_STATUS,
REVISION_NUM,
REVISED_DATE,
APPROVED_FLAG,
APPROVED_DATE,
AMOUNT_LIMIT,
MIN_RELEASE_AMOUNT,
NOTE_TO_AUTHORIZER,
NOTE_TO_VENDOR,
NOTE_TO_RECEIVER,
PRINT_COUNT,
PRINTED_DATE,
VENDOR_ORDER_NUM,
CONFIRMING_ORDER_FLAG,
COMMENTS,
REPLY_DATE,
REPLY_METHOD_LOOKUP_CODE,
RFQ_CLOSE_DATE,
QUOTE_TYPE_LOOKUP_CODE,
QUOTATION_CLASS_CODE,
QUOTE_WARNING_DELAY_UNIT,
QUOTE_WARNING_DELAY,
QUOTE_VENDOR_QUOTE_NUMBER,
ACCEPTANCE_REQUIRED_FLAG,
ACCEPTANCE_DUE_DATE,
CLOSED_DATE,
USER_HOLD_FLAG,
APPROVAL_REQUIRED_FLAG,
CANCEL_FLAG,
FIRM_STATUS_LOOKUP_CODE,
FIRM_DATE,
FROZEN_FLAG,
ATTRIBUTE_CATEGORY,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
ATTRIBUTE9,
ATTRIBUTE10,
ATTRIBUTE11,
ATTRIBUTE12,
ATTRIBUTE13,
ATTRIBUTE14,
ATTRIBUTE15,
CLOSED_CODE,
USSGL_TRANSACTION_CODE,
GOVERNMENT_CONTEXT,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
ORG_ID,
SUPPLY_AGREEMENT_FLAG,
EDI_PROCESSED_FLAG,
EDI_PROCESSED_STATUS,
GLOBAL_ATTRIBUTE_CATEGORY,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE3,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE5,
GLOBAL_ATTRIBUTE6,
GLOBAL_ATTRIBUTE7,
GLOBAL_ATTRIBUTE8,
GLOBAL_ATTRIBUTE9,
GLOBAL_ATTRIBUTE10,
GLOBAL_ATTRIBUTE11,
GLOBAL_ATTRIBUTE12,
GLOBAL_ATTRIBUTE13,
GLOBAL_ATTRIBUTE14,
GLOBAL_ATTRIBUTE15,
GLOBAL_ATTRIBUTE16,
GLOBAL_ATTRIBUTE17,
GLOBAL_ATTRIBUTE18,
GLOBAL_ATTRIBUTE19,
GLOBAL_ATTRIBUTE20,
INTERFACE_SOURCE_CODE,
REFERENCE_NUM,
WF_ITEM_TYPE,
WF_ITEM_KEY,
MRC_RATE_TYPE,
MRC_RATE_DATE,
MRC_RATE,
PCARD_ID,
PRICE_UPDATE_TOLERANCE,
PAY_ON_CODE,
XML_FLAG,
XML_SEND_DATE,
XML_CHANGE_SEND_DATE,
GLOBAL_AGREEMENT_FLAG,
CONSIGNED_CONSUMPTION_FLAG,
CBC_ACCOUNTING_DATE,
CONSUME_REQ_DEMAND_FLAG,
CHANGE_REQUESTED_BY,
SHIPPING_CONTROL,
CONTERMS_EXIST_FLAG,
CONTERMS_ARTICLES_UPD_DATE,
CONTERMS_DELIV_UPD_DATE,
--ENCUMBRANCE_REQUIRED,
PENDING_SIGNATURE_FLAG,
CHANGE_SUMMARY,
ENCUMBRANCE_REQUIRED_FLAG,
DOCUMENT_CREATION_METHOD,
SUBMIT_DATE,
SUPPLIER_NOTIF_METHOD,
FAX,
EMAIL_ADDRESS,
RETRO_PRICE_COMM_UPDATES_FLAG,
RETRO_PRICE_APPLY_UPDATES_FLAG,
UPDATE_SOURCING_RULES_FLAG,
AUTO_SOURCING_FLAG,
CREATED_LANGUAGE,
CPA_REFERENCE,
LOCK_OWNER_ROLE,
LOCK_OWNER_USER_ID,
SUPPLIER_AUTH_ENABLED_FLAG,
CAT_ADMIN_AUTH_ENABLED_FLAG,
STYLE_ID,
TAX_ATTRIBUTE_UPDATE_CODE,
LAST_UPDATED_PROGRAM,
ENABLE_ALL_SITES,
PAY_WHEN_PAID,
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
	--order_originating_ind,
KCA_OPERATION,
kca_seq_id
,KCA_SEQ_DATE
	from
		bec_raw_dl_ext.po_headers_all
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' and (PO_HEADER_ID,kca_seq_id) in (select PO_HEADER_ID,max(kca_seq_id) from bec_raw_dl_ext.po_headers_all 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
group by PO_HEADER_ID)
and 
(KCA_SEQ_DATE > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'po_headers_all')
            )
);
end;
