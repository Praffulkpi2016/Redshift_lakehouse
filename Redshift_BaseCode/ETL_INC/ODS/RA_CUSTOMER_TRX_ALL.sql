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

delete from bec_ods.RA_CUSTOMER_TRX_ALL
where CUSTOMER_TRX_ID in (
select stg.CUSTOMER_TRX_ID from bec_ods.RA_CUSTOMER_TRX_ALL ods,  bec_ods_stg.RA_CUSTOMER_TRX_ALL stg
where ods.CUSTOMER_TRX_ID = stg.CUSTOMER_TRX_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.RA_CUSTOMER_TRX_ALL
(
CUSTOMER_TRX_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,TRX_NUMBER
,CUST_TRX_TYPE_ID
,TRX_DATE
,SET_OF_BOOKS_ID
,BILL_TO_CONTACT_ID
,BATCH_ID
,BATCH_SOURCE_ID
,REASON_CODE
,SOLD_TO_CUSTOMER_ID
,SOLD_TO_CONTACT_ID
,SOLD_TO_SITE_USE_ID
,BILL_TO_CUSTOMER_ID
,BILL_TO_SITE_USE_ID
,SHIP_TO_CUSTOMER_ID
,SHIP_TO_CONTACT_ID
,SHIP_TO_SITE_USE_ID
,SHIPMENT_ID
,REMIT_TO_ADDRESS_ID
,TERM_ID
,TERM_DUE_DATE
,PREVIOUS_CUSTOMER_TRX_ID
,PRIMARY_SALESREP_ID
,PRINTING_ORIGINAL_DATE
,PRINTING_LAST_PRINTED
,PRINTING_OPTION
,PRINTING_COUNT
,PRINTING_PENDING
,PURCHASE_ORDER
,PURCHASE_ORDER_REVISION
,PURCHASE_ORDER_DATE
,CUSTOMER_REFERENCE
,CUSTOMER_REFERENCE_DATE
,COMMENTS
,INTERNAL_NOTES
,EXCHANGE_RATE_TYPE
,EXCHANGE_DATE
,EXCHANGE_RATE
,TERRITORY_ID
,INVOICE_CURRENCY_CODE
,INITIAL_CUSTOMER_TRX_ID
,AGREEMENT_ID
,END_DATE_COMMITMENT
,START_DATE_COMMITMENT
,LAST_PRINTED_SEQUENCE_NUM
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
,ORIG_SYSTEM_BATCH_NAME
,POST_REQUEST_ID
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,FINANCE_CHARGES
,COMPLETE_FLAG
,POSTING_CONTROL_ID
,BILL_TO_ADDRESS_ID
,RA_POST_LOOP_NUMBER
,SHIP_TO_ADDRESS_ID
,CREDIT_METHOD_FOR_RULES
,CREDIT_METHOD_FOR_INSTALLMENTS
,RECEIPT_METHOD_ID
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,RELATED_CUSTOMER_TRX_ID
,INVOICING_RULE_ID
,SHIP_VIA
,SHIP_DATE_ACTUAL
,WAYBILL_NUMBER
,FOB_POINT
,CUSTOMER_BANK_ACCOUNT_ID
,INTERFACE_HEADER_ATTRIBUTE1
,INTERFACE_HEADER_ATTRIBUTE2
,INTERFACE_HEADER_ATTRIBUTE3
,INTERFACE_HEADER_ATTRIBUTE4
,INTERFACE_HEADER_ATTRIBUTE5
,INTERFACE_HEADER_ATTRIBUTE6
,INTERFACE_HEADER_ATTRIBUTE7
,INTERFACE_HEADER_ATTRIBUTE8
,INTERFACE_HEADER_CONTEXT
,DEFAULT_USSGL_TRX_CODE_CONTEXT
,INTERFACE_HEADER_ATTRIBUTE10
,INTERFACE_HEADER_ATTRIBUTE11
,INTERFACE_HEADER_ATTRIBUTE12
,INTERFACE_HEADER_ATTRIBUTE13
,INTERFACE_HEADER_ATTRIBUTE14
,INTERFACE_HEADER_ATTRIBUTE15
,INTERFACE_HEADER_ATTRIBUTE9
,DEFAULT_USSGL_TRANSACTION_CODE
,RECURRED_FROM_TRX_NUMBER
,STATUS_TRX
,DOC_SEQUENCE_ID
,DOC_SEQUENCE_VALUE
,PAYING_CUSTOMER_ID
,PAYING_SITE_USE_ID
,RELATED_BATCH_SOURCE_ID
,DEFAULT_TAX_EXEMPT_FLAG
,CREATED_FROM
,ORG_ID
,WH_UPDATE_DATE
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
,GLOBAL_ATTRIBUTE_CATEGORY
,EDI_PROCESSED_FLAG
,EDI_PROCESSED_STATUS
,GLOBAL_ATTRIBUTE21
,GLOBAL_ATTRIBUTE22
,GLOBAL_ATTRIBUTE23
,GLOBAL_ATTRIBUTE24
,GLOBAL_ATTRIBUTE25
,GLOBAL_ATTRIBUTE26
,GLOBAL_ATTRIBUTE27
,GLOBAL_ATTRIBUTE28
,GLOBAL_ATTRIBUTE29
,GLOBAL_ATTRIBUTE30
,MRC_EXCHANGE_RATE_TYPE
,MRC_EXCHANGE_DATE
,MRC_EXCHANGE_RATE
,PAYMENT_SERVER_ORDER_NUM
,APPROVAL_CODE
,ADDRESS_VERIFICATION_CODE
,OLD_TRX_NUMBER
,BR_AMOUNT
,BR_UNPAID_FLAG
,BR_ON_HOLD_FLAG
,DRAWEE_ID
,DRAWEE_CONTACT_ID
,DRAWEE_SITE_USE_ID
,REMITTANCE_BANK_ACCOUNT_ID
,OVERRIDE_REMIT_ACCOUNT_FLAG
,DRAWEE_BANK_ACCOUNT_ID
,SPECIAL_INSTRUCTIONS
,REMITTANCE_BATCH_ID
,PREPAYMENT_FLAG
,CT_REFERENCE
,CONTRACT_ID
,BILL_TEMPLATE_ID
,REVERSED_CASH_RECEIPT_ID
,CC_ERROR_CODE
,CC_ERROR_TEXT
,CC_ERROR_FLAG
,UPGRADE_METHOD
,LEGAL_ENTITY_ID
,REMIT_BANK_ACCT_USE_ID
,PAYMENT_TRXN_EXTENSION_ID
,AX_ACCOUNTED_FLAG
,APPLICATION_ID
,PAYMENT_ATTRIBUTES
,BILLING_DATE
,INTEREST_HEADER_ID
,LATE_CHARGES_ASSESSED,
	trailer_number,
	rev_rec_application,
	document_type_id,
	document_creation_date,
	src_invoicing_rule_id,
	billing_ext_request
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date
)
(
	select
CUSTOMER_TRX_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,TRX_NUMBER
,CUST_TRX_TYPE_ID
,TRX_DATE
,SET_OF_BOOKS_ID
,BILL_TO_CONTACT_ID
,BATCH_ID
,BATCH_SOURCE_ID
,REASON_CODE
,SOLD_TO_CUSTOMER_ID
,SOLD_TO_CONTACT_ID
,SOLD_TO_SITE_USE_ID
,BILL_TO_CUSTOMER_ID
,BILL_TO_SITE_USE_ID
,SHIP_TO_CUSTOMER_ID
,SHIP_TO_CONTACT_ID
,SHIP_TO_SITE_USE_ID
,SHIPMENT_ID
,REMIT_TO_ADDRESS_ID
,TERM_ID
,TERM_DUE_DATE
,PREVIOUS_CUSTOMER_TRX_ID
,PRIMARY_SALESREP_ID
,PRINTING_ORIGINAL_DATE
,PRINTING_LAST_PRINTED
,PRINTING_OPTION
,PRINTING_COUNT
,PRINTING_PENDING
,PURCHASE_ORDER
,PURCHASE_ORDER_REVISION
,PURCHASE_ORDER_DATE
,CUSTOMER_REFERENCE
,CUSTOMER_REFERENCE_DATE
,COMMENTS
,INTERNAL_NOTES
,EXCHANGE_RATE_TYPE
,EXCHANGE_DATE
,EXCHANGE_RATE
,TERRITORY_ID
,INVOICE_CURRENCY_CODE
,INITIAL_CUSTOMER_TRX_ID
,AGREEMENT_ID
,END_DATE_COMMITMENT
,START_DATE_COMMITMENT
,LAST_PRINTED_SEQUENCE_NUM
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
,ORIG_SYSTEM_BATCH_NAME
,POST_REQUEST_ID
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,FINANCE_CHARGES
,COMPLETE_FLAG
,POSTING_CONTROL_ID
,BILL_TO_ADDRESS_ID
,RA_POST_LOOP_NUMBER
,SHIP_TO_ADDRESS_ID
,CREDIT_METHOD_FOR_RULES
,CREDIT_METHOD_FOR_INSTALLMENTS
,RECEIPT_METHOD_ID
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,RELATED_CUSTOMER_TRX_ID
,INVOICING_RULE_ID
,SHIP_VIA
,SHIP_DATE_ACTUAL
,WAYBILL_NUMBER
,FOB_POINT
,CUSTOMER_BANK_ACCOUNT_ID
,INTERFACE_HEADER_ATTRIBUTE1
,INTERFACE_HEADER_ATTRIBUTE2
,INTERFACE_HEADER_ATTRIBUTE3
,INTERFACE_HEADER_ATTRIBUTE4
,INTERFACE_HEADER_ATTRIBUTE5
,INTERFACE_HEADER_ATTRIBUTE6
,INTERFACE_HEADER_ATTRIBUTE7
,INTERFACE_HEADER_ATTRIBUTE8
,INTERFACE_HEADER_CONTEXT
,DEFAULT_USSGL_TRX_CODE_CONTEXT
,INTERFACE_HEADER_ATTRIBUTE10
,INTERFACE_HEADER_ATTRIBUTE11
,INTERFACE_HEADER_ATTRIBUTE12
,INTERFACE_HEADER_ATTRIBUTE13
,INTERFACE_HEADER_ATTRIBUTE14
,INTERFACE_HEADER_ATTRIBUTE15
,INTERFACE_HEADER_ATTRIBUTE9
,DEFAULT_USSGL_TRANSACTION_CODE
,RECURRED_FROM_TRX_NUMBER
,STATUS_TRX
,DOC_SEQUENCE_ID
,DOC_SEQUENCE_VALUE
,PAYING_CUSTOMER_ID
,PAYING_SITE_USE_ID
,RELATED_BATCH_SOURCE_ID
,DEFAULT_TAX_EXEMPT_FLAG
,CREATED_FROM
,ORG_ID
,WH_UPDATE_DATE
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
,GLOBAL_ATTRIBUTE_CATEGORY
,EDI_PROCESSED_FLAG
,EDI_PROCESSED_STATUS
,GLOBAL_ATTRIBUTE21
,GLOBAL_ATTRIBUTE22
,GLOBAL_ATTRIBUTE23
,GLOBAL_ATTRIBUTE24
,GLOBAL_ATTRIBUTE25
,GLOBAL_ATTRIBUTE26
,GLOBAL_ATTRIBUTE27
,GLOBAL_ATTRIBUTE28
,GLOBAL_ATTRIBUTE29
,GLOBAL_ATTRIBUTE30
,MRC_EXCHANGE_RATE_TYPE
,MRC_EXCHANGE_DATE
,MRC_EXCHANGE_RATE
,PAYMENT_SERVER_ORDER_NUM
,APPROVAL_CODE
,ADDRESS_VERIFICATION_CODE
,OLD_TRX_NUMBER
,BR_AMOUNT
,BR_UNPAID_FLAG
,BR_ON_HOLD_FLAG
,DRAWEE_ID
,DRAWEE_CONTACT_ID
,DRAWEE_SITE_USE_ID
,REMITTANCE_BANK_ACCOUNT_ID
,OVERRIDE_REMIT_ACCOUNT_FLAG
,DRAWEE_BANK_ACCOUNT_ID
,SPECIAL_INSTRUCTIONS
,REMITTANCE_BATCH_ID
,PREPAYMENT_FLAG
,CT_REFERENCE
,CONTRACT_ID
,BILL_TEMPLATE_ID
,REVERSED_CASH_RECEIPT_ID
,CC_ERROR_CODE
,CC_ERROR_TEXT
,CC_ERROR_FLAG
,UPGRADE_METHOD
,LEGAL_ENTITY_ID
,REMIT_BANK_ACCT_USE_ID
,PAYMENT_TRXN_EXTENSION_ID
,AX_ACCOUNTED_FLAG
,APPLICATION_ID
,PAYMENT_ATTRIBUTES
,BILLING_DATE
,INTEREST_HEADER_ID
,LATE_CHARGES_ASSESSED,
	trailer_number,
	rev_rec_application,
	document_type_id,
	document_creation_date,
	src_invoicing_rule_id,
	billing_ext_request
,KCA_OPERATION
      , 'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.RA_CUSTOMER_TRX_ALL
	where kca_operation in ('INSERT','UPDATE') 
	and (CUSTOMER_TRX_ID,kca_seq_id) in 
	(select CUSTOMER_TRX_ID,max(kca_seq_id) from bec_ods_stg.RA_CUSTOMER_TRX_ALL 
     where kca_operation in ('INSERT','UPDATE')
     group by CUSTOMER_TRX_ID)
);

commit;



-- Soft delete
update bec_ods.RA_CUSTOMER_TRX_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RA_CUSTOMER_TRX_ALL set IS_DELETED_FLG = 'Y'
where (CUSTOMER_TRX_ID)  in
(
select CUSTOMER_TRX_ID from bec_raw_dl_ext.RA_CUSTOMER_TRX_ALL
where (CUSTOMER_TRX_ID,KCA_SEQ_ID)
in 
(
select CUSTOMER_TRX_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RA_CUSTOMER_TRX_ALL
group by CUSTOMER_TRX_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ra_customer_trx_all';

commit;