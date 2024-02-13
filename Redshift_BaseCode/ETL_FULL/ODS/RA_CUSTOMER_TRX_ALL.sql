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

DROP TABLE if exists bec_ods.RA_CUSTOMER_TRX_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.RA_CUSTOMER_TRX_ALL
(

CUSTOMER_TRX_ID NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64
,CREATION_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64
,TRX_NUMBER VARCHAR(20)   ENCODE lzo
,CUST_TRX_TYPE_ID NUMERIC(15,0)   ENCODE az64
,TRX_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,SET_OF_BOOKS_ID NUMERIC(15,0)   ENCODE az64
,BILL_TO_CONTACT_ID NUMERIC(15,0)   ENCODE az64
,BATCH_ID NUMERIC(15,0)   ENCODE az64
,BATCH_SOURCE_ID NUMERIC(15,0)   ENCODE az64
,REASON_CODE VARCHAR(30)   ENCODE lzo
,SOLD_TO_CUSTOMER_ID NUMERIC(15,0)   ENCODE az64
,SOLD_TO_CONTACT_ID NUMERIC(15,0)   ENCODE az64
,SOLD_TO_SITE_USE_ID NUMERIC(15,0)   ENCODE az64
,BILL_TO_CUSTOMER_ID NUMERIC(15,0)   ENCODE az64
,BILL_TO_SITE_USE_ID NUMERIC(15,0)   ENCODE az64
,SHIP_TO_CUSTOMER_ID NUMERIC(15,0)   ENCODE az64
,SHIP_TO_CONTACT_ID NUMERIC(15,0)   ENCODE az64
,SHIP_TO_SITE_USE_ID NUMERIC(15,0)   ENCODE az64
,SHIPMENT_ID NUMERIC(15,0)   ENCODE az64
,REMIT_TO_ADDRESS_ID NUMERIC(15,0)   ENCODE az64
,TERM_ID NUMERIC(15,0)   ENCODE az64
,TERM_DUE_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,PREVIOUS_CUSTOMER_TRX_ID NUMERIC(15,0)   ENCODE az64
,PRIMARY_SALESREP_ID NUMERIC(15,0)   ENCODE az64
,PRINTING_ORIGINAL_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,PRINTING_LAST_PRINTED  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,PRINTING_OPTION VARCHAR(20)   ENCODE lzo
,PRINTING_COUNT NUMERIC(15,0)   ENCODE az64
,PRINTING_PENDING	 VARCHAR(1)   ENCODE lzo
,PURCHASE_ORDER	 VARCHAR(50)   ENCODE lzo
,PURCHASE_ORDER_REVISION	 VARCHAR(50)   ENCODE lzo
,PURCHASE_ORDER_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CUSTOMER_REFERENCE VARCHAR(30)   ENCODE lzo
,CUSTOMER_REFERENCE_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,COMMENTS	VARCHAR(1760)   ENCODE lzo
,INTERNAL_NOTES	VARCHAR(240)   ENCODE lzo
,EXCHANGE_RATE_TYPE VARCHAR(30)   ENCODE lzo
,EXCHANGE_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,EXCHANGE_RATE	NUMERIC(28,10)   ENCODE az64
,TERRITORY_ID NUMERIC(15,0)   ENCODE az64
,INVOICE_CURRENCY_CODE	VARCHAR(15)   ENCODE lzo
,INITIAL_CUSTOMER_TRX_ID NUMERIC(15,0)   ENCODE az64
,AGREEMENT_ID NUMERIC(15,0)   ENCODE az64
,END_DATE_COMMITMENT  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,START_DATE_COMMITMENT  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_PRINTED_SEQUENCE_NUM NUMERIC(15,0)   ENCODE az64
,ATTRIBUTE_CATEGORY VARCHAR(30)   ENCODE lzo
,ATTRIBUTE1	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE9	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE10	 VARCHAR(150)   ENCODE lzo
,ORIG_SYSTEM_BATCH_NAME	VARCHAR(40)   ENCODE lzo
,POST_REQUEST_ID NUMERIC(15,0)   ENCODE az64
,REQUEST_ID NUMERIC(15,0)   ENCODE az64
,PROGRAM_APPLICATION_ID NUMERIC(15,0)   ENCODE az64
,PROGRAM_ID NUMERIC(15,0)   ENCODE az64
,PROGRAM_UPDATE_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,FINANCE_CHARGES	 VARCHAR(1)   ENCODE lzo
,COMPLETE_FLAG	 VARCHAR(1)   ENCODE lzo
,POSTING_CONTROL_ID NUMERIC(15,0)   ENCODE az64
,BILL_TO_ADDRESS_ID NUMERIC(15,0)   ENCODE az64
,RA_POST_LOOP_NUMBER NUMERIC(15,0)   ENCODE az64
,SHIP_TO_ADDRESS_ID NUMERIC(15,0)   ENCODE az64
,CREDIT_METHOD_FOR_RULES VARCHAR(30)   ENCODE lzo
,CREDIT_METHOD_FOR_INSTALLMENTS VARCHAR(30)   ENCODE lzo
,RECEIPT_METHOD_ID NUMERIC(15,0)   ENCODE az64
,ATTRIBUTE11	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE12	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE13	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE14	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE15	 VARCHAR(150)   ENCODE lzo
,RELATED_CUSTOMER_TRX_ID NUMERIC(15,0)   ENCODE az64
,INVOICING_RULE_ID NUMERIC(15,0)   ENCODE az64
,SHIP_VIA VARCHAR(30)   ENCODE lzo
,SHIP_DATE_ACTUAL  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,WAYBILL_NUMBER	 VARCHAR(50)   ENCODE lzo
,FOB_POINT VARCHAR(30)   ENCODE lzo
,CUSTOMER_BANK_ACCOUNT_ID NUMERIC(15,0)   ENCODE az64
,INTERFACE_HEADER_ATTRIBUTE1	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE2	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE3	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE4	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE5	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE6	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE7	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE8	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_CONTEXT VARCHAR(30)   ENCODE lzo
,DEFAULT_USSGL_TRX_CODE_CONTEXT VARCHAR(30)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE10	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE11	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE12	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE13	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE14	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE15	 VARCHAR(150)   ENCODE lzo
,INTERFACE_HEADER_ATTRIBUTE9	 VARCHAR(150)   ENCODE lzo
,DEFAULT_USSGL_TRANSACTION_CODE VARCHAR(30)   ENCODE lzo
,RECURRED_FROM_TRX_NUMBER VARCHAR(20)   ENCODE lzo
,STATUS_TRX VARCHAR(30)   ENCODE lzo
,DOC_SEQUENCE_ID NUMERIC(15,0)   ENCODE az64
,DOC_SEQUENCE_VALUE NUMERIC(15,0)   ENCODE az64
,PAYING_CUSTOMER_ID NUMERIC(15,0)   ENCODE az64
,PAYING_SITE_USE_ID NUMERIC(15,0)   ENCODE az64
,RELATED_BATCH_SOURCE_ID NUMERIC(15,0)   ENCODE az64
,DEFAULT_TAX_EXEMPT_FLAG	 VARCHAR(1)   ENCODE lzo
,CREATED_FROM VARCHAR(30)   ENCODE lzo
,ORG_ID NUMERIC(15,0)   ENCODE az64
,WH_UPDATE_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,GLOBAL_ATTRIBUTE1	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE2	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE3	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE4	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE5	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE6	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE7	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE8	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE9	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE10	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE11	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE12	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE13	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE14	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE15	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE16	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE17	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE18	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE19	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE20	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE_CATEGORY VARCHAR(30)   ENCODE lzo
,EDI_PROCESSED_FLAG	 VARCHAR(1)   ENCODE lzo
,EDI_PROCESSED_STATUS	VARCHAR(10)    ENCODE lzo
,GLOBAL_ATTRIBUTE21	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE22	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE23	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE24	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE25	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE26	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE27	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE28	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE29	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE30	 VARCHAR(150)   ENCODE lzo
,MRC_EXCHANGE_RATE_TYPE	VARCHAR(2000)    ENCODE lzo
,MRC_EXCHANGE_DATE	VARCHAR(2000)   ENCODE lzo
,MRC_EXCHANGE_RATE	VARCHAR(2000)   ENCODE lzo
,PAYMENT_SERVER_ORDER_NUM	VARCHAR(80)   ENCODE lzo
,APPROVAL_CODE	VARCHAR(80)   ENCODE lzo
,ADDRESS_VERIFICATION_CODE	VARCHAR(80)   ENCODE lzo
,OLD_TRX_NUMBER VARCHAR(20)   ENCODE lzo
,BR_AMOUNT	NUMERIC(28,10)   ENCODE az64
,BR_UNPAID_FLAG	 VARCHAR(1)   ENCODE lzo
,BR_ON_HOLD_FLAG	 VARCHAR(1)   ENCODE lzo
,DRAWEE_ID NUMERIC(15,0)   ENCODE az64
,DRAWEE_CONTACT_ID NUMERIC(15,0)   ENCODE az64
,DRAWEE_SITE_USE_ID NUMERIC(15,0)   ENCODE az64
,REMITTANCE_BANK_ACCOUNT_ID NUMERIC(15,0)   ENCODE az64
,OVERRIDE_REMIT_ACCOUNT_FLAG	 VARCHAR(1)   ENCODE lzo
,DRAWEE_BANK_ACCOUNT_ID NUMERIC(15,0)   ENCODE az64
,SPECIAL_INSTRUCTIONS	VARCHAR(240)   ENCODE lzo
,REMITTANCE_BATCH_ID NUMERIC(15,0)   ENCODE az64
,PREPAYMENT_FLAG	 VARCHAR(1)   ENCODE lzo
,CT_REFERENCE	 VARCHAR(150)   ENCODE lzo
,CONTRACT_ID	NUMERIC(15,0)   ENCODE az64
,BILL_TEMPLATE_ID NUMERIC(15,0)   ENCODE az64
,REVERSED_CASH_RECEIPT_ID NUMERIC(15,0)   ENCODE az64
,CC_ERROR_CODE	VARCHAR(80)   ENCODE lzo
,CC_ERROR_TEXT	VARCHAR(255)   ENCODE lzo
,CC_ERROR_FLAG	 VARCHAR(1)   ENCODE lzo
,UPGRADE_METHOD VARCHAR(30)   ENCODE lzo
,LEGAL_ENTITY_ID NUMERIC(15,0)   ENCODE az64
,REMIT_BANK_ACCT_USE_ID NUMERIC(15,0)   ENCODE az64
,PAYMENT_TRXN_EXTENSION_ID NUMERIC(15,0)   ENCODE az64
,AX_ACCOUNTED_FLAG	 VARCHAR(1)   ENCODE lzo
,APPLICATION_ID NUMERIC(15,0)   ENCODE az64
,PAYMENT_ATTRIBUTES	VARCHAR(1000)   ENCODE lzo
,BILLING_DATE  TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,INTEREST_HEADER_ID NUMERIC(15,0)   ENCODE az64
,LATE_CHARGES_ASSESSED VARCHAR(30)   ENCODE lzo
	,trailer_number VARCHAR(50)   ENCODE lzo
	,rev_rec_application VARCHAR(30)   ENCODE lzo
	,document_type_id NUMERIC(15,0)   ENCODE az64
	,document_creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,src_invoicing_rule_id NUMERIC(15,0)   ENCODE az64
	,billing_ext_request NUMERIC(15,0)   ENCODE az64
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;


INSERT INTO bec_ods.RA_CUSTOMER_TRX_ALL
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

SELECT 
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
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
,kca_seq_date
    FROM
        bec_ods_stg.RA_CUSTOMER_TRX_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ra_customer_trx_all';
	
commit;