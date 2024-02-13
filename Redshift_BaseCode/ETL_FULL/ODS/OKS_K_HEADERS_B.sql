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

DROP TABLE if exists bec_ods.OKS_K_HEADERS_B;

CREATE TABLE IF NOT EXISTS bec_ods.OKS_K_HEADERS_B
(
	id VARCHAR(50)   ENCODE lzo
	,chr_id NUMERIC(15,0)   ENCODE az64
	,acct_rule_id NUMERIC(15,0)   ENCODE az64
	,payment_type VARCHAR(50)   ENCODE lzo
	,cc_no VARCHAR(80)   ENCODE lzo
	,cc_expiry_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cc_bank_acct_id NUMERIC(15,0)   ENCODE az64
	,cc_auth_code VARCHAR(150)   ENCODE lzo
	,commitment_id NUMERIC(15,0)   ENCODE az64
	,grace_duration NUMERIC(38,10)   ENCODE az64
	,grace_period VARCHAR(30)   ENCODE lzo
	,est_rev_percent NUMERIC(28,10)   ENCODE az64
	,est_rev_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tax_amount NUMERIC(28,10)   ENCODE az64
	,tax_status VARCHAR(30)   ENCODE lzo
	,tax_code NUMERIC(28,10)   ENCODE az64
	,tax_exemption_id NUMERIC(15,0)   ENCODE az64
	,billing_profile_id NUMERIC(15,0)   ENCODE az64
	,renewal_status VARCHAR(30)   ENCODE lzo
	,electronic_renewal_flag VARCHAR(1)   ENCODE lzo
	,quote_to_contact_id NUMERIC(15,0)   ENCODE az64
	,quote_to_site_id NUMERIC(15,0)   ENCODE az64
	,quote_to_email_id NUMERIC(15,0)   ENCODE az64
	,quote_to_phone_id NUMERIC(15,0)   ENCODE az64
	,quote_to_fax_id NUMERIC(15,0)   ENCODE az64
	,renewal_po_required VARCHAR(1)   ENCODE lzo
	,renewal_po_number VARCHAR(50)   ENCODE lzo
	,renewal_price_list NUMERIC(28,10)   ENCODE az64
	,renewal_pricing_type VARCHAR(30)   ENCODE lzo
	,renewal_markup_percent NUMERIC(28,10)   ENCODE az64
	,renewal_grace_duration NUMERIC(28,10)   ENCODE az64
	,renewal_grace_period VARCHAR(30)   ENCODE lzo
	,renewal_est_rev_percent NUMERIC(28,10)   ENCODE az64
	,renewal_est_rev_duration NUMERIC(28,10)   ENCODE az64
	,renewal_est_rev_period VARCHAR(50)   ENCODE lzo
	,renewal_price_list_used NUMERIC(28,10)   ENCODE az64
	,renewal_type_used VARCHAR(30)   ENCODE lzo
	,renewal_notification_to NUMERIC(28,10)   ENCODE az64
	,renewal_po_used VARCHAR(1)   ENCODE lzo
	,renewal_pricing_type_used VARCHAR(30)   ENCODE lzo
	,renewal_markup_percent_used NUMERIC(28,10)   ENCODE az64
	,rev_est_percent_used NUMERIC(28,10)   ENCODE az64
	,rev_est_duration_used NUMERIC(28,10)   ENCODE az64
	,rev_est_period_used VARCHAR(50)   ENCODE lzo
	,billing_profile_used NUMERIC(28,10)   ENCODE az64
	,ern_flag_used_yn VARCHAR(1)   ENCODE lzo
	,evn_threshold_amt NUMERIC(28,10)   ENCODE az64
	,evn_threshold_cur VARCHAR(30)   ENCODE lzo
	,ern_threshold_amt NUMERIC(28,10)   ENCODE az64
	,ern_threshold_cur VARCHAR(30)   ENCODE lzo
	,renewal_grace_duration_used NUMERIC(28,10)   ENCODE az64
	,renewal_grace_period_used VARCHAR(30)   ENCODE lzo
	,inv_trx_type VARCHAR(30)   ENCODE lzo
	,inv_print_profile VARCHAR(1)   ENCODE lzo
	,ar_interface_yn VARCHAR(1)   ENCODE lzo
	,hold_billing VARCHAR(1)   ENCODE lzo
	,summary_trx_yn VARCHAR(1)   ENCODE lzo
	,service_po_number VARCHAR(240)   ENCODE lzo
	,service_po_required VARCHAR(1)   ENCODE lzo
	,billing_schedule_type VARCHAR(10)   ENCODE lzo
	,object_version_number NUMERIC(28,10)   ENCODE az64
	,security_group_id NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,follow_up_action VARCHAR(30)   ENCODE lzo
	,follow_up_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,trxn_extension_id NUMERIC(15,0)   ENCODE az64
	,date_accepted TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,accepted_by NUMERIC(15,0)   ENCODE az64
	,rmndr_suppress_flag VARCHAR(1)   ENCODE lzo
	,rmndr_sent_flag VARCHAR(1)   ENCODE lzo
	,quote_sent_flag VARCHAR(1)   ENCODE lzo
	,process_request_id NUMERIC(15,0)   ENCODE az64
	,wf_item_key VARCHAR(240)   ENCODE lzo
	,period_start VARCHAR(30)   ENCODE lzo
	,period_type VARCHAR(30)   ENCODE lzo
	,price_uom VARCHAR(10)   ENCODE lzo
	,person_party_id NUMERIC(15,0)   ENCODE az64
	,tax_classification_code VARCHAR(50)   ENCODE lzo
	,exempt_certificate_number VARCHAR(80)   ENCODE lzo
	,exempt_reason_code VARCHAR(30)   ENCODE lzo
	,approval_type_used VARCHAR(30)   ENCODE lzo
	,renewal_comment VARCHAR(600)   ENCODE lzo
	,cc_email_address VARCHAR(2000)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.OKS_K_HEADERS_B (
	  ID
,      CHR_ID
,      ACCT_RULE_ID
,      PAYMENT_TYPE
,      CC_NO
,      CC_EXPIRY_DATE
,      CC_BANK_ACCT_ID
,      CC_AUTH_CODE
,      COMMITMENT_ID
,      GRACE_DURATION
,      GRACE_PERIOD
,      EST_REV_PERCENT
,      EST_REV_DATE
,      TAX_AMOUNT
,      TAX_STATUS
,      TAX_CODE
,      TAX_EXEMPTION_ID
,      BILLING_PROFILE_ID
,      RENEWAL_STATUS
,      ELECTRONIC_RENEWAL_FLAG
,      QUOTE_TO_CONTACT_ID
,      QUOTE_TO_SITE_ID
,      QUOTE_TO_EMAIL_ID
,      QUOTE_TO_PHONE_ID
,      QUOTE_TO_FAX_ID
,      RENEWAL_PO_REQUIRED
,      RENEWAL_PO_NUMBER
,      RENEWAL_PRICE_LIST
,      RENEWAL_PRICING_TYPE
,      RENEWAL_MARKUP_PERCENT
,      RENEWAL_GRACE_DURATION
,      RENEWAL_GRACE_PERIOD
,      RENEWAL_EST_REV_PERCENT
,      RENEWAL_EST_REV_DURATION
,      RENEWAL_EST_REV_PERIOD
,      RENEWAL_PRICE_LIST_USED
,      RENEWAL_TYPE_USED
,      RENEWAL_NOTIFICATION_TO
,      RENEWAL_PO_USED
,      RENEWAL_PRICING_TYPE_USED
,      RENEWAL_MARKUP_PERCENT_USED
,      REV_EST_PERCENT_USED
,      REV_EST_DURATION_USED
,      REV_EST_PERIOD_USED
,      BILLING_PROFILE_USED
,      ERN_FLAG_USED_YN
,      EVN_THRESHOLD_AMT
,      EVN_THRESHOLD_CUR
,      ERN_THRESHOLD_AMT
,      ERN_THRESHOLD_CUR
,      RENEWAL_GRACE_DURATION_USED
,      RENEWAL_GRACE_PERIOD_USED
,      INV_TRX_TYPE
,      INV_PRINT_PROFILE
,      AR_INTERFACE_YN
,      HOLD_BILLING
,      SUMMARY_TRX_YN
,      SERVICE_PO_NUMBER
,      SERVICE_PO_REQUIRED
,      BILLING_SCHEDULE_TYPE
,      OBJECT_VERSION_NUMBER
,      SECURITY_GROUP_ID
,      REQUEST_ID
,      CREATED_BY
,      CREATION_DATE
,      LAST_UPDATED_BY
,      LAST_UPDATE_DATE
,      LAST_UPDATE_LOGIN
,      FOLLOW_UP_ACTION
,      FOLLOW_UP_DATE
,      TRXN_EXTENSION_ID
,      DATE_ACCEPTED
,      ACCEPTED_BY
,      RMNDR_SUPPRESS_FLAG
,      RMNDR_SENT_FLAG
,      QUOTE_SENT_FLAG
,      PROCESS_REQUEST_ID
,      WF_ITEM_KEY
,      PERIOD_START
,      PERIOD_TYPE
,      PRICE_UOM
,      PERSON_PARTY_ID
,      TAX_CLASSIFICATION_CODE
,      EXEMPT_CERTIFICATE_NUMBER
,      EXEMPT_REASON_CODE
,      APPROVAL_TYPE_USED
,      RENEWAL_COMMENT
,      CC_EMAIL_ADDRESS
,	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		  ID
,      CHR_ID
,      ACCT_RULE_ID
,      PAYMENT_TYPE
,      CC_NO
,      CC_EXPIRY_DATE
,      CC_BANK_ACCT_ID
,      CC_AUTH_CODE
,      COMMITMENT_ID
,      GRACE_DURATION
,      GRACE_PERIOD
,      EST_REV_PERCENT
,      EST_REV_DATE
,      TAX_AMOUNT
,      TAX_STATUS
,      TAX_CODE
,      TAX_EXEMPTION_ID
,      BILLING_PROFILE_ID
,      RENEWAL_STATUS
,      ELECTRONIC_RENEWAL_FLAG
,      QUOTE_TO_CONTACT_ID
,      QUOTE_TO_SITE_ID
,      QUOTE_TO_EMAIL_ID
,      QUOTE_TO_PHONE_ID
,      QUOTE_TO_FAX_ID
,      RENEWAL_PO_REQUIRED
,      RENEWAL_PO_NUMBER
,      RENEWAL_PRICE_LIST
,      RENEWAL_PRICING_TYPE
,      RENEWAL_MARKUP_PERCENT
,      RENEWAL_GRACE_DURATION
,      RENEWAL_GRACE_PERIOD
,      RENEWAL_EST_REV_PERCENT
,      RENEWAL_EST_REV_DURATION
,      RENEWAL_EST_REV_PERIOD
,      RENEWAL_PRICE_LIST_USED
,      RENEWAL_TYPE_USED
,      RENEWAL_NOTIFICATION_TO
,      RENEWAL_PO_USED
,      RENEWAL_PRICING_TYPE_USED
,      RENEWAL_MARKUP_PERCENT_USED
,      REV_EST_PERCENT_USED
,      REV_EST_DURATION_USED
,      REV_EST_PERIOD_USED
,      BILLING_PROFILE_USED
,      ERN_FLAG_USED_YN
,      EVN_THRESHOLD_AMT
,      EVN_THRESHOLD_CUR
,      ERN_THRESHOLD_AMT
,      ERN_THRESHOLD_CUR
,      RENEWAL_GRACE_DURATION_USED
,      RENEWAL_GRACE_PERIOD_USED
,      INV_TRX_TYPE
,      INV_PRINT_PROFILE
,      AR_INTERFACE_YN
,      HOLD_BILLING
,      SUMMARY_TRX_YN
,      SERVICE_PO_NUMBER
,      SERVICE_PO_REQUIRED
,      BILLING_SCHEDULE_TYPE
,      OBJECT_VERSION_NUMBER
,      SECURITY_GROUP_ID
,      REQUEST_ID
,      CREATED_BY
,      CREATION_DATE
,      LAST_UPDATED_BY
,      LAST_UPDATE_DATE
,      LAST_UPDATE_LOGIN
,      FOLLOW_UP_ACTION
,      FOLLOW_UP_DATE
,      TRXN_EXTENSION_ID
,      DATE_ACCEPTED
,      ACCEPTED_BY
,      RMNDR_SUPPRESS_FLAG
,      RMNDR_SENT_FLAG
,      QUOTE_SENT_FLAG
,      PROCESS_REQUEST_ID
,      WF_ITEM_KEY
,      PERIOD_START
,      PERIOD_TYPE
,      PRICE_UOM
,      PERSON_PARTY_ID
,      TAX_CLASSIFICATION_CODE
,      EXEMPT_CERTIFICATE_NUMBER
,      EXEMPT_REASON_CODE
,      APPROVAL_TYPE_USED
,      RENEWAL_COMMENT
,      CC_EMAIL_ADDRESS
		,KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.OKS_K_HEADERS_B;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'oks_k_headers_b';
	
commit;