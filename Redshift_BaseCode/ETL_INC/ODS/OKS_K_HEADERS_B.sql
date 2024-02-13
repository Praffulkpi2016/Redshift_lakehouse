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

delete from bec_ods.oks_k_headers_b
where ID in (
select stg.ID from bec_ods.oks_k_headers_b ods, bec_ods_stg.oks_k_headers_b stg
where ods.ID = stg.ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.oks_k_headers_b
       (
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
 ,       KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
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
 ,       KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.oks_k_headers_b
	where kca_operation IN ('INSERT','UPDATE') 
	and (ID,kca_seq_id) in 
	(select ID,max(kca_seq_id) from bec_ods_stg.oks_k_headers_b 
     where kca_operation IN ('INSERT','UPDATE')
     group by ID)
);

commit;

-- Soft delete
update bec_ods.oks_k_headers_b set IS_DELETED_FLG = 'N';
commit;
update bec_ods.oks_k_headers_b set IS_DELETED_FLG = 'Y'
where (NVL(ID,'NA'))  in
(
select NVL(ID,'NA') from bec_raw_dl_ext.oks_k_headers_b
where (NVL(ID,'NA'),KCA_SEQ_ID)
in 
(
select NVL(ID,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.oks_k_headers_b
group by NVL(ID,'NA')
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oks_k_headers_b'; 

commit;