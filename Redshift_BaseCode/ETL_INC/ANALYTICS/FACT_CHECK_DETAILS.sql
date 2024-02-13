/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.fact_check_details
where CHECK_ID in (
select ods.CHECK_ID from bec_dwh.fact_check_details dw, 
(
select CH.CHECK_ID
FROM
    bec_ods.AP_CHECKS_ALL CH,
    bec_ods.CE_PAYMENT_DOCUMENTS CPD,
    bec_ods.CE_BANK_ACCT_USES_ALL CBAU,
    bec_ods.CE_BANK_ACCOUNTS BA,
    bec_ods.FND_LOOKUP_VALUES LK2,
    bec_ods.AP_SUPPLIER_SITES_ALL APSA
WHERE 1=1
    AND CPD.PAYMENT_DOCUMENT_ID (+) = CH.PAYMENT_DOCUMENT_ID
    AND CH.CE_BANK_ACCT_USE_ID = CBAU.BANK_ACCT_USE_ID
    AND CBAU.BANK_ACCOUNT_ID = BA.BANK_ACCOUNT_ID
     AND LK2.LOOKUP_TYPE = 'CHECK STATE'
    AND LK2.LANGUAGE ='US'
    AND LK2.LOOKUP_CODE = CH.STATUS_LOOKUP_CODE
    AND CH.VENDOR_SITE_ID = APSA.VENDOR_SITE_ID (+)
	AND (
CH.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_check_details' and batch_name = 'ap') 
OR BA.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_check_details' and batch_name = 'ap')
or CH.is_deleted_flg = 'Y'
or CPD.is_deleted_flg = 'Y'
or CBAU.is_deleted_flg = 'Y'
or BA.is_deleted_flg = 'Y'
or LK2.is_deleted_flg = 'Y'
or APSA.is_deleted_flg = 'Y'
)
ORDER BY
  
    UPPER(CH.BANK_ACCOUNT_NAME),
    CH.CURRENCY_CODE,
    CPD.PAYMENT_DOCUMENT_NAME,
    CH.CHECK_NUMBER
) ods
where dw.dw_load_id = 
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-'
       || nvl(ods.CHECK_ID, 0)
);

commit;
-- Insert records

INSERT INTO bec_dwh.fact_check_details
(
CHECK_ID,
VENDOR_ID,
VENDOR_SITE_ID,
CHECK_ID_KEY,
VENDOR_ID_KEY,
VENDOR_SITE_ID_KEY,
BANK_ACCOUNT_ID,
BANK_BRANCH_ID,
CHECK_NUMBER,
CHECK_DATE,
AMOUNT,
VENDOR_NAME,
VENDOR_SITE_CODE,
ADDRESS_LINE1,
ADDRESS_LINE2,
ADDRESS_LINE3,
CITY,
STATE,
ZIP,
CLEARED_DATE,
CLEARED_AMOUNT,
CHECK_STOCK_NAME,
NLS_STATUS,
ACCOUNT,
ACCOUNTID,
CURRENCY_CODE,
PAY_CURRENCY_CODE,
DOC_SEQUENCE_VALUE,
PAYMENT_METHOD_CODE,
ORG_ID_KEY,
ORG_ID,
IS_DELETED_FLG,
source_app_id,
dw_load_id,
dw_insert_date,
dw_update_date
)
(
SELECT
    CH.CHECK_ID,
    CH.VENDOR_ID,
    CH.VENDOR_SITE_ID,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||CH.CHECK_ID as CHECK_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||CH.VENDOR_ID as VENDOR_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||CH.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
    BA.BANK_ACCOUNT_ID,
    BA.BANK_BRANCH_ID,
    CH.CHECK_NUMBER             CHECK_NUMBER,
    CH.CHECK_DATE               CHECK_DATE,
    CH.AMOUNT                   AMOUNT,
    NVL(SUBSTRING(CH.VENDOR_NAME, 1, 240), SUBSTRING(CH.REMIT_TO_SUPPLIER_NAME, 1, 240)) VENDOR_NAME,
    NVL(CH.REMIT_TO_SUPPLIER_SITE, APSA.VENDOR_SITE_CODE) VENDOR_SITE_CODE,
    SUBSTRING(CH.ADDRESS_LINE1, 1, 23) ADDRESS_LINE1,
    SUBSTRING(CH.ADDRESS_LINE2, 1, 23) ADDRESS_LINE2,
    SUBSTRING(CH.ADDRESS_LINE3, 1, 23) ADDRESS_LINE3,
    SUBSTRING(CH.CITY, 1, 13) CITY,
    SUBSTRING(CH.STATE, 1, 4) STATE,
    SUBSTRING(CH.ZIP, 1, 6) ZIP,
   -- SUBSTRING(FT.TERRITORY_SHORT_NAME, 1, 23) COUNTRY,
    CH.CLEARED_DATE             CLEARED_DATE,
    CH.CLEARED_AMOUNT           CLEARED_AMOUNT,
    CPD.PAYMENT_DOCUMENT_NAME   CHECK_STOCK_NAME,
    LK2.MEANING         NLS_STATUS,
   -- BR.BANK_NAME                BANK,
   -- BR.BANK_BRANCH_NAME         BRANCH,
    CH.BANK_ACCOUNT_NAME        ACCOUNT,
    CH.BANK_ACCOUNT_ID          ACCOUNTID,
   -- BR.BRANCH_PARTY_ID          BANK_BRANCH,
    BA.CURRENCY_CODE            CURRENCY_CODE,
    CH.CURRENCY_CODE            PAY_CURRENCY_CODE,
    CH.DOC_SEQUENCE_VALUE       DOC_SEQUENCE_VALUE,
	CH.PAYMENT_METHOD_CODE      PAYMENT_METHOD_CODE,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||CH.ORG_ID as ORG_ID_KEY,
	 CH.ORG_ID as ORG_ID,
	'N' AS IS_DELETED_FLG,
		(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-'
       || nvl(CH.CHECK_ID, 0)  as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
    (select * from bec_ods.AP_CHECKS_ALL where is_deleted_flg <> 'Y')                CH,
    (select * from bec_ods.CE_PAYMENT_DOCUMENTS where is_deleted_flg <> 'Y')     CPD,
    (select * from bec_ods.CE_BANK_ACCT_USES_ALL where is_deleted_flg <> 'Y')    CBAU,
    (select * from bec_ods.CE_BANK_ACCOUNTS where is_deleted_flg <> 'Y')         BA,
     (select * from bec_ods.FND_LOOKUP_VALUES where is_deleted_flg <> 'Y')          LK2,
     (select * from bec_ods.AP_SUPPLIER_SITES_ALL where is_deleted_flg <> 'Y')    APSA
WHERE 1=1
    AND CPD.PAYMENT_DOCUMENT_ID (+) = CH.PAYMENT_DOCUMENT_ID
    AND CH.CE_BANK_ACCT_USE_ID = CBAU.BANK_ACCT_USE_ID
    AND CBAU.BANK_ACCOUNT_ID = BA.BANK_ACCOUNT_ID
   -- AND BA.BANK_BRANCH_ID = BR.BRANCH_PARTY_ID
   -- AND CH.COUNTRY = FT.TERRITORY_CODE (+)
    AND LK2.LOOKUP_TYPE = 'CHECK STATE'
    AND LK2.LANGUAGE ='US'
    AND LK2.LOOKUP_CODE = CH.STATUS_LOOKUP_CODE
    AND CH.VENDOR_SITE_ID = APSA.VENDOR_SITE_ID (+)
	AND (
CH.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_check_details' and batch_name = 'ap') 
OR BA.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_check_details' and batch_name = 'ap')
)
ORDER BY
  --  UPPER(BR.BANK_NAME),
  --  UPPER(BR.BANK_BRANCH_NAME),
    UPPER(CH.BANK_ACCOUNT_NAME),
    CH.CURRENCY_CODE,
    CPD.PAYMENT_DOCUMENT_NAME,
    CH.CHECK_NUMBER
	
);
 
 
commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'fact_check_details'
and batch_name = 'ap';

COMMIT;
