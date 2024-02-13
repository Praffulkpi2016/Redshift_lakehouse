/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.FACT_CHECK_DETAILS;

create table bec_dwh.FACT_CHECK_DETAILS 
	diststyle all sortkey(CHECK_ID)
as
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
ORDER BY
  --  UPPER(BR.BANK_NAME),
  --  UPPER(BR.BANK_BRANCH_NAME),
    UPPER(CH.BANK_ACCOUNT_NAME),
    CH.CURRENCY_CODE,
    CPD.PAYMENT_DOCUMENT_NAME,
    CH.CHECK_NUMBER
	
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_check_details'
	and batch_name = 'ap';

commit;