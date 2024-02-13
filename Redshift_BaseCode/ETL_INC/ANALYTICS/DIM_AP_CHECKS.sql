/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.DIM_AP_CHECKS
where check_id in (
select ods.check_id from bec_dwh.dim_ap_checks dw, bec_ods.ap_checks_all ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.CHECK_ID,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_checks' and batch_name = 'ap')
 )
);

commit;

-- Insert records

insert into bec_dwh.dim_ap_checks
(
amount
,bank_account_id
,bank_account_name
,check_date
,check_id
,check_number
,currency_code
,payment_method_lookup_code
,address_line1
,address_line2
,address_line3
,checkrun_name
,city
,country
,status_lookup_code
,vendor_name
,vendor_site_code
,zip
,supplier_bank_account
,supplier_bank_type
,supplier_bank_num
,cleared_amount
,cleared_date
,cleared_base_amount
,cleared_exchange_rate
,cleared_exchange_date
,org_id
,vendor_id
,vendor_site_id
,exchange_rate
,exchange_date
,exchange_rate_type
,payment_base_amount
,checkrun_id
,external_bank_account_id
,void_date
,legal_entity_id
,remit_to_supplier_name
,remit_to_supplier_site
,doc_sequence_value
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
SELECT
    AMOUNT,
	BANK_ACCOUNT_ID,
	BANK_ACCOUNT_NAME,
	CHECK_DATE,
	CHECK_ID,
	CHECK_NUMBER,
	CURRENCY_CODE,
	PAYMENT_METHOD_CODE PAYMENT_METHOD_LOOKUP_CODE,
	ADDRESS_LINE1,
	ADDRESS_LINE2,
	ADDRESS_LINE3,
	CHECKRUN_NAME,
	CITY,
	COUNTRY,
	STATUS_LOOKUP_CODE,
	VENDOR_NAME,
	VENDOR_SITE_CODE,
	ZIP,
	BANK_ACCOUNT_NUM SUPPLIER_BANK_ACCOUNT,
	BANK_ACCOUNT_TYPE SUPPLIER_BANK_TYPE,
	BANK_NUM SUPPLIER_BANK_NUM,
	CLEARED_AMOUNT,
	CLEARED_DATE,
	CLEARED_BASE_AMOUNT,
	CLEARED_EXCHANGE_RATE,
	CLEARED_EXCHANGE_DATE,
	ORG_ID,
	VENDOR_ID,
	VENDOR_SITE_ID,
	EXCHANGE_RATE,
	EXCHANGE_DATE,
	EXCHANGE_RATE_TYPE,
	NVL(BASE_AMOUNT, AMOUNT) as PAYMENT_BASE_AMOUNT,
	CHECKRUN_ID,
	EXTERNAL_BANK_ACCOUNT_ID,
	VOID_DATE,
	LEGAL_ENTITY_ID,
	REMIT_TO_SUPPLIER_NAME,
	REMIT_TO_SUPPLIER_SITE,
	DOC_SEQUENCE_VALUE,
	-- audit columns
	'N' as is_deleted_flg,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(CHECK_ID,0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
    bec_ods.AP_CHECKS_ALL
WHERE 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_checks' and batch_name = 'ap')
 )
	);
commit;
-- Soft delete

update bec_dwh.dim_ap_checks set is_deleted_flg = 'Y'
where check_id not in (
select ods.check_id from bec_dwh.dim_ap_checks dw, bec_ods.ap_checks_all ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.CHECK_ID,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_checks'
and batch_name = 'ap';

COMMIT;