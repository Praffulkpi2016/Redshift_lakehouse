/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.dim_ap_checks;

create table bec_dwh.dim_ap_checks 
	diststyle all
	sortkey (CHECK_ID)
as
(
select
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
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || nvl(CHECK_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.AP_CHECKS_ALL
	);
	
end;



update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ap_checks'
	and batch_name = 'ap';

commit;
