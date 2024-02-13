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

drop table if exists bec_dwh.FACT_AP_HOLDS;

create table bec_dwh.FACT_AP_HOLDS diststyle all sortkey (invoice_id,hold_id)
as
(
select
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||ah.invoice_id as INVOICE_ID_KEY,
	ah.hold_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||ai.vendor_id as VENDOR_ID_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||ai.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
      (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||SET_OF_BOOKS_ID as LEDGER_ID_KEY,
	ah.line_location_id,
	ah.hold_lookup_code,
	ah.release_lookup_code,
	ah.held_by,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||ah.ORG_ID as ORG_ID_KEY,
	ah.ORG_ID as ORG_ID,
	DECODE (ah.RELEASE_LOOKUP_CODE, NULL, NULL, ah.LAST_UPDATE_DATE) RELEASE_DATE,
	RES.RELEASE_BY_USER_NAME,
	ah.hold_date,
	ah.hold_reason,
	ah.release_reason,
	ah.status_flag,
	ah.rcv_transaction_id,
	ah.creation_date,
	ah.last_update_date,
	ah.invoice_id,
	ai.invoice_amount,
	NVL(ai.BASE_AMOUNT, ai.INVOICE_AMOUNT) "INVOICE_BASE_AMOUNT",
	cast(NVL(ai.invoice_amount,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INVOICE_AMOUNT,
    cast(NVL(ai.amount_paid,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_AMOUNT_PAID,
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
       || nvl(ah.invoice_id, 0) || '-' || nvl(ah.hold_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select * from bec_ods.ap_holds_all where is_deleted_flg <> 'Y') ah,
	(select * from bec_ods.ap_invoices_all where is_deleted_flg <> 'Y') ai,
    (select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y') DCR	
,(select distinct invoice_id,
hold_id, 
DECODE (AH.RELEASE_LOOKUP_CODE,
NULL, 
NULL,
DECODE (AH.LAST_UPDATED_BY,5, (SELECT ALC6.meaning
FROM (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') ALC6
WHERE ALC6.LOOKUP_TYPE(+) = 'NLS TRANSLATION'
AND ALC6.LOOKUP_CODE(+) = 'SYSTEM'
AND ALC6.LANGUAGE = 'US'),
FU2.USER_NAME)) RELEASE_BY_USER_NAME
from (select * from bec_ods.ap_holds_all where is_deleted_flg <> 'Y') ah
, (select * from bec_ods.FND_USER where is_deleted_flg <> 'Y') FU2
where  AH.LAST_UPDATED_BY = FU2.USER_ID(+)
) res
where 1=1
and ah.invoice_id = ai.invoice_id 
and DCR.to_currency(+) = 'USD'
and DCR.conversion_type(+) = 'Corporate'
and ai.invoice_currency_code = DCR.from_currency(+)
and DCR.conversion_date(+) = ai.invoice_date
and ah.invoice_id=res.invoice_id(+) 
and ah.hold_id =res.hold_id(+) 

);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ap_holds'
	and batch_name = 'ap';

commit;