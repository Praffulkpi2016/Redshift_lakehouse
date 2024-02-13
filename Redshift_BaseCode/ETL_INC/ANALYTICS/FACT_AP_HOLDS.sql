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

delete
from
	bec_dwh.fact_ap_holds
where
	(nvl(invoice_id,0),
	nvl(hold_id,0)) in (
	select
		nvl(ods.invoice_id,0) as invoice_id,
		nvl(ods.hold_id,0) as hold_id
	from
		bec_dwh.fact_ap_holds dw,
		(
		select
			ah.invoice_id,
			ah.hold_id,
			ai.last_update_date,
			ah.kca_operation,
			ai.kca_seq_date,
			ah.is_deleted_flg,
			ai.is_deleted_flg is_deleted_flg1,
			dcr.is_deleted_flg is_deleted_flg2,
			res.is_deleted_flg3,
			res.is_deleted_flg4
		from
			bec_ods.ap_holds_all ah,
			bec_ods.ap_invoices_all ai,
			bec_ods.GL_DAILY_RATES DCR	
,(select distinct invoice_id,
hold_id, 
DECODE (AH.RELEASE_LOOKUP_CODE,
NULL, 
NULL,
DECODE (AH.LAST_UPDATED_BY,5, (SELECT ALC6.meaning
FROM bec_ods.fnd_lookup_values ALC6
WHERE ALC6.LOOKUP_TYPE(+) = 'NLS TRANSLATION'
AND ALC6.LOOKUP_CODE(+) = 'SYSTEM'
AND ALC6.LANGUAGE = 'US'),
FU2.USER_NAME)) RELEASE_BY_USER_NAME,
ah.is_deleted_flg is_deleted_flg3,
FU2.is_deleted_flg is_deleted_flg4
from bec_ods.ap_holds_all ah
,bec_ods.FND_USER FU2
where  AH.LAST_UPDATED_BY = FU2.USER_ID(+)
) res
		where
			1 = 1
			and ah.invoice_id = ai.invoice_id
			and DCR.to_currency(+) = 'USD'
			and DCR.conversion_type(+) = 'Corporate'
			and ai.invoice_currency_code = DCR.from_currency(+)
			and DCR.conversion_date(+) = ai.invoice_date
			and ah.invoice_id = res.invoice_id(+)
			and ah.hold_id = res.hold_id(+)
) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' ||
nvl(ods.invoice_id, 0) || '-' || nvl(ods.hold_id, 0)
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_holds'
				and batch_name = 'ap')
			or ods.is_deleted_flg = 'Y'
			or ods.is_deleted_flg1 = 'Y'
			or ods.is_deleted_flg2 = 'Y'
			or ods.is_deleted_flg3 = 'Y'
			or ods.is_deleted_flg4 = 'Y')
);

commit;
-- Insert records

insert
	into
	bec_dwh.FACT_AP_HOLDS
(
	INVOICE_ID_KEY,
	hold_id,
	VENDOR_ID_KEY,
	VENDOR_SITE_ID_KEY,
	LEDGER_ID_KEY,
	line_location_id,
	hold_lookup_code,
	release_lookup_code,
	held_by,
	ORG_ID_KEY,
	ORG_ID,
	RELEASE_DATE,
	RELEASE_BY_USER_NAME,
	hold_date,
	hold_reason,
	release_reason,
	status_flag,
	rcv_transaction_id,
	creation_date,
	last_update_date,
	invoice_id,	
	invoice_amount,
	invoice_base_amount,
	GBL_INVOICE_AMOUNT,
	GBL_AMOUNT_PAID,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date)
	select
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || ah.invoice_id as INVOICE_ID_KEY,
	ah.hold_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || ai.vendor_id as VENDOR_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || ai.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || SET_OF_BOOKS_ID as LEDGER_ID_KEY,
	ah.line_location_id,
	ah.hold_lookup_code,
	ah.release_lookup_code,
	ah.held_by,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || ah.ORG_ID as ORG_ID_KEY,
		ah.ORG_ID as ORG_ID,
	DECODE (ah.RELEASE_LOOKUP_CODE,
	null,
	null,
	ah.LAST_UPDATE_DATE) RELEASE_DATE,
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
	cast(NVL(ai.invoice_amount, 0) * NVL(DCR.conversion_rate, 1) as decimal(18, 2)) GBL_INVOICE_AMOUNT,
	cast(NVL(ai.amount_paid, 0) * NVL(DCR.conversion_rate, 1) as decimal(18, 2)) GBL_AMOUNT_PAID,
	'N' as is_deleted_flg,
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
where
	1 = 1
	and ah.invoice_id = ai.invoice_id
	and DCR.to_currency(+) = 'USD'
	and DCR.conversion_type(+) = 'Corporate'
	and ai.invoice_currency_code = DCR.from_currency(+)
	and DCR.conversion_date(+) = ai.invoice_date
	and ah.invoice_id = res.invoice_id(+)
	and ah.hold_id = res.hold_id(+)
	and (ai.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_holds'
				and batch_name = 'ap'));
				
commit;

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