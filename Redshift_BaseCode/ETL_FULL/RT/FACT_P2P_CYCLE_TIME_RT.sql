/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh_rpt.FACT_P2P_CYCLE_TIME_RT;

create table bec_dwh_rpt.FACT_P2P_CYCLE_TIME_RT 
	diststyle all
as
(
select
org_id,
check_id ,
invoice_id,
po_header_id,
vendor_id,
vendor_site_id,
check_number,
check_date,
check_amount,
period_num,
period_year,
period_name,
payment_due_date,
invoice_type_lookup_code ,
invoice_num,
invoice_amount,
invoice_date,
voucher_date,
vendor_name,
po_type,
po_date,
po_number,
po_release_number,
receipt_date,
receipt_number,
po_creation_date,
datediff(hours,
	PO_DATE ,
	INVOICE_DATE ) INV_PODT,
	   datediff(hours,
	INVOICE_DATE,
	RECEIPT_DATE) INV_TO_REPT,
	   datediff(hours,
	INVOICE_DATE,
	voucher_date) INV_TO_VOC,
	   datediff(hours,
	receipt_date,
	check_date ) REC_TO_CHK,
	datediff(hours,
	invoice_date,
	check_date ) INV_TO_CHK,
	   datediff(hours,
	decode(invoice_type_lookup_code,
	'EXPENSE REPORT',
	(voucher_date + 15),
	(payment_due_date + 10)),
	check_date) CHK_TO_DUE,
	   decode(sign(datediff(days,
	receipt_date ,
	invoice_date) + 30),
	- 1,
	1,
	0,
	1,
	0) RCPT_30D,
	   decode(sign(datediff(days,
	decode(invoice_type_lookup_code,
	'EXPENSE REPORT',
	(voucher_date + 15),
	(payment_due_date + 10)),
	check_date)),
	- 1,
	1,
	0,
	1,
	0) Payments_ontime,
	--Added for quicksight development
	datediff(second,PO_DATE ,	INVOICE_DATE ) INV_PODT_SEC,
	datediff(second,INVOICE_DATE,	RECEIPT_DATE) INV_TO_REPT_SEC,
	datediff(second,INVOICE_DATE,	voucher_date) INV_TO_VOC_SEC,
	datediff(second,receipt_date,	check_date ) REC_TO_CHK_SEC,
	datediff(second,invoice_date,	check_date ) INV_TO_CHK_SEC,
	datediff(second,decode(invoice_type_lookup_code,	'EXPENSE REPORT',	(voucher_date + 15),	(payment_due_date + 10)),check_date) CHK_TO_DUE_SEC
from
	(
	select
		distinct ac.org_id,
		ac.check_id ,
		api.invoice_id,
		poh.po_header_id,
		api.vendor_id,
		api.vendor_site_id,
		ac.check_number,
		ac.check_date,
		ac.amount check_amount,
		gp.period_num,
		gp.period_year,
		gp.period_name,
		aps.due_date payment_due_date,
		api.invoice_type_lookup_code ,
		api.invoice_num,
		api.invoice_amount,
		api.invoice_date,
		decode(api.invoice_type_lookup_code,
		'EXPENSE REPORT',
		exp_rep.entered_date,
		api.creation_date) voucher_date,
		sup.vendor_name,
		poh.type_lookup_code po_type,
		decode(poh.type_lookup_code,
		'STANDARD',
		poh.APPROVED_DATE,
		'BLANKET',
		por.release_date,
		poh.APPROVED_DATE) po_date,
		poh.segment1 po_number,
		por.release_num po_release_number,
		MAX(rsh.creation_date) receipt_date,
		MAX(rsh.receipt_num) receipt_number,
		poh.creation_date po_creation_date
	from
		(
		select
			*
		from
			BEC_ODS.ap_checks_all
		where
			is_deleted_flg <> 'Y')ac,
		(
		select
			*
		from
			BEC_ODS.gl_period_statuses
		where
			is_deleted_flg <> 'Y')gp,
		(
		select
			*
		from
			BEC_ODS.ap_invoice_payments_all
		where
			is_deleted_flg <> 'Y')aip,
		(
		select
			*
		from
			BEC_ODS.ap_invoices_all
		where
			is_deleted_flg <> 'Y')api,
		(
		select
			*
		from
			BEC_ODS.ap_invoice_lines_all
		where
			is_deleted_flg <> 'Y')apil,
		(
		select
			*
		from
			BEC_ODS.ap_payment_schedules_all
		where
			is_deleted_flg <> 'Y')aps,
		(
		select
			*
		from
			BEC_ODS.po_headers_all
		where
			is_deleted_flg <> 'Y')poh,
		(
		select
			*
		from
			BEC_ODS.po_lines_all
		where
			is_deleted_flg <> 'Y')pol,
		(
		select
			*
		from
			BEC_ODS.po_releases_all
		where
			is_deleted_flg <> 'Y')por,
		(
		select
			*
		from
			BEC_ODS.po_line_locations_all
		where
			is_deleted_flg <> 'Y')poll,
		(
		select
			*
		from
			BEC_ODS.rcv_shipment_lines
		where
			is_deleted_flg <> 'Y')rsl,
		(
		select
			*
		from
			BEC_ODS.rcv_shipment_headers
		where
			is_deleted_flg <> 'Y')rsh,
		(
		select
			*
		from
			BEC_ODS.ap_suppliers
		where
			is_deleted_flg <> 'Y')sup,
		(
		select
			aer.invoice_num,
			an.entered_date,
			an.notes_detail
		from
			(
			select
				*
			from
				BEC_ODS.ap_notes
			where
				is_deleted_flg <> 'Y') an,
			(
			select
				*
			from
				BEC_ODS.ap_expense_report_headers_all
			where
				is_deleted_flg <> 'Y') aer
		where
			1 = 1
			and an.source_object_id = aer.report_header_id
			and aer.org_id = 85
			and an.notes_detail like 'Approver Action: Approve%'
			and an.entered_date = (
			select
				MAX(entered_date)
			from
				(
				select
					*
				from
					BEC_ODS.ap_notes
				where
					is_deleted_flg <> 'Y') an1
			where
				an1.source_object_id = an.source_object_id
				and an1.notes_detail like 'Approver Action: Approve%'
                )
        ) exp_rep
	where
		trunc(ac.check_date) between gp.start_date and gp.end_date
		and gp.application_id = 200
		and gp.set_of_books_id = aip.set_of_books_id
		and ac.check_id = aip.check_id (+)
		and aip.invoice_id = apil.invoice_id
		and apil.invoice_id = api.invoice_id
		and api.invoice_id = aps.invoice_id (+)
		and apil.po_line_location_id = poll.line_location_id (+)
		and poll.po_line_id = pol.po_line_id (+)
		and poll.po_release_id = por.po_release_id (+)
		and pol.po_header_id = poh.po_header_id (+)
		and poll.line_location_id = rsl.po_line_location_id (+)
		and rsl.shipment_header_id = rsh.shipment_header_id (+)
		and api.invoice_num = exp_rep.invoice_num (+)
		and api.vendor_id = sup.vendor_id (+)
		-- AND gp.PERIOD_NAME = 'FEB-12'
		--        AND ac.org_id = 85
		--and ac.check_id = '43584026'
		and ac.status_lookup_code <> 'VOIDED'
		--AND apil.LINE_TYPE_LOOKUP_CODE <> 'TAX'
	group by
		ac.org_id,
		ac.check_id ,
		api.invoice_id,
		poh.po_header_id,
		api.vendor_id,
		api.vendor_site_id,
		ac.check_number,
		ac.check_date,
		ac.amount,
		gp.period_num,
		gp.period_year,
		gp.period_name,
		aps.due_date,
		api.invoice_type_lookup_code,
		apil.line_type_lookup_code,
		-- apil.LINE_SOURCE, apil.MATCH_TYPE,
		api.invoice_num,
		api.invoice_amount,
		api.invoice_date,
		api.creation_date,
		exp_rep.entered_date,
		sup.vendor_name,
		poll.po_header_id,
		poll.po_release_id,
		poh.type_lookup_code,
		decode(poh.type_lookup_code,
		'STANDARD',
		poh.APPROVED_DATE,
		'BLANKET',
		por.release_date,
		poh.APPROVED_DATE),
		poh.segment1,
		por.release_num,
		poh.creation_date 
)
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_p2p_cycle_time_rt'
	and batch_name = 'ap';

commit;
