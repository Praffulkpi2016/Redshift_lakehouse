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

drop table if exists bec_dwh.FACT_OM_IR_ISO;

create table bec_dwh.FACT_OM_IR_ISO 
	diststyle all
	sortkey (org_id,header_id)
as
(
select distinct
	ooh.org_id,
    ooh.header_id,
	ool.line_id,
	rql.item_id,
    rqh.requisition_header_id,
    rql.requisition_line_id,
	rql.source_organization_id,
	rqd.distribution_id,
	mmt.transaction_id  ,
	--wdd.delivery_detail_id,
	mta.accounting_line_type,
	mp.organization_code source_organization_code,
	rql.destination_organization_id,
	rql.deliver_to_location_id destination_location,
    hp.party_name customer,
	hca.account_number customer_acc_number,
	rqh.segment1 requistion_number,
	ooh.order_number ISO_NUMBER,
	ooh.flow_status_code header_status,
	ool.flow_status_code iso_line_status,
	DECODE (item_type_code,
                 'MODEL', ool.line_number || '.' || ool.shipment_number,
                 'STANDARD', ool.line_number || '.' || ool.shipment_number,
                 'INCLUDED', ool.line_number
                  || '.'
                  || ool.shipment_number
                  || '.'
                  || DECODE (component_number,
                             NULL, NULL,
                             '.' || component_number
                            ),
                 'SERVICE', ool.line_number
                  || '.'
                  || ool.shipment_number
                  || '.'
                  || component_number
                  || '.'
                  || DECODE (service_number,
                             NULL, NULL,
                             '.' || service_number
                            )
                ) ISO_LINE_NUMBER,
	ool.ordered_item,
	mta.rate_or_amount requisition_unit_price,
	ool.unit_selling_price order_selling_price,
	rql.quantity requisition_quantity,
	MTRH.REQUEST_NUMBER move_order_number,
	MTRL.PICK_SLIP_NUMBER pick_slip_number,
	cta.trx_number ar_invoice_num,
	ctl.revenue_amount ar_invoice_amount,
	aps.gl_date ar_gl_date,
	aia.invoice_num ap_invoice_num,
	aial.amount ap_invoice_amount,
	aia.gl_date ap_gl_date,
	rsh.shipment_num shipment_num,
    rsh.shipped_date shipped_date,
    rsl.quantity_shipped shipped_quantity,
    rsh.receipt_num receipt_num,
	rcv.transaction_date receipt_date,
    rcv.quantity receipt_quantity,
	rcv.subinventory subinventory,
    mp.material_account inv_gl_account_id,
	rsl.mmt_transaction_id,
    'Account' cogs_acct_type,
    'Profit in inventory' markup_acct_type,
    'Intransit Inventory' intransit_acct_type,
    rqh.creation_date,
    rqh.AUTHORIZATION_STATUS,
    rqh.created_by,
    rqh.last_update_date,
    rqh.last_updated_by,	
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || rqh.org_id as org_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || rql.source_organization_id as source_organization_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || rql.destination_organization_id as destination_organization_id_KEY,
	-- audit columns
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
		source_system = 'EBS')
		|| '-' || nvl(rqh.requisition_header_id, 0)
		--|| '-' || nvl(rql.requisition_line_id,0)
		|| '-' || nvl(rqd.distribution_id,0) 
		|| '-' || nvl(mmt.transaction_id ,0)
		--|| '-' || nvl(wdd.delivery_detail_id,0)
        || '-' || nvl(mta.accounting_line_type,0)	
		|| '-' || nvl(rcv.quantity,0)
		|| '-' || nvl(rcv.transaction_date,'1900-01-01')
		|| '-' || nvl(MTRH.REQUEST_NUMBER,'NA') AS dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(select * from bec_ods.po_requisition_headers_all where is_deleted_flg <> 'Y') rqh
inner join (select * from bec_ods.po_requisition_lines_all where is_deleted_flg <> 'Y') rql
on rqh.requisition_header_id = rql.requisition_header_id
inner join (select * from bec_ods.mtl_parameters where is_deleted_flg <> 'Y') mp
on rql.source_organization_id = MP.organization_id
inner join (select * from bec_ods.po_req_distributions_all where is_deleted_flg <> 'Y') rqd
on rql.requisition_line_id   = rqd.requisition_line_id
left outer join (select * from bec_ods.RCV_SHIPMENT_LINES where is_deleted_flg <> 'Y') RSL
on rqd.requisition_line_id   = rsl.requisition_line_id
and rqd.distribution_id       = rsl.req_distribution_id
and rql.source_type_code      = rsl.destination_type_code
left outer join (select * from bec_ods.RCV_SHIPMENT_HEADERS where is_deleted_flg <> 'Y') RSH
on rsl.shipment_header_id    = rsh.shipment_header_id
left outer join (select * from bec_ods.RCV_TRANSACTIONS where is_deleted_flg <> 'Y') RCV
on rsl.shipment_header_id    = rcv.shipment_header_id
and rsl.shipment_line_id      = rcv.shipment_line_id
and rsl.requisition_line_id   = rcv.requisition_line_id
and rsl.req_distribution_id   = rcv.req_distribution_id
and rsl.destination_type_code = rcv.destination_type_code
left outer join (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt
on rsl.mmt_transaction_id    = mmt.transaction_id
left outer join (select * from bec_ods.mtl_transaction_accounts where is_deleted_flg <> 'Y') mta
on mmt.transaction_id        = mta.transaction_id
and mmt.intransit_account     = mta.reference_account
left outer join (select * from bec_ods.ap_invoice_lines_all where is_deleted_flg <> 'Y') aial
on rsl.mmt_transaction_id    = aial.reference_2
left outer join (select * from bec_ods.ap_invoices_all where is_deleted_flg <> 'Y') aia
on aial.invoice_id = aia.invoice_id
left outer join (select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') ctl
on rql.item_id = ctl.inventory_item_id
and aial.source_trx_id = ctl.customer_trx_id
and aial.source_line_id = ctl.customer_trx_line_id
left outer join (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') cta
on ctl.customer_trx_id = cta.customer_trx_id
left outer join (select * from bec_ods.ar_payment_schedules_all where is_deleted_flg <> 'Y') aps
on cta.customer_trx_id = aps.customer_trx_id
and cta.trx_number = aps.trx_number
left outer join (select * from bec_ods.oe_order_lines_all where is_deleted_flg <> 'Y') ool
on ctl.interface_line_attribute9 = ool.header_id::VARCHAR
and ctl.interface_line_attribute6 = ool.line_id::VARCHAR
and rql.item_id = ool.inventory_item_id
and rql.requisition_line_id = ool.source_document_line_id
left outer join (select * from bec_ods.oe_order_headers_all where is_deleted_flg <> 'Y') ooh 
on ool.header_id = ooh.header_id
and ool.org_id = ooh.org_id
left outer join (select * from bec_ods.HZ_CUST_ACCOUNTS where is_deleted_flg <> 'Y') HCA
on OOH.SOLD_TO_ORG_ID = HCA.CUST_ACCOUNT_ID
left outer join (select * from bec_ods.HZ_PARTIES where is_deleted_flg <> 'Y') HP 
on HCA.PARTY_ID = HP.PARTY_ID  
left outer join (select * from bec_ods.WSH_DELIVERY_DETAILS where is_deleted_flg <> 'Y') WDD
on OOL.HEADER_ID = WDD.SOURCE_HEADER_ID
and OOL.LINE_ID = WDD.SOURCE_LINE_ID
left outer join (select * from bec_ods.MTL_TXN_REQUEST_LINES where is_deleted_flg <> 'Y') MTRL		
on WDD.MOVE_ORDER_LINE_ID = MTRL.LINE_ID 
left outer join	(select * from bec_ods.MTL_TXN_REQUEST_HEADERS where is_deleted_flg <> 'Y') MTRH
on MTRL.HEADER_ID = MTRH.HEADER_ID
WHERE 1 = 1
AND rqh.type_lookup_code = 'INTERNAL'
AND rql.source_type_code = 'INVENTORY'
AND ooh.order_source_id = 10
AND ool.order_source_id = 10
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_om_ir_iso'
	and batch_name = 'om';

commit;