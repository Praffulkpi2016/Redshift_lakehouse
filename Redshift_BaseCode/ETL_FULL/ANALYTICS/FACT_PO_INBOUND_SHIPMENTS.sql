/*# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0begin; */

Begin;

drop table if exists bec_dwh.FACT_PO_INBOUND_SHIPMENTS;

create table bec_dwh.FACT_PO_INBOUND_SHIPMENTS distkey(ORGANIZATION_ID) sortkey(SHIPMENT_HEADER_ID,SHIPMENT_LINE_ID,
ORGANIZATION_ID,TRANSACTION_DATE)
as 
(
SELECT T.TRANSACTION_ID,
    T.TRANSACTION_DATE,
    a.shipment_header_id,
    a.creation_date header_creation_date,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||OD.ORGANIZATION_ID as ORGANIZATION_ID_KEY,
    a.receipt_source_code,
    a.vendor_id,
    a.vendor_site_id,
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||a.vendor_id as VENDOR_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||a.vendor_site_id as VENDOR_SITE_ID_KEY,
    OD.OPERATING_UNIT ORG_ID,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||OD.OPERATING_UNIT as ORG_ID_KEY,
    A.organization_id "SOURCE_ORG_ID",
    OD.ORGANIZATION_ID,
    a.shipment_num,
    a.receipt_num,
    a.bill_of_lading,
    a.packing_slip,
    a.shipped_date,
    a.freight_carrier_code,
    a.num_of_containers,
    a.waybill_airbill_num,
    a.government_context,
    a.edi_control_num,
    a.hazard_code,
    a.invoice_num,
    a.invoice_date,
    a.invoice_amount,
    a.tax_amount header_tax_amount,
    a.freight_amount,
    a.currency_code,
    a.payment_terms_id,
    a.ship_to_org_id,
    a.ship_to_location_id,
    b.shipment_line_id,
    b.last_update_date line_last_update,
    b.last_updated_by line_updated_by,
    b.creation_date line_creation_date,
    b.created_by line_created_by,
    b.line_num line_num,
    b.category_id,
    b.quantity_shipped,
    b.quantity_received,
    b.unit_of_measure,
    b.item_description,
    b.item_id,
    b.to_organization_id "INVENTORY_ORG_ID",
    B.ITEM_ID "INVENTORY_ITEM_ID",
    b.item_revision,
    b.vendor_item_num,
    b.vendor_lot_num,
    b.shipment_line_status_code,
    b.source_document_code,
    b.po_header_id,
    b.po_release_id,
    b.po_line_id,
    b.to_organization_id,
    b.po_line_location_id,
    b.po_distribution_id,
    b.requisition_line_id,
    b.req_distribution_id,
    b.routing_header_id,
    b.from_organization_id,
    b.deliver_to_person_id,
    b.employee_id,
    b.destination_type_code,
    b.to_subinventory,
    b.locator_id,
    b.deliver_to_location_id,
    b.comments,
    b.reason_id,
    b.ussgl_transaction_code,
    b.primary_unit_of_measure,
    b.vendor_cum_shipped_quantity,
    b.notice_unit_price,
    b.tax_name,
    b.tax_amount,
    b.container_num,
    b.truck_num,
    T.QUANTITY "TRANSACTION_QUANTITY",
	b.AMOUNT_RECEIVED,
	a.expected_receipt_date,
	cast(NVL(a.invoice_amount,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INVOICE_AMOUNT,
    cast(NVL(b.AMOUNT_RECEIVED,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_PAID,
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
       || nvl(a.SHIPMENT_HEADER_ID, 0) 
	|| '-'|| nvl(b.SHIPMENT_LINE_ID,0)   
	|| '-'|| nvl(OD.ORGANIZATION_ID,0) 
	|| '-'|| nvl(T.TRANSACTION_ID,0) as dw_load_id,
    getdate() as dw_insert_date,
    getdate() as dw_update_date
	
  FROM  (select * from bec_ods.rcv_shipment_headers where is_deleted_flg<>'Y') a,
		(select * from bec_ods.rcv_shipment_lines where is_deleted_flg<>'Y') b,
		(select * from bec_ods.org_organization_definitions where is_deleted_flg<>'Y') OD,
		(select * from bec_ods.RCV_TRANSACTIONS where is_deleted_flg<>'Y') T,
		(select * from bec_ods.GL_DAILY_RATES where is_deleted_flg<>'Y') DCR
  WHERE A.SHIP_TO_ORG_ID   = OD.ORGANIZATION_ID
  AND A.SHIPMENT_HEADER_ID = B.SHIPMENT_HEADER_ID
  AND B.SHIPMENT_LINE_ID   = T.SHIPMENT_LINE_ID
  AND T.TRANSACTION_TYPE   = 'RECEIVE'
  and DCR.to_currency(+) = 'USD'
  and DCR.conversion_type(+) = 'Corporate'
  and A.currency_code = DCR.from_currency(+)
  and DCR.conversion_date(+) = A.invoice_date
 );
 
 END;
 
 update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_inbound_shipments'
	and batch_name = 'po';

commit;