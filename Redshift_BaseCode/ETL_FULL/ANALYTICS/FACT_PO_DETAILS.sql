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

drop table if exists bec_dwh.FACT_PO_DETAILS;

CREATE TABLE bec_dwh.FACT_PO_DETAILS
DISTKEY(PO_HEADER_ID)
SORTKEY(PO_HEADER_ID, PO_LINE_ID, PROMISED_DATE, CLOSED_CODE)
AS (
SELECT a.po_header_id po_header_id,
  a.agent_id,
  a.type_lookup_code,
  a.last_updated_by,
  a.segment1 po_number,
  a.summary_flag,
  a.enabled_flag,
  a.segment2,
  a.segment3,
  a.segment4,
  a.segment5,
  a.start_date_active,
  A.END_DATE_ACTIVE,
  TRUNC(a.creation_date) creation_date,
  TO_CHAR (a.creation_date, 'MON-YYYY') po_creation_month,
  vendor_id,
  VENDOR_SITE_ID,
  a.created_by,
  (select system_id from bec_etl_ctrl.etlsourceappid 
	where source_system='EBS')||'-'||nvl(a.vendor_id, 0) PO_SUPPLIER_ID_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid 
	where source_system='EBS')||'-'|| nvl(a.vendor_site_id,0) PO_SUPPLIER_SITE_ID_KEY,
  a.vendor_contact_id,
  a.ship_to_location_id,
  a.bill_to_location_id,
  a.terms_id,
  a.ship_via_lookup_code,
  a.fob_lookup_code,
  a.freight_terms_lookup_code,
  a.currency_code,
  a.start_date,
  a.end_date,
  a.blanket_total_amount,
  a.authorization_status,
  a.revision_num,
  a.revised_date,
  a.approved_flag,
  a.approved_date,
  a.amount_limit,
  a.print_count,
  a.printed_date,
  a.vendor_order_num,
  a.rfq_close_date,
  a.closed_date,
  a.cancel_flag,
  a.closed_code as closed_code,
  b.closed_code as line_closed_code,
  poll.closed_code as location_closed_code,
  a.org_id,
  b.po_line_id po_line_id,
  b.last_update_date line_last_update,
  b.last_updated_by line_updated_by,
  b.line_type_id,
  (b.line_num :: varchar) line_num,
  b.creation_date line_creation_date,
  b.created_by line_created_by,
  b.item_id,
  fsp.inventory_organization_id,
  B.ITEM_ID "INVENTORY_ITEM_ID",
  b.item_revision,
  b.category_id,
  b.item_description,
  b.unit_meas_lookup_code,
  b.quantity_committed,
  b.committed_amount,
  b.list_price_per_unit,
  b.unit_price,
  b.quantity,
  (b.quantity * b.unit_price) po_amont,
  b.un_number_id,
  b.hazard_class_id,
  b.qty_rcv_tolerance,
  b.over_tolerance_error_flag,
  b.market_price,
  b.cancel_flag line_cancel_flag,
  b.cancelled_by line_cancelled_by,
  b.cancel_date line_cancel_date,
  b.cancel_reason line_cancel_reason,
  b.taxable_flag taxable_flag,
  b.type_1099,
  b.capital_expense_flag,
  b.attribute1,
  b.attribute2,
  b.attribute3,
  b.attribute4,
  b.attribute5,
  b.attribute6,
  b.attribute7,
  b.attribute8,
  b.attribute9,
  b.attribute10,
  b.closed_date line_closed_date,
  b.closed_reason line_closed_reason,
  poll.promised_date,
  poll.CVMI_FLAG,
  pr.release_date po_date,
  pr.release_num,
  a.clm_document_number as document_num,
  a.status_lookup_code,
  0 as asn_po_header_id,
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
       || nvl(a.po_header_id, 0)
	   || '-'
       || nvl(b.po_line_id, 0)
 as dw_load_id, 
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM (select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') a
left outer join 
(select * from bec_ods.po_lines_all where is_deleted_flg <> 'Y') b on a.po_header_id = b.po_header_id
left outer join (select * from bec_ods.financials_system_params_all where is_deleted_flg <> 'Y') fsp on a.org_id         = fsp.org_id
left outer join
(select max(promised_date) as promised_date ,po_header_id ,po_line_id,max(closed_code) as closed_code, NVL(consigned_flag,'N') as CVMI_FLAG
 ,max(po_release_id) as po_release_id
from (select * from bec_ods.po_line_locations_all  where is_deleted_flg <> 'Y')
group by po_header_id ,po_line_id, NVL(consigned_flag,'N')
)poll
on (a.po_header_id = poll.po_header_id
and b.po_line_id = poll.po_line_id)
left outer join
(select * from bec_ods.po_releases_all where is_deleted_flg <> 'Y') pr
on (poll.po_release_id = pr.po_release_id
   and poll.po_header_id = pr.po_header_id )
)
;

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_details'
	and batch_name = 'po';

commit;