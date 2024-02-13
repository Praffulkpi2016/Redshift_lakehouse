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

drop table if exists bec_dwh_rpt.FACT_CST_ITEM_COST_RT;

create table bec_dwh_rpt.FACT_CST_ITEM_COST_RT --3806330
	diststyle all as (
	WITH po_details AS (SELECT item_id, max(poh.segment1) po_number,
                max(pol.creation_date), 
				max(UNIT_MEAS_LOOKUP_CODE) po_uom, MAX (unit_price) po_price 
FROM bec_ods.po_lines_all pol, bec_ods.po_headers_all poh
WHERE pol.org_id = 85
AND poh.po_header_id = pol.po_header_id 
group by item_id
 ),
rcv_details AS (select rsl.item_id, 
       rsl.to_organization_id,
	   max(rsl.po_header_id) po_header_id,
	   max(rsl.shipment_line_id) shipment_line_id
from  bec_ods.rcv_shipment_headers rsh, bec_ods.rcv_shipment_lines rsl
where  rsh.SHIPMENT_HEADER_ID = rsl.SHIPMENT_HEADER_ID
and rsh.RECEIPT_NUM is not null
group by rsl.item_id, 
       rsl.to_organization_id
 )
SELECT 	cic.organization_id ,
	cic.inventory_item_id ,
	cic.cost_type_id,
	msi.segment1 part_number ,
	msi.description,
	msi.creation_date item_creation_date,
	msi.primary_uom_code,
	msi.primary_unit_of_measure, --MSI.COSTING_ENABLED_FLAG
	msi.FULL_LEAD_TIME lead_time,
	lu1.meaning planning_make_buy_code,
	msi.inventory_item_status_code ,
	--msi.planning_make_buy_code,
	cct.cost_type,
	cct.cost_type default_cost_type,
	msi.planner_code,
	--msi.organization_code,
	--msi.organization_name,
    DECODE (cic.based_on_rollup_flag,
                  1, 'Yes',
                  2, 'No',
                  3, 'Default',
                  cic.based_on_rollup_flag::char
                 )  based_on_rollup_flag,
    cic.item_cost ,
    NVL (cic.material_cost, 0) material_cost,
	cic.material_overhead_cost material_overhead,
    cic.resource_cost, 
	cic.overhead_cost, 
	cic.outside_processing_cost,
	mic.item_category_segment1,
	mic.item_category_segment2,
	msi.BUYER_ID,
	po.po_number last_po,
	po.po_uom  last_po_uom,
	(po.po_price*(nvl((
            SELECT
                conversion_rate
            FROM
                bec_ods.mtl_uom_conversions muc
            WHERE
                    unit_of_measure = po.po_uom
                AND muc.inventory_item_id = msi.inventory_item_id
        ), nvl((
            SELECT
                conversion_rate
            FROM
                bec_ods.mtl_uom_conversions
            WHERE
                    unit_of_measure = po.po_uom
                AND inventory_item_id = 0
        ), 1)) / nvl((
            SELECT
                conversion_rate
            FROM
                bec_ods.mtl_uom_conversions muc
            WHERE
                    unit_of_measure = msi.primary_unit_of_measure
                AND muc.inventory_item_id = msi.inventory_item_id
        ), nvl((
            SELECT
                conversion_rate
            FROM
                bec_ods.mtl_uom_conversions muc
            WHERE
                    unit_of_measure = msi.primary_unit_of_measure
                AND muc.inventory_item_id = 0
        ), 1)))
        ) last_po_price,
	(select segment1 --into l_po_number 
	 from bec_ods.po_headers_all
     where po_header_id = rcv.po_header_id
	 ) last_receipt_po,
	( select max((po_unit_price*rt.quantity )/rt.primary_quantity) --into l_unit_price 
	  from bec_ods.rcv_transactions rt
             where shipment_line_id = rcv.shipment_line_id	
			 AND rt.organization_id = cic.organization_id
               AND rt.destination_type_code = 'INVENTORY'
	) last_receipt_price,
	(select max(rt.SOURCE_DOC_UNIT_OF_MEASURE) --into l_recpt_uom
	   from bec_ods.rcv_transactions rt
             where shipment_line_id = rcv.shipment_line_id
              AND rt.organization_id = cic.organization_id
               AND rt.destination_type_code = 'INVENTORY'			 
	) last_receipt_uom,
	(SELECT max(vendor_id) 
	 from  bec_ods.po_headers_all poh
     where 1=1
     and poh.SEGMENT1 = po.po_number
	) vendor_id
FROM
	 bec_ods.mtl_system_items_b msi
	,bec_ods.cst_item_costs cic
	,bec_dwh.dim_cost_types cct
	,bec_dwh.dim_cost_types cct2
	,bec_dwh.dim_inv_item_category_set mic
	,bec_dwh.dim_lookups LU1
	,po_details PO
	,rcv_details rcv
	--,bec_dwh.dim_inv_safty_stock  mss
WHERE msi.inventory_item_id = cic.inventory_item_id
AND   msi.organization_id   = cic.organization_id
AND   MSI.COSTING_ENABLED_FLAG = 'Y'
AND   cic.cost_type_id       = cct.cost_type_id
AND   cic.cost_type_id       = 1
AND   CCT2.COST_TYPE_ID      = CCT.DEFAULT_COST_TYPE_ID
AND   cic.ORGANIZATION_ID    = mic.ORGANIZATION_ID(+)
AND   cic.INVENTORY_ITEM_ID  = mic.INVENTORY_ITEM_ID(+)
AND   mic.category_set_id(+) = 1 
AND   LU1.LOOKUP_CODE(+)     = MSI.PLANNING_MAKE_BUY_CODE
AND   LU1.LOOKUP_TYPE(+)     = 'MTL_PLANNING_MAKE_BUY'  
AND   cic.inventory_item_id  = po.item_id(+)
AND   cic.inventory_item_id  = rcv.item_id(+)
AND   cic.organization_id    = rcv.to_organization_id(+) 
 ) 
 ;
 
 end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate() 
where
	dw_table_name = 'fact_cst_item_cost_rt'
	and batch_name = 'inv';

commit;