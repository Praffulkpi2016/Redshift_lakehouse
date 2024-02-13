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

drop table if exists bec_dwh_rpt.FACT_SUPPLIER_PRICE_LIST_RT;

create table bec_dwh_rpt.FACT_SUPPLIER_PRICE_LIST_RT 
	diststyle all as (WITH 
item_category AS (SELECT
		mc.segment1,
		mc.segment2,
		mic.inventory_item_id,
		mic.organization_id,
		(DECODE(STRUCTURE_ID,  
      3, SEGMENT1,  
      4, SEGMENT1,  
      5, SEGMENT1,  
      6, SEGMENT1,  
      101, SEGMENT3 || '.' || SEGMENT4 || '.' || SEGMENT1 || '.' || SEGMENT2 || '.' || SEGMENT5 || '.' || SEGMENT6,  
      201, SEGMENT1,  
      50136, SEGMENT1 || '.' || SEGMENT2 || '.' || SEGMENT3,  
      50152, SEGMENT1,  
      50153, SEGMENT1,  
      50168, SEGMENT1 || '.' || SEGMENT2,  
      50169, SEGMENT1,  
      50190, SEGMENT1 || '.' || SEGMENT2 || '.' || SEGMENT3 || '.' || SEGMENT4,  
      50208, SEGMENT1,  
      50229, SEGMENT1,  
      50268, SEGMENT1,  
      50272, SEGMENT1,  
      50309, SEGMENT1 || '.' || SEGMENT2,  
      50312, SEGMENT1 || '.' || SEGMENT2,  
      50328, SEGMENT1 || '.' || SEGMENT2,  
      50348, SEGMENT1,  
      50368, SEGMENT1,  
      50388, SEGMENT1 || '~' || SEGMENT2,  
      50409, SEGMENT1, NULL)) concatenated_segments
	FROM
		bec_ods.mtl_item_categories mic,
		bec_ods.MTL_CATEGORIES_B mc
	WHERE
		mic.category_id = mc.category_id
		AND mic.organization_id = 106
		AND category_set_id = 1
)
 SELECT aps.vendor_id,qph.list_header_id, qpl.list_line_id,cat.inventory_item_id,cat.organization_id, 
	aps.vendor_name supplier,
	QPHT.NAME price_list_name,
	msi.segment1 part_number,
	msi.description part_description,
	qpl.list_price_uom_code uom_code,
	cat.segment1 subsystem,
	cat.segment2 category,
	QPL.operand unit_price,
	qpl.start_date_active effectivity_start_date,
	qpl.end_date_active effectivity_end_date,
	cic.material_cost,
	cat.concatenated_segments complete_category
FROM
	bec_ods.qp_list_headers_b qph,
	bec_ods.qp_list_headers_tl qpht,
	bec_ods.qp_list_lines qpl,
	bec_ods.qp_qualifiers qpq,
	bec_ods.ap_suppliers aps,
	bec_ods.qp_pricing_attributes qpa,
	bec_ods.mtl_system_items_b msi,
	item_category cat
	,bec_ods.cst_item_costs cic
WHERE  qph.list_header_id = qpht.list_header_id 
    AND qpht.LANGUAGE = 'US'
	AND qph.list_header_id = qpl.list_header_id
	AND qpl.LIST_LINE_ID = qpa.list_line_id
	AND qpa.product_attr_value = msi.inventory_item_id
	AND msi.organization_id = 90
	--and qph.creation_date < sysdate-1
	AND qph.source_system_code = 'PO'
	AND qph.end_date_active IS NULL
	--and qpl.end_date_active is null
	AND qph.list_header_id = qpq.list_header_id(+)
	AND qpq.qualifier_attr_value = aps.vendor_id(+)
	AND msi.inventory_item_id = cat.Inventory_item_id(+)
	AND cic.cost_type_id(+) = 1
	AND cic.organization_id(+) = 106
	AND cic.inventory_item_id(+) = msi.inventory_item_id);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_supplier_price_list_rt'
	and batch_name = 'ap';

commit;