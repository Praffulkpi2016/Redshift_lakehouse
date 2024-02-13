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

drop table if exists bec_dwh.FACT_RAPA_STG9;

create table bec_dwh.FACT_RAPA_STG9
	diststyle all
	sortkey (inventory_item_id,organization_id,source_organization_id)
as
(
select
	   inventory_item_id,
       organization_id,
	   cost_type,
	   plan_name,
	   data_type,
	   order_group,
	   order_type_text,
	   po_number,
	   promised_date1,
	   aging_period,
	   promised_date,
	   purchase_item,
	   item_description,
	   planning_make_buy_code,
	   category_name,
	   po_open_qty,
	   primary_quantity,
	   unit_price,
	   primary_unit_of_measure,
	   po_open_amount,
	   po_line_type,
	   suggested_vendor_name,
	   material_cost,
	   ext_mtl_cost,
	   VARIANCE,
	   source_organization_id,
	   planner_code,
	   buyer_name,
	   transactional_uom_code,
	   release_num,  
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id as organization_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || source_organization_id as source_organization_id_key,
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
		source_system = 'EBS')|| '-' || nvl(cost_type, 'NA')|| '-' || nvl(plan_name, 'NA') as dw_load_id, 
	
	getdate() as dw_insert_date,
	getdate() as dw_update_date 
from

---- ORIGINAL QUERY STARTS HERE

(SELECT inventory_item_id
      ,ship_to_organization_id organization_id 
	  ,cost_type
	  ,plan_name
	  ,data_type
	  ,order_group
	  ,order_type_text
	  ,po_number
	  ,promised_date1
	  ,aging_period
	  ,promised_date
	  ,purchase_item
	  ,item_description
	  ,planning_make_buy_code
	  ,category_name
	  ,po_open_qty
	  ,NULL::numeric(38, 10) as primary_quantity
	  ,unit_price
	  ,primary_unit_of_measure
	  ,po_open_amount1*unit_price as po_open_amount
	  ,po_line_type
	  ,suggested_vendor_name
	  ,material_cost
	  ,material_cost*po_open_qty ext_mtl_cost
	  ,nvl(po_open_qty*unit_price - material_cost*po_open_qty,0) as VARIANCE
	  ,source_organization_id
	  ,planner_code
	  ,buyer_name
	  ,transactional_uom_code
	  ,release_num
FROM (SELECT
	decode(cic.cost_type_id, 1, 'Frozen', 3, 'Pending') cost_type,
	NULL plan_name,
	'Receipt Forecast' data_type,
	'Open PR' order_group,
	'Purchase Requisitions' order_type_text,
	prh.segment1 po_number,
	destination_organization_id ship_to_organization_id,
    msi.inventory_item_id,
	trunc(prl.need_by_date) promised_date1,
	0 aging_period,
	trunc(need_by_date) promised_date,
	msi.segment1 purchase_item,
	msi.description item_description,
	decode(msi.planning_make_buy_code, 1, 'Make', 2, 'Buy') planning_make_buy_code,
	( select
	mc.segment1
                || '.'
                || mc.segment2
	from
	bec_ods.mtl_categories_b mc,
	bec_ods.mtl_item_categories mic
	where
	mic.category_id = mc.category_id
	and mic.category_set_id = 1
    AND mic.inventory_item_id = msi.inventory_item_id
    AND mic.organization_id = msi.organization_id
	) category_name,
	nvl(prl.quantity, 0) - nvl(prl.quantity_cancelled, 0) - nvl(prl.quantity_received, 0) po_open_qty,
	decode(prl.unit_price, 0, nvl((
            SELECT
                MAX(qpl.operand)
            FROM
			    (select * from bec_ods.qp_list_headers_b where is_deleted_flg<>'Y') qph    , 
				(select * from bec_ods.qp_list_headers_tl where is_deleted_flg<>'Y') qpht  ,
				(select * from bec_ods.qp_qualifiers where is_deleted_flg<>'Y') qq         ,
				(select * from bec_ods.qp_list_lines where is_deleted_flg<>'Y') qpl        ,
				(select * from bec_ods.qp_pricing_attributes where is_deleted_flg<>'Y') qpa,
				(select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') msi1  ,
				(select * from bec_ods.ap_suppliers where is_deleted_flg<>'Y') aps1 
            WHERE qph.list_header_id = qpht.list_header_id
                AND qpht.LANGUAGE = 'US'
                AND qph.list_header_id = qpl.list_header_id
                AND qpl.list_line_id = qpa.list_line_id
                AND qpa.product_attr_value = (msi1.inventory_item_id)::character 
                AND qph.list_header_id = qq.list_header_id
                AND qq.qualifier_attr_value = prl.vendor_id
--(select vendor_name from ap_suppliers where vendor_name =suggested_vendor_name)
                AND aps1.vendor_name = suggested_vendor_name
                AND msi1.organization_id = 90
                AND qph.source_system_code = 'PO'
                AND qpht.name = suggested_vendor_name
                AND msi1.inventory_item_id = msi.inventory_item_id
                AND prl.need_by_date BETWEEN qpl.start_date_active AND nvl(qpl.end_date_active, need_by_date + 1)
        ), 0), prl.unit_price) unit_price,
	primary_unit_of_measure,
	( nvl(prl.quantity, 0) - nvl(prl.quantity_cancelled, 0) - nvl(prl.quantity_received, 0) )  po_open_amount1,
	NULL po_line_type,
	suggested_vendor_name,
	cic.material_cost,
	cic.material_cost * ( nvl(prl.quantity, 0) - nvl(prl.quantity_cancelled, 0) - nvl(prl.quantity_received, 0) ) ext_mtl_cost,
	NULL::Numeric(15,0) source_organization_id,
	msi.planner_code,
	(
	SELECT
		agent_name
	FROM
		bec_ods.po_agents_v
	WHERE
		agent_id = msi.buyer_id
		and is_deleted_flg<>'Y'
        ) buyer_name,
	unit_meas_lookup_code transactional_uom_code,
	NULL::Numeric(15,0) release_num
FROM
	(select * from bec_ods.po_requisition_lines_all where is_deleted_flg<>'Y') prl,
	(select * from bec_ods.po_requisition_headers_all where is_deleted_flg<>'Y') prh ,
	(select * from bec_ods.cst_item_costs where is_deleted_flg<>'Y') cic ,
	(select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') msi 
WHERE
	prh.requisition_header_id = prl.requisition_header_id
	AND prl.item_id = cic.inventory_item_id
	AND prl.destination_organization_id = msi.organization_id
	AND prl.item_id = msi.inventory_item_id
	AND prl.destination_organization_id = cic.organization_id
	AND cic.cost_type_id IN ( 1, 3 )
	AND NOT EXISTS ( SELECT 1
	FROM
		(select * from bec_ods.po_distributions_all where is_deleted_flg<>'Y') pod,
	    (select * from bec_ods.po_req_distributions_all where is_deleted_flg<>'Y') prd 
	WHERE
		pod.req_distribution_id = prd.distribution_id
		AND prd.requisition_line_id = prl.requisition_line_id
        )
	AND nvl(prh.cancel_flag, 'N') = 'N'
	AND nvl(prl.cancel_flag, 'N') = 'N'
	AND prl.item_id IS NOT NULL
	--AND prh.org_id = 85
	AND prl.source_organization_id IS NULL
	AND destination_organization_id = 265 --FS1
	AND prl.destination_type_code = 'INVENTORY'
	AND prh.interface_source_code = 'MSC'
	--and prh.segment1 ='136355'
	AND prh.authorization_status = 'APPROVED'
	)
    )
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_rapa_stg9'
	and batch_name = 'ascp';

commit;