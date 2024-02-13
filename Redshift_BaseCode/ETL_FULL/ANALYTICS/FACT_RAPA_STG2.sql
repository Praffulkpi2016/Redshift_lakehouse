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

drop table if exists bec_dwh.FACT_RAPA_STG2;

create table bec_dwh.FACT_RAPA_STG2 
	diststyle all
	sortkey (new_dock_date1)
as 
(
  select 
    inventory_item_id, 
    organization_id, 
    cost_type, 
    plan_name, 
    data_type, 
    order_group, 
	(CASE WHEN order_group = 'CVMI Planned' THEN
	        DECODE(order_type_meas ,NULL, 'CVMI Planned Order (Mat''l Cost)', 'CVMI Planned Order') 
	     WHEN order_group = 'Planned Order' THEN
		    DECODE(order_type_meas ,NULL, 'Planned Order (Mat''l Cost)', 'Planned Order')
		 END
		 )order_type_text,
    order_number, 
    new_dock_date1, 
    aging_period, 
    new_dock_date, 
    item_segments, 
    description, 
	decode(
        planning_make_buy_code, 1, 'Make', 
        2, 'Buy'
      ) planning_make_buy_code, 	
    category_name, 
    quantity, 
    primary_quantity, 
	order_type_meas as calc_unit_cost,
    primary_unit_of_measure, 
	(nvl(order_type_meas,mtl_cost) * quantity) as ext_unit_cost,
    po_line_type, 
    source_vendor_name, 
    mtl_cost, 
    ext_mtl_cost, 
	((nvl(order_type_meas,mtl_cost) * quantity) - ext_mtl_cost) as variance,
    source_organization_id, 
    planner_code, 
    buyer_name, 
    transactional_uom_code, 
    release_num, 
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id as organization_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || source_organization_id as source_organization_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || order_number as order_number_key, 
    -- audit columns
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
    )|| '-' || nvl(order_number, 'NA')|| '-' || nvl(cost_type, 'NA') as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  from 
    (
      (
        WITH CTE_main_stg as (
          WITH CVMI AS (
            SELECT 
              --ASL.ITEM_ID, 
              msi.ORGANIZATION_ID, 
              aps.vendor_name,
              msi.segment1 item_Segments,
              NVL(
                MAX(aging_period), 
                0
              ) AGING_PERIOD, 
              nvl(
                MAX (
                  ASL.CONSIGNED_FROM_SUPPLIER_FLAG
                ), 
                'N'
              ) CVMI_FLAG 
            FROM 
              (select * from bec_ods.PO_ASL_ATTRIBUTES where is_deleted_flg <> 'Y') asl,
              (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi,
              (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps 
            WHERE 
              1 = 1 
              and ASL.ITEM_ID = MSI.INVENTORY_ITEM_ID
              AND ASL.USING_ORGANIZATION_ID = MSI.ORGANIZATION_ID
              and asl.vendor_id = aps.vendor_id 
            group by 
              --ASL.ITEM_ID, 
              msi.ORGANIZATION_ID, 
              aps.vendor_name,
              msi.segment1
          )
		  
          SELECT 
            DECODE(
              cic.cost_type_id, 1, 'Frozen', 3, 'Pending'
            ) cost_type, 
            v.compile_designator, 
            'Receipt Forecast' data_type, 
            'Planned Order' order_group, 
            v.order_number, 
            v.organization_id, 
            v.inventory_item_id, 
            v.new_order_date, 
            0 aging_period, 
            trunc(v.new_dock_date) new_dock_date, 
            v.item_segments, 
            v.description, 
            v.planning_make_buy_code, 
            v.category_name, 
            v.quantity, 
            NULL::numeric(38, 10) primary_quantity, 
            NULL primary_unit_of_measure, 
            NULL po_line_type, 
            v.source_vendor_name, 
            cic.material_cost mtl_cost, 
            cic.material_cost * quantity ext_mtl_cost, 
            v.source_organization_id, 
            v.planner_code, 
            v.buyer_name, 
            (
              SELECT 
                pol.unit_meas_lookup_code 
              FROM 
                (select * from bec_ods.po_lines_all where is_deleted_flg <> 'Y') pol, 
                (select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh 
              WHERE 
                poh.po_header_id = pol.po_header_id 
                AND pol.line_num = v.po_line_id 
                AND poh.segment1 = SUBSTRING (
                  v.order_number, 
                  1, 
                  case when regexp_instr (v.order_number, '[(]', 1, 1) = 0 then 0 else regexp_instr (v.order_number, '[(]', 1, 1) -1 end
                )
            ) transactional_uom_code, 
            NULL::Numeric(15,0) release_num, 
            msi.inventory_item_id :: varchar as inventory_item_id_msi 
          FROM 
            bec_ods.MSC_ORDERS_V v, 
            (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') flv, 
            (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
            (select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y') cic, 
            CVMI cv 
          WHERE 
            1 = 1 
            AND v.wip_supply_type = flv.lookup_code 
            AND flv.lookup_type = 'WIP_SUPPLY' 
            AND v.category_set_id = 9 
            AND v.planning_make_buy_code = 2 
            AND v.order_type_text = 'Planned order' 
            AND msi.organization_id = v.organization_id 
            AND msi.segment1 = v.item_segments 
            AND cic.cost_type_id IN (1, 3) 
            AND cic.organization_id = msi.organization_id 
            AND cic.inventory_item_id = msi.inventory_item_id 
            AND v.item_segments  = cv.item_segments(+) 
            AND v.ORGANIZATION_ID = cv.ORGANIZATION_ID(+) 
            and v.source_vendor_name = cv.vendor_name(+) 
            AND (
              cvmi_flag = 'N' 
              or cvmi_flag is null
            )
        ) 
        select 
          cm.cost_type, 
          cm.compile_designator as plan_name, 
          cm.data_type, 
          cm.order_group, 
          (
            SELECT 
              MAX(qpl.operand) 
            FROM 
              (select * from bec_ods.qp_list_headers_b where is_deleted_flg <> 'Y') qph, 
              (select * from bec_ods.qp_list_headers_tl where is_deleted_flg <> 'Y') qpht, 
              (select * from bec_ods.qp_qualifiers where is_deleted_flg <> 'Y') qq, 
              (select * from bec_ods.qp_list_lines where is_deleted_flg <> 'Y') qpl, 
              (select * from bec_ods.qp_pricing_attributes where is_deleted_flg <> 'Y') qpa, 
              (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps 
            WHERE 
              qph.list_header_id = qpht.list_header_id 
              AND qpht.LANGUAGE = 'US' 
              AND qph.list_header_id = qpl.list_header_id 
              AND qpl.list_line_id = qpa.list_line_id 
              AND qpa.product_attr_value = cm.inventory_item_id_msi 
              AND qph.list_header_id = qq.list_header_id 
              AND qq.qualifier_attr_value = aps.vendor_id 
              AND aps.vendor_name = cm.source_vendor_name 
              AND qph.source_system_code = 'PO' 
              AND Case when qpht.name = 'NEWARK' Then 'NEWARK..' Else qpht.name END = decode(
                cm.compile_designator, 
                'BE-GLOBAL', 
                aps.vendor_name, 
                decode(
                  comt.name, NULL, aps.vendor_name, 
                  aps.vendor_name || '*' || cm.compile_designator
                )
              ) 
              AND cm.new_order_date BETWEEN qpl.start_date_active 
              AND nvl(
                qpl.end_date_active, cm.new_order_date + 1
              )
          ) order_type_meas, 
          cm.order_number, 
          cm.organization_id, 
          cm.inventory_item_id, 
          trunc(cm.new_order_date) new_dock_date1, 
          cm.aging_period, 
          cm.new_dock_date, 
          cm.item_segments, 
          cm.description, 
          cm.planning_make_buy_code, 
          cm.category_name, 
          cm.quantity, 
          cm.primary_quantity, 
          cm.primary_unit_of_measure, 
          cm.po_line_type, 
          cm.source_vendor_name, 
          cm.mtl_cost, 
          cm.ext_mtl_cost, 
          cm.source_organization_id, 
          cm.planner_code, 
          cm.buyer_name, 
          cm.transactional_uom_code, 
          cm.release_num 
        from 
          CTE_main_stg cm 
          Left outer join (
            SELECT  
			  qpht.name,
              aps.vendor_name 
            FROM 
              (select * from bec_ods.qp_list_headers_b where is_deleted_flg <> 'Y') qph, 
              (select * from bec_ods.qp_list_headers_tl where is_deleted_flg <> 'Y') qpht, 
              (select * from bec_ods.qp_qualifiers where is_deleted_flg <> 'Y') qq, 
              (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps 
            WHERE 
              qph.list_header_id = qpht.list_header_id 
              AND qpht.LANGUAGE = 'US' 
              AND qph.list_header_id = qq.list_header_id 
              AND qq.qualifier_attr_value = aps.vendor_id
          ) comt on comt.vendor_name = cm.source_vendor_name 
          and comt.name = comt.vendor_name || '*' || cm.compile_designator
      ) 
      UNION ALL 
        (
          With CTE_main_stg as (
            WITH CVMI AS (
            SELECT 
              --ASL.ITEM_ID, 
              msi.ORGANIZATION_ID, 
              aps.vendor_name,
              msi.segment1 item_Segments,
              NVL(
                MAX(aging_period), 
                0
              ) AGING_PERIOD, 
              nvl(
                MAX (
                  ASL.CONSIGNED_FROM_SUPPLIER_FLAG
                ), 
                'N'
              ) CVMI_FLAG 
            FROM 
              (select * from bec_ods.PO_ASL_ATTRIBUTES where is_deleted_flg <> 'Y') asl,
              (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi,
              (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps 
            WHERE 
              1 = 1 
              and ASL.ITEM_ID = MSI.INVENTORY_ITEM_ID
              AND ASL.USING_ORGANIZATION_ID = MSI.ORGANIZATION_ID
              and asl.vendor_id = aps.vendor_id 
            group by 
              --ASL.ITEM_ID, 
              msi.ORGANIZATION_ID, 
              aps.vendor_name,
              msi.segment1
          ) 
            SELECT 
              DECODE(
                cic.cost_type_id, 1, 'Frozen', 3, 'Pending'
              ) cost_type, 
              v.compile_designator, 
              'Receipt Forecast' data_type, 
              'CVMI Planned' order_group, 
              v.order_number, 
              v.organization_id, 
              v.inventory_item_id, 
              trunc(v.new_dock_date) new_dock_date1, 
              (
                SELECT 
                  MAX(aging_period) 
                FROM 
                  (select * from bec_ods.po_asl_attributes where is_deleted_flg <> 'Y') pasl, 
                  (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps 
                WHERE 
                  aging_period IS NOT NULL 
                  AND pasl.vendor_id = aps.vendor_id 
                  AND pasl.using_organization_id = msi.organization_id 
                  AND pasl.item_id = msi.inventory_item_id 
                  AND pasl.using_organization_id = v.organization_id 
                  AND aps.vendor_name = v.source_vendor_name
              ) aging_period, 
              v.new_dock_date, 
              v.item_segments, 
              v.description, 
              v.planning_make_buy_code, 
              v.category_name, 
              v.quantity, 
              NULL::numeric(38, 10) primary_quantity, 
              NULL primary_unit_of_measure, 
              NULL po_line_type, 
              v.source_vendor_name, 
              cic.material_cost mtl_cost, 
              cic.material_cost * quantity ext_mtl_cost, 
              v.source_organization_id, 
              v.planner_code, 
              v.buyer_name, 
              (
                SELECT 
                  pol.unit_meas_lookup_code 
                FROM 
                  (select * from bec_ods.po_lines_all where is_deleted_flg <> 'Y') pol, 
                  (select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh 
                WHERE 
                  poh.po_header_id = pol.po_header_id 
                  AND pol.line_num = v.po_line_id 
                  AND poh.segment1 = SUBSTRING (
                    v.order_number, 
                    1, 
                    case when regexp_instr (v.order_number, '[(]', 1, 1) = 0 then 0 else regexp_instr (v.order_number, '[(]', 1, 1) -1 end
                  )
              ) transactional_uom_code, 
              NULL::Numeric(15,0) release_num, 
              msi.inventory_item_id :: varchar as inventory_item_id_msi 
            FROM 
              bec_ods.MSC_ORDERS_V v, 
              (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') flv, 
              (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
              (select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y') cic, 
              CVMI cv 
            WHERE 
              1 = 1 
              AND v.wip_supply_type = flv.lookup_code 
              AND flv.lookup_type = 'WIP_SUPPLY' 
              AND v.category_set_id = 9 
              AND v.planning_make_buy_code = 2 
              AND order_type_text = 'Planned order' 
              AND msi.organization_id = v.organization_id 
              AND msi.segment1 = v.item_segments 
              AND cic.cost_type_id IN (1, 3) 
              AND cic.organization_id = msi.organization_id 
              AND cic.inventory_item_id = msi.inventory_item_id 
              AND v.item_segments  = cv.item_segments(+) 
              AND v.ORGANIZATION_ID = cv.ORGANIZATION_ID(+) 
              AND v.source_vendor_name = cv.vendor_name(+) 
              AND (cvmi_flag = 'Y')
          ) 
          select 
            cm.cost_type, 
            cm.compile_designator as plan_name, 
            cm.data_type, 
            cm.order_group, 
            (
              SELECT 
                MAX(qpl.operand) 
              FROM 
                (select * from bec_ods.qp_list_headers_b where is_deleted_flg <> 'Y') qph, 
                (select * from bec_ods.qp_list_headers_tl where is_deleted_flg <> 'Y') qpht, 
                (select * from bec_ods.qp_qualifiers where is_deleted_flg <> 'Y') qq, 
                (select * from bec_ods.qp_list_lines where is_deleted_flg <> 'Y') qpl, 
                (select * from bec_ods.qp_pricing_attributes where is_deleted_flg <> 'Y') qpa, 
                (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps 
              WHERE 
                1 = 1 
                AND qph.list_header_id = qpht.list_header_id 
                AND qpht.LANGUAGE = 'US' 
                AND qph.list_header_id = qpl.list_header_id 
                AND qpl.list_line_id = qpa.list_line_id 
                AND qpa.product_attr_value = cm.inventory_item_id_msi 
                AND qph.list_header_id = qq.list_header_id 
                AND qq.qualifier_attr_value = aps.vendor_id 
                AND aps.vendor_name = cm.source_vendor_name 
                AND qph.source_system_code = 'PO' 
                AND Case when qpht.name = 'NEWARK' Then 'NEWARK..' Else qpht.name END = decode(
                  cm.compile_designator, 
                  'BE-GLOBAL', 
                  aps.vendor_name, 
                  decode(
                    comt.name, NULL, aps.vendor_name, 
                    aps.vendor_name || '*' || cm.compile_designator
                  )
                ) 
                AND cm.new_dock_date BETWEEN qpl.start_date_active 
                AND nvl(
                  qpl.end_date_active, cm.new_dock_date + 1
                )
            ) order_type_meas, 
            cm.order_number, 
            cm.organization_id, 
            cm.inventory_item_id, 
            cm.new_dock_date1, 
            cm.aging_period, 
            trunc(cm.new_dock_date) new_dock_date, 
            cm.item_segments, 
            cm.description, 
            cm.planning_make_buy_code, 
            cm.category_name, 
            cm.quantity, 
            cm.primary_quantity, 
            cm.primary_unit_of_measure, 
            cm.po_line_type, 
            cm.source_vendor_name, 
            cm.mtl_cost, 
            cm.ext_mtl_cost, 
            cm.source_organization_id, 
            cm.planner_code, 
            cm.buyer_name, 
            cm.transactional_uom_code, 
            cm.release_num 
          from 
            CTE_main_stg cm 
            Left outer join (
              SELECT 
				qpht.name, 
                aps.vendor_name 
              FROM 
                (select * from bec_ods.qp_list_headers_b where is_deleted_flg <> 'Y') qph, 
                (select * from bec_ods.qp_list_headers_tl where is_deleted_flg <> 'Y') qpht, 
                (select * from bec_ods.qp_qualifiers where is_deleted_flg <> 'Y') qq, 
                (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps 
              WHERE 
                qph.list_header_id = qpht.list_header_id 
                AND qpht.LANGUAGE = 'US' 
                AND qph.list_header_id = qq.list_header_id 
                AND qq.qualifier_attr_value = aps.vendor_id
            ) comt on comt.vendor_name = cm.source_vendor_name 
            and comt.name = comt.vendor_name || '*' || cm.compile_designator
        )
    )
);

commit;

end;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_rapa_stg2' 
  and batch_name = 'ascp';
  
commit;
