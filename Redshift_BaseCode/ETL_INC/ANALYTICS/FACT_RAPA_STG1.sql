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

Truncate table bec_dwh.FACT_RAPA_STG1;

Insert Into bec_dwh.FACT_RAPA_STG1
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
	new_dock_date1, 
	aging_period, 
	dock_promised_date, 
	part_number, 
	description, 
	planning_make_buy_code, 
	NULL as category_name,
	quantity, 
	primary_quantity, 
	unit_cost, 
	primary_uom_code, 
	extended_cost, 
	po_line_type, 
	VENDOR_NAME, 
	mtl_cost, 
	ext_mtl_cost, 
	variance, 
	source_organization_id, 
	planner_code, 
	buyer_name, 
	transactional_uom_code, 
	release_num,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id as organization_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || source_organization_id as source_organization_id_KEY,
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
		source_system = 'EBS')|| '-' || nvl(cost_type, 'NA')|| '-' || nvl(plan_name, 'NA')
	|| '-' || nvl(new_dock_date1, '1900-01-01 12:00:00')|| '-' || nvl(quantity, 0)|| '-' || nvl(unit_cost, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(SELECT 
  inventory_item_id, 
  organization_id, 
  cost_type, 
  plan_name, 
  data_type, 
  order_group, 
  (
    CASE WHEN (
      unit_price is not null 
      AND osign >= 0
    ) THEN 'CVMI Future Consumption' WHEN operand IS NOT NULL THEN 'CVMI Future Consumption' ELSE 'CVMI Future Consumption (Mat''l Cost)' END
  ) order_type_text, 
  order_number po_number, 
  new_dock_date1, 
  aging_period, 
  dock_promised_date, 
  part_number, 
  description, 
  planning_make_buy_code, 
  decode(
    cvmi_count, 0, future_consumptions * allocation_percent, 
    future_consumptions
  ) quantity, 
  NULL::numeric(38, 10) primary_quantity, 
  NVL(
    DECODE(
      osign, -1, OPERAND, 0, UNIT_PRICE, UNIT_PRICE
    ), 
    mtl_cost
  ) unit_cost, 
  primary_uom_code, 
  (
    (
      NVL(
        DECODE(
          osign, -1, OPERAND, 0, UNIT_PRICE, UNIT_PRICE
        ), 
        mtl_cost
      )
    ):: numeric(38, 10)* (
      decode(
        cvmi_count, 0, future_consumptions * allocation_percent, 
        future_consumptions
      )
    ):: numeric(38, 10)
  ) extended_cost, 
  po_line_type, 
  VENDOR_NAME, 
  mtl_cost, 
  (
    mtl_cost * (
      decode(
        cvmi_count, 0, future_consumptions * allocation_percent, 
        future_consumptions
      )
    ):: numeric(38, 10)
  ) ext_mtl_cost, 
  (
    nvl(extended_cost - ext_mtl_cost, 0)
  ) variance, 
  source_organization_id, 
  planner_code, 
  buyer_name, 
  transactional_uom_code, 
  release_num 
FROM 
  (
    SELECT 
      cost_type, 
      cvmi.plan_name, 
      'Receipt Forecast' data_type, 
      'CVMI Planned' order_group, 
      POL.unit_price, 
      sign(
        last_day(
          add_months(
            date_trunc(
              'quarter', 
              getdate()
            ), 
            2
          )
        ):: Date - as_of_date :: Date
      ) osign, 
      (
        SELECT 
          MAX(qpl.operand) 
        FROM 
          (
            Select 
              B.list_header_id, 
              B.source_system_code, 
              T.name 
            from 
              (select * from bec_ods.QP_LIST_HEADERS_TL where is_deleted_flg <> 'Y') T, 
              (select * from bec_ods.QP_LIST_HEADERS_B where is_deleted_flg <> 'Y') B 
            WHERE 
              B.LIST_HEADER_ID = T.LIST_HEADER_ID 
              and T.LANGUAGE = 'US'
          ) qph, 
          (select * from bec_ods.qp_qualifiers where is_deleted_flg <> 'Y') qq, 
          (select * from bec_ods.qp_list_lines where is_deleted_flg <> 'Y') qpl, 
          (select * from bec_ods.qp_pricing_attributes where is_deleted_flg <> 'Y') qpa, 
          (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi1, 
          (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps1 
        WHERE 
          qph.list_header_id = qpl.list_header_id 
          AND qpl.list_line_id = qpa.list_line_id 
          AND qpa.product_attr_value = msi1.inventory_item_id :: varchar 
          AND qph.list_header_id = qq.list_header_id 
          AND qq.qualifier_attr_value = aps.vendor_id 
          AND aps1.vendor_name = aps.vendor_name 
          AND msi1.organization_id = 90 
          AND qph.source_system_code = 'PO' 
          AND qph.name = decode(
            plan_name, 
            'BE-GLOBAL', 
            aps.vendor_name, 
            decode(
              (
                SELECT 
                  name 
                FROM 
                  (
                    Select 
                      T.name 
                    from 
                      (select * from bec_ods.QP_LIST_HEADERS_TL where is_deleted_flg <> 'Y') T, 
                      (select * from bec_ods.QP_LIST_HEADERS_B where is_deleted_flg <> 'Y') B 
                    WHERE 
                      B.LIST_HEADER_ID = T.LIST_HEADER_ID 
                      and T.LANGUAGE = 'US'
                  ) 
                WHERE 
                  name = aps.vendor_name || '*' || plan_name
              ), 
              NULL, 
              aps.vendor_name, 
              aps.vendor_name || '*' || plan_name
            )
          ) 
          AND msi1.segment1 = cvmi.part_number 
          AND as_of_date BETWEEN qpl.start_date_active 
          AND nvl(
            qpl.end_date_active, as_of_date + 1
          )
      ) operand, 
      NULL order_number, 
      msi.organization_id, 
      msi.inventory_item_id, 
      as_of_date new_dock_date1, 
      aging_period, 
      as_of_date dock_promised_date, 
      part_number, 
      msi.description, 
      decode(
        planning_make_buy_code, 1, 'Make', 
        2, 'Buy'
      ) planning_make_buy_code, 
      (
        SELECT 
          COUNT(
            nvl(bqoh, 0)
          ) 
        FROM 
          (select * from bec_ods.xxbec_cvmi_cons_cal_final where is_deleted_flg <> 'Y') x 
        WHERE 
          as_of_date >= cvmi.as_of_date 
          AND nvl(bqoh, 0) > 0 
          AND x.part_number = cvmi.part_number 
          AND x.organization_id = cvmi.organization_id
      ) cvmi_count, 
      future_consumptions, 
      (
        SELECT 
          MAX(sso.allocation_percent) / 100 
        FROM 
          (select * from bec_ods.mrp_sourcing_rules where is_deleted_flg <> 'Y') sr, 
          (select * from bec_ods.mrp_sr_assignments where is_deleted_flg <> 'Y') sra, 
          (select * from bec_ods.mrp_sr_receipt_org where is_deleted_flg <> 'Y') sro, 
          (select * from bec_ods.mrp_sr_source_org where is_deleted_flg <> 'Y') sso, 
          (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi1 
        WHERE 
          sso.source_organization_id IS NULL 
          AND sr.sourcing_rule_id = sro.sourcing_rule_id(+) 
          AND sr.sourcing_rule_id = sra.sourcing_rule_id 
          AND sro.sr_receipt_id = sso.sr_receipt_id(+) 
          AND sra.inventory_item_id = msi1.inventory_item_id 
          AND msi1.organization_id = 90 
          AND msi1.inventory_item_id IN(
            SELECT 
              DISTINCT paa.item_id 
            FROM 
              (select * from bec_ods.po_asl_attributes where is_deleted_flg <> 'Y') paa, 
              (select * from bec_ods.po_approved_supplier_list where is_deleted_flg <> 'Y') pasl 
            WHERE 
              consigned_from_supplier_flag = 'Y' 
              AND pasl.asl_id = paa.asl_id 
              AND nvl(pasl.disable_flag, 'N') = 'N'
          ) 
          AND msi1.inventory_item_id = msi.inventory_item_id 
          AND cvmi.as_of_date BETWEEN sro.effective_date 
          AND nvl(
            sro.disable_date, 
            getdate() + 1500
          ) 
          AND sra.organization_id = cvmi.organization_id 
          AND sso.vendor_id = aps.vendor_id
      ) allocation_percent, 
      NULL, 
      msi.primary_unit_of_measure primary_uom_code, 
      pol.line_type_id po_line_type_id, 
      plt.line_type po_line_type, 
      aps.vendor_name, 
      cic.material_cost mtl_cost, 
      NULL::Numeric(15,0) source_organization_id, 
      msi.planner_code, 
      msi.buyer_id, 
      papf.full_name buyer_name, 
      NULL transactional_uom_code, 
      NULL::Numeric(15,0) release_num 
    FROM 
      (select * from bec_ods.xxbec_cvmi_cons_cal_final where is_deleted_flg <> 'Y') cvmi, 
      (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
      (select * from bec_ods.po_asl_attributes where is_deleted_flg <> 'Y') paa, 
      (select * from bec_ods.po_approved_supplier_list where is_deleted_flg <> 'Y') pasl, 
      (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps, 
      (select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y') cic, 
      (select * from bec_ods.cst_cost_types where is_deleted_flg <> 'Y') ct, 
      (select * from bec_ods.po_asl_documents where is_deleted_flg <> 'Y') pasld, 
      (select * from bec_ods.po_lines_all where is_deleted_flg <> 'Y') pol, 
      (select * from bec_ods.po_line_types_vl where is_deleted_flg <> 'Y') plt, 
      (select * from bec_ods.per_all_people_f where is_deleted_flg <> 'Y') papf 
    WHERE 
      cvmi.organization_id = msi.organization_id 
      AND cvmi.part_number = msi.segment1 
      AND pasl.asl_id = paa.asl_id 
      AND paa.item_id = msi.inventory_item_id 
      AND paa.using_organization_id = msi.organization_id 
      AND consigned_from_supplier_flag = 'Y' 
      AND pasl.asl_id = paa.asl_id 
      AND nvl(pasl.disable_flag, 'N') = 'N' 
      AND pasl.vendor_id = aps.vendor_id 
      AND msi.organization_id = cic.organization_id 
      AND msi.inventory_item_id = cic.inventory_item_id 
      AND cic.cost_type_id = ct.cost_type_id(+) 
      AND pasl.asl_id = pasld.asl_id(+) 
      AND pasld.document_line_id = pol.po_line_id(+) 
      AND pol.line_type_id = plt.line_type_id(+) 
      AND aps.vendor_name <> 'PANWELL INTERNATIONAL CO. LTD' 
      AND msi.buyer_id = papf.person_id(+)
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
	dw_table_name = 'fact_rapa_stg1'
	and batch_name = 'ascp';

commit;