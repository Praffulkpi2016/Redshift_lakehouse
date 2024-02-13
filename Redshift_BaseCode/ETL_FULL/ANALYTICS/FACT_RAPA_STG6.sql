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

drop table if exists bec_dwh.FACT_RAPA_STG6;

create table bec_dwh.FACT_RAPA_STG6 
	diststyle all
	sortkey (promised_date1)
as
(
select
	INVENTORY_ITEM_ID, 
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
	PART_NUMBER, 
	description, 
	planning_make_buy_code, 
	NULL as category_name,
	po_open_quantity, 
	primary_quantity, 
	price_override, 
	primary_unit_of_measure, 
	extended_cost, 
	po_line_type, 
	vendor_name, 
	mtl_cost, 
	ext_mtl_cost, 
	variance, 
	source_organization_id, 
	planner_code, 
	buyer_name, 
	unit_meas_lookup_code, 
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
	|| '-' || nvl(promised_date1, '1900-01-01 12:00:00')|| '-' || nvl(po_open_quantity, 0)|| '-' || nvl(price_override, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(SELECT 
  INVENTORY_ITEM_ID, 
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
  PART_NUMBER, 
  description, 
  planning_make_buy_code, 
  po_open_quantity, 
  ( nvl(NV1, nvl(NV2, 1)) / nvl(NV3, nvl(NV4, 1)) ) conversion_percent,
  quantity * conversion_percent - quantity_received * conversion_percent - quantity_cancelled * conversion_percent primary_quantity, 
  price_override, 
  primary_unit_of_measure, 
  extended_cost, 
  po_line_type, 
  vendor_name, 
  mtl_cost, 
  mtl_cost *(
    quantity * conversion_percent - quantity_received * conversion_percent - quantity_cancelled * conversion_percent
  ) ext_mtl_cost, 
  extended_cost - (
    mtl_cost *(
      quantity * conversion_percent - quantity_received * conversion_percent - quantity_cancelled * conversion_percent
    )
  ) variance, 
  source_organization_id, 
  planner_code, 
  buyer_name, 
  unit_meas_lookup_code, 
  release_num 
FROM 
  (
    SELECT 
      msi.inventory_item_id, 
      poll.ship_to_organization_id AS organization_id, 
      ct.cost_type, 
      NULL plan_name, 
      'Receipt Forecast' data_type, 
      'CVMI Planned' order_group, 
      'CVMI Shipment PO' order_type_text, 
      poh.segment1 po_number, 
      trunc(promised_date) promised_date1, 
      pasl.aging_period, 
      decode(
        sign(
          datediff(
            days, 
            sysdate, 
            nvl(poll.promised_date, sysdate - 1)
          )
        ), 
        -1, 
        sysdate, 
        trunc(
          nvl(poll.promised_date, sysdate - 1)
        )
      ) promised_date, 
      msi.segment1 PART_NUMBER, 
      msi.description, 
      decode(
        msi.planning_make_buy_code, 1, 'Make', 
        2, 'Buy', NULL
      ) planning_make_buy_code, 
      (
        poll.quantity - poll.quantity_received - quantity_cancelled
      ) po_open_quantity, 
      poll.quantity, 
      poll.quantity_received, 
      poll.QUANTITY_CANCELLED, 
      (
            SELECT
                conversion_rate
            FROM
                (select * from bec_ods.mtl_uom_conversions where is_deleted_flg <> 'Y') muc
            WHERE
                    muc.unit_of_measure = pol.unit_meas_lookup_code
                AND muc.inventory_item_id = msi.inventory_item_id
        ) NV1,
        (
            SELECT
                conversion_rate
            FROM
                (select * from bec_ods.mtl_uom_conversions where is_deleted_flg <> 'Y')
            WHERE
                    unit_of_measure = pol.unit_meas_lookup_code
                AND inventory_item_id = 0
        ) NV2,
        (
            SELECT
                conversion_rate
            FROM
                (select * from bec_ods.mtl_uom_conversions where is_deleted_flg <> 'Y') muc
            WHERE
                    muc.unit_of_measure = msi.primary_unit_of_measure
                AND muc.inventory_item_id = msi.inventory_item_id
        ) NV3,
        (
            SELECT
                conversion_rate
            FROM
                (select * from bec_ods.mtl_uom_conversions where is_deleted_flg <> 'Y')
            WHERE
                    unit_of_measure = msi.primary_unit_of_measure
                AND inventory_item_id = 0
        ) NV4,
      price_override, 
      msi.primary_unit_of_measure, 
      price_override * (
        poll.quantity - poll.quantity_received - poll.QUANTITY_CANCELLED
      ) extended_cost, 
      NULL po_line_type, 
      aps.vendor_name, 
      cic.material_cost mtl_cost, 
      NULL::Numeric(15,0) source_organization_id, 
      msi.planner_code, 
      papf.full_name buyer_name, 
      pol.unit_meas_lookup_code, 
      por.release_num 
    FROM 
      (select * from bec_ods.po_line_locations_all where is_deleted_flg <> 'Y') poll, 
      (select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh, 
      (select * from bec_ods.po_lines_all where is_deleted_flg <> 'Y') pol, 
      (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
      (select * from bec_ods.ap_suppliers where is_deleted_flg <> 'Y') aps, 
      (select * from bec_ods.po_asl_attributes where is_deleted_flg <> 'Y') pasl, 
      (select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y') cic, 
      (select * from bec_ods.cst_cost_types where is_deleted_flg <> 'Y') ct, 
      (select * from bec_ods.po_releases_all where is_deleted_flg <> 'Y') por, 
      (select * from bec_ods.per_all_people_f where is_deleted_flg <> 'Y') papf 
    WHERE 
      poll.po_header_id = poh.po_header_id 
      AND poll.po_line_id = pol.po_line_id 
      AND pol.item_id = msi.inventory_item_id 
      AND poh.vendor_id = aps.vendor_id 
      AND poll.ship_to_organization_id = msi.organization_id
      AND poll.consigned_flag = 'Y' 
      AND nvl(poll.closed_code, 'OPEN') <> 'CLOSED'
      AND (
        poll.quantity - poll.quantity_received - poll.QUANTITY_CANCELLED
      ) > 0 
      and pasl.aging_period IS NOT NULL 
      AND pasl.using_organization_id = poll.ship_to_organization_id 
      AND pasl.vendor_id = poh.vendor_id 
      AND pasl.vendor_site_id = poh.vendor_site_id 
      AND pasl.item_id = pol.item_id 
      and poll.po_release_id = por.po_release_id(+) 
      AND cic.organization_id = poll.ship_to_organization_id 
      AND cic.inventory_item_id = pol.item_id 
      AND cic.cost_type_id = ct.cost_type_id 
      and ct.cost_type_id in (1, 3) 
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
	dw_table_name = 'fact_rapa_stg6'
	and batch_name = 'ascp';

commit;