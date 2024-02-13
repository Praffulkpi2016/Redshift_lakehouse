/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;
drop 
  table if exists bec_dwh.FACT_OM_SHIP_BACKLOG_STG;
commit;
create table bec_dwh.FACT_OM_SHIP_BACKLOG_STG diststyle all sortkey (actual_shipped_date) as (
  select 
    bill_to_customer, 
    ship_to_customer, 
    order_number, 
    line_number, 
    item, 
    item_description, 
    ordered_quantity, 
    shipped_quantity, 
    backlog_quantity, 
    actual_shipped_date, 
    schedule_ship_date, 
    pick_list, 
    delivery, 
    waybill, 
    serial_number, 
    order_amnt, 
    shipment_amnt, 
    to_go_shipments, 
    support_reference, 
    organization_id, 
    component_number, 
    site_address, 
    cust_po_number, 
    shipment_number, 
    header_id, 
    line_id, 
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id organization_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || header_id header_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || line_id line_id_KEY,
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
    )|| '-' || nvl(item, 'NA')|| '-' || nvl(shipment_number, 0)|| '-' || nvl(organization_id, 0)|| '-' || nvl(order_number, 'NA')|| '-' || nvl(line_number, 0)|| '-' || nvl(serial_number, 'NA')|| '-' || nvl(delivery, 'NA')
	as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  FROM 
    (
      WITH oe_lines as (
        select 
          oeh.header_id, 
          oeh.org_id, 
          oeh.cust_po_number, 
          oeh.order_number order_number, 
          ola.line_id, 
          ola.shipment_number, 
          ola.inventory_item_id, 
          ola.component_number, 
          ola.ordered_quantity, 
          ola.item_type_code, 
          ola.ship_to_org_id, 
          ola.invoice_to_org_id, 
          ola.line_number, 
          ola.fulfilled_quantity, 
          ola.fulfillment_date, 
          ola.actual_shipment_date, 
          schedule_ship_date, 
          ola.unit_selling_price 
        from 
          (select * from bec_ods.oe_order_headers_all where is_deleted_flg <> 'Y') oeh, 
          (select * from bec_ods.oe_order_lines_all where is_deleted_flg <> 'Y') ola 
        where 
          oeh.header_id = ola.header_id 
          AND ola.item_type_code IN ('INCLUDED', 'OPTION', 'MODEL') 
          and oeh.org_id = decode(
            ola.item_type_code, 'MODEL', 85, oeh.org_id
          )
      ), 
      bec_cst_details as (
        SELECT 
          site_use_id, 
          party_name, 
          address1, 
          address2, 
          address3, 
          address4, 
          address5, 
          address1 || ' ' || address2 || ' ' || address3 || ' ' || address4 || ' ' || address5 site_address 
        FROM 
          bec_ods.bec_customer_details_view 
		where is_deleted_flg <> 'Y'
      ), 
      wsh_pick as (
        SELECT 
          pick_slip_number, 
          move_order_line_id 
        FROM 
          bec_ods.mtl_material_transactions_temp 
		where is_deleted_flg <> 'Y' 
        AND pick_slip_number IS NOT NULL 
          AND abs(
            nvl(transaction_quantity, 0)
          ) > 0 
        UNION ALL 
        SELECT 
          pick_slip_number, 
          move_order_line_id 
        FROM 
          bec_ods.mtl_material_transactions 
		where is_deleted_flg <> 'Y'
		AND pick_slip_number IS NOT NULL 
        AND nvl(transaction_quantity, 0) < 0 
        UNION ALL 
        SELECT 
          pick_slip_number, 
          line_id move_order_line_id 
        FROM 
          bec_ods.mtl_txn_request_lines 
		where is_deleted_flg <> 'Y'
        AND pick_slip_number IS NOT NULL
      ), 
      wsh_pick_slip as (
        select 
          pick_slip_number, 
          move_order_line_id, 
          row_number() over(
            order by 
              move_order_line_id
          ) rn 
        from 
          wsh_pick
      ) 
      select 
        bcd.party_name bill_to_customer, 
        ship.party_name ship_to_customer, 
        ola.order_number :: varchar order_number, 
        ola.line_number, 
        msi.segment1 item, 
        msi.description item_description, 
        ola.ordered_quantity, 
        0 shipped_quantity, 
        ola.ordered_quantity backlog_quantity, 
        ola.actual_shipment_date actual_shipped_date, 
        ola.schedule_ship_date AS schedule_ship_date, 
        NULL AS pick_list, 
        NULL delivery, 
        NULL waybill, 
        NULL serial_number, 
        NULL order_amnt, 
        NULL shipment_amnt, 
        NULL to_go_shipments, 
        NULL support_reference, 
        msi.organization_id organization_id, 
        ola.component_number, 
        ship.site_address, 
        ola.cust_po_number, 
        ola.shipment_number, 
        ola.header_id, 
        ola.line_id 
      from 
        oe_lines ola, 
        bec_cst_details bcd, 
        bec_cst_details ship, 
        (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi 
      where 
        bcd.site_use_id (+) = ola.invoice_to_org_id 
        AND ship.site_use_id (+) = ola.ship_to_org_id 
        AND ola.inventory_item_id = msi.inventory_item_id 
        AND ola.item_type_code IN ('INCLUDED', 'OPTION') 
        AND ola.ordered_quantity > 0 
        AND ola.actual_shipment_date IS NULL 
      UNION 
      select 
        bcd.party_name bill_to_customer, 
        ship.party_name ship_to_customer, 
        ola.order_number :: varchar, 
        ola.line_number, 
        msi.segment1 item, 
        msi.description item_description, 
        ola.ordered_quantity, 
        nvl(ola.fulfilled_quantity, 0) shipped_quantity, 
        nvl(
          (
            ola.ordered_quantity - nvl(ola.fulfilled_quantity, 0)
          ), 
          0
        ) backlog_quantity, 
        ola.fulfillment_date actual_shipped_date, 
        schedule_ship_date, 
        NULL pick_list, 
        NULL delivery, 
        NULL waybill, 
        NULL serial_number, 
        ola.unit_selling_price order_amnt, 
        nvl(
          (
            ola.unit_selling_price * ola.fulfilled_quantity
          ), 
          0
        ) shipment_amnt, 
        nvl(
          (
            ola.unit_selling_price - nvl(
              (
                ola.unit_selling_price * ola.fulfilled_quantity
              ), 
              0
            )
          ), 
          0
        ) to_go_shipments, 
        NULL support_reference, 
        msi.organization_id organization_id, 
        0 AS component_number, 
        ship.site_address, 
        ola.cust_po_number, 
        ola.shipment_number, 
        ola.header_id, 
        ola.line_id 
      from 
        oe_lines ola, 
        bec_cst_details bcd, 
        bec_cst_details ship, 
        (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi 
      where 
        ship.site_use_id (+) = ola.ship_to_org_id 
        AND bcd.site_use_id (+) = ola.invoice_to_org_id 
        AND ola.inventory_item_id = msi.inventory_item_id 
        AND ola.ordered_quantity > 0 
        AND ola.item_type_code = 'MODEL' 
      UNION 
      select 
        bcd.party_name bill_to_customer, 
        ship.party_name ship_to_customer, 
        wdd.source_header_number :: varchar order_number, 
        ola.line_number, 
        msi.segment1 item, 
        wdd.item_description item_description, 
        1 ordered_quantity, 
        1 shipped_quantity, 
        0 backlog_quantity, 
        ola.actual_shipment_date actual_shipped_date, 
        wdd.date_scheduled AS schedule_ship_date, 
        (
          SELECT 
            pick_slip_number 
          FROM 
            wsh_pick_slip pick 
          WHERE 
            pick.move_order_line_id = wdd.move_order_line_id 
            AND rn < 2
        ) pick_list, 
        wnd.name delivery, 
        waybill, 
        wsn.fm_serial_number serial_number, 
        NULL order_amnt, 
        NULL shipment_amnt, 
        NULL to_go_shipments, 
        NULL support_reference, 
        wnd.organization_id, 
        ola.component_number, 
        ship.site_address, 
        ola.cust_po_number, 
        ola.shipment_number, 
        ola.header_id, 
        ola.line_id 
      from 
        oe_lines ola, 
        bec_cst_details bcd, 
        bec_cst_details ship, 
        (select * from bec_ods.wsh_serial_numbers where is_deleted_flg <> 'Y') wsn, 
        (select * from bec_ods.wsh_new_deliveries where is_deleted_flg <> 'Y') wnd, 
        (select * from bec_ods.wsh_delivery_details where is_deleted_flg <> 'Y') wdd, 
        (select * from bec_ods.wsh_delivery_assignments where is_deleted_flg <> 'Y') wda, 
        (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi 
      where 
        wda.delivery_detail_id = wdd.delivery_detail_id 
        AND nvl(wda.type, 'S') IN ('S', 'C') 
        AND nvl(wdd.line_direction, 'O') IN ('O', 'IO') 
        AND wnd.delivery_id = wda.delivery_id (+) 
        AND wdd.source_line_id = ola.line_id 
        AND wdd.source_header_id = ola.header_id 
        AND wdd.delivery_detail_id = wsn.delivery_detail_id (+) 
        AND wdd.inventory_item_id = msi.inventory_item_id 
        AND wdd.organization_id = msi.organization_id 
        AND ship.site_use_id (+) = ola.ship_to_org_id 
        AND ola.invoice_to_org_id = bcd.site_use_id (+) 
        AND ola.item_type_code IN ('INCLUDED', 'OPTION') 
        AND ola.ordered_quantity > 0
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
  dw_table_name = 'fact_om_ship_backlog_stg' 
  and batch_name = 'om';
commit;