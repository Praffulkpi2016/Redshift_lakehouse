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
Truncate table bec_dwh.FACT_OM_SHIP_BACKLOG;
commit;
Insert Into bec_dwh.FACT_OM_SHIP_BACKLOG
(
  with ship as (
    SELECT 
      DISTINCT o.order_number order_ship, 
      trunc(
        nvl(
          l.actual_shipment_date, l.fulfillment_date
        )
      ) ship_filter 
    FROM 
      (select * from bec_ods.oe_order_lines_all where is_deleted_flg <> 'Y') l, 
      (select * from bec_ods.oe_order_headers_all where is_deleted_flg <> 'Y') o 
    WHERE 
      o.header_id = l.header_id 
      AND l.item_type_code IN ('MODEL', 'INCLUDED', 'OPTION')
  ), 
  rma as (
    SELECT 
      order_number rma_number, 
      reference_header_id, 
      reference_line_id, 
      return_attribute2 
    FROM 
      (select * from bec_ods.oe_order_lines_all where is_deleted_flg <> 'Y') rma_oel, 
      (select * from bec_ods.oe_order_headers_all where is_deleted_flg <> 'Y') rma_oeh 
    WHERE 
      rma_oel.header_id = rma_oeh.header_id 
      AND rma_oeh.order_type_id IN (
        SELECT 
          transaction_type_id 
        FROM 
          (select * from bec_ods.oe_transaction_types_tl where is_deleted_flg <> 'Y') 
        WHERE 
          name = 'Customer Service Swap'
      )
  ) 
  select 
    ship.ship_filter, 
    'Q1' filter1, 
    a.BILL_TO_CUSTOMER, 
    a.SHIP_TO_CUSTOMER, 
    a.ORDER_NUMBER, 
    a.LINE_NUMBER, 
    a.ITEM, 
    a.ITEM_DESCRIPTION, 
    a.ORDERED_QUANTITY, 
    a.SHIPPED_QUANTITY, 
    a.BACKLOG_QUANTITY, 
    a.ACTUAL_SHIPPED_DATE, 
    a.SCHEDULE_SHIP_DATE, 
    a.PICK_LIST, 
    a.DELIVERY, 
    a.WAYBILL, 
    a.SERIAL_NUMBER, 
    a.ORDER_AMNT, 
    a.SHIPMENT_AMNT, 
    a.TO_GO_SHIPMENTS, 
    a.SUPPORT_REFERENCE, 
    a.ORGANIZATION_ID, 
    a.COMPONENT_NUMBER, 
    a.SITE_ADDRESS, 
    a.cust_po_number, 
    a.shipment_number, 
    rma.rma_number, 
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id organization_id_KEY, 
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
    )|| '-' || nvl(a.dw_load_id, 'NA') || '-' || nvl(ship.ship_filter, '1990-01-01 12:00:00')|| '-' || nvl(rma.rma_number, 0)
    as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  from 
    bec_dwh.FACT_OM_SHIP_BACKLOG_STG a, 
    ship, 
    rma 
  where 
    ship.order_ship = a.order_number 
    AND rma.reference_header_id (+) = a.header_id 
    AND rma.reference_line_id (+) = a.line_id 
    AND rma.return_attribute2 (+) = a.serial_number
);
commit;
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_om_ship_backlog' 
  and batch_name = 'om';
commit;
