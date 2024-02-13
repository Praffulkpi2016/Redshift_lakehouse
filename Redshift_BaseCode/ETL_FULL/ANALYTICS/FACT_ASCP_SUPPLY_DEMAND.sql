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

drop table if exists bec_dwh.FACT_ASCP_SUPPLY_DEMAND;

commit;

create table bec_dwh.FACT_ASCP_SUPPLY_DEMAND 
	diststyle all
	sortkey (INVENTORY_ITEM_ID)
as
(
select 
inventory_item_id,
organization_id,
plan_id,
sr_instance_id,
category_set_id,
compile_designator,
source_table,
creation_date,
rescheduled_flag,
new_processing_days,
ship_date,
release_time_fence_code,
within_rel_time_fence,
purchasing_enabled_flag,
old_need_by_date,
buyer_name,
action,
transaction_id,
count_action,
new_due_date,
order_type,
order_type_text,
organization_code,
item_segments,
description,
category_name,
order_number,
comments,
planner_code,
supplier_name,
new_dock_date,
OLD_DOCK_DATE,
promise_date,
days_diff,
quantity_rate,
amount,
old_due_date,
reschedule_days,
list_price,
delivery_price,
new_order_date,
new_start_date,
source_organization_id,
source_vendor_site_code,
schedule_compression_days,
subinventory_code,
vmi_flag,
days_from_today,
source_vendor_name,
USING_ASSEMBLY_SEGMENTS,
INTRANSIT_LEAD_TIME,
line_number,
po_number,
po_uom,
po_amount,
release_number,
shipment_num,
po_type,
cvmi_flag,
material_cost,
extended_material_cost,
po_qty_due,
po_qty,
po_price,
open_po_price,
BUILD_IN_WIP_FLAG,
LOT_NUMBER,
REQUEST_DATE,
NEED_BY_DATE,
quantity,
--columns added to support drilldown
drill_order_number,
drill_release_num,
drill_line_num,
drill_shipment_num,
order_type_entity,
forecast_set,
forecast_designator,
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
		source_system = 'EBS')|| '-' || nvl(PLAN_ID, 0)|| '-' || nvl(TRANSACTION_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
(
with cvmi_flag_t as (
        SELECT
            MAX(asl.consigned_from_supplier_flag) cvmi_flag,
            msi.segment1 segment1,
            msi.organization_id organization_id,
            aps.vendor_name vendor_name
        FROM
            bec_ods.po_asl_attributes asl,
            bec_ods.mtl_system_items_b   msi,
            bec_ods.ap_suppliers      aps
        WHERE
                asl.item_id = msi.inventory_item_id
            AND asl.using_organization_id = msi.organization_id
            AND asl.vendor_id = aps.vendor_id
        group by
        msi.segment1 ,
            msi.organization_id,
            aps.vendor_name
    )
,cic_cost as (    SELECT
            cic.material_cost,
            cic.organization_id,
            msi.segment1
        FROM
            bec_ods.cst_item_costs   cic,
            bec_ods.mtl_system_items_b msi
        WHERE
                cic.cost_type_id = 1
            AND cic.organization_id = msi.organization_id
            AND cic.inventory_item_id = msi.inventory_item_id
    )
    ,po_quantity_details as (
        select
            poll.quantity po_qty,
            (poll.quantity - poll.quantity_cancelled - poll.quantity_received) po_qty_due,
            pol.line_num,
			pol.unit_meas_lookup_code po_uom,
            poll.shipment_num,
            poh.segment1
        FROM
            bec_ods.po_lines_all          pol,
            bec_ods.po_headers_all        poh,
            bec_ods.po_line_locations_all poll
        WHERE
                poh.po_header_id = pol.po_header_id
            AND pol.po_line_id = poll.po_line_id
    )
    ,por_quantity_details as ( select poll.quantity po_qty,
            (poll.quantity - poll.quantity_cancelled - poll.quantity_received) po_qty_due,
            por.release_num,poll.shipment_num,poh.segment1 ,pol.line_num
        FROM
            bec_ods.po_lines_all          pol,
            bec_ods.po_headers_all        poh,
            bec_ods.po_line_locations_all poll,
            bec_ods.po_releases_all       por
        WHERE
                poh.po_header_id = pol.po_header_id
            --AND pol.line_num = a.po_line_id
            AND pol.po_line_id = poll.po_line_id
            AND poll.po_release_id = por.po_release_id)
SELECT
	INVENTORY_ITEM_ID,
	mo.ORGANIZATION_ID,
	mo.PLAN_ID,
	mo.SR_INSTANCE_ID,
	category_set_id,
	mo.COMPILE_DESIGNATOR,
	source_table,
	creation_date,
    rescheduled_flag,
    new_processing_days,
    ship_date,
    release_time_fence_code,
    within_rel_time_fence,
    purchasing_enabled_flag,
    old_need_by_date,
	buyer_name,
	ACTION,
	TRANSACTION_ID,
	count(*) OVER (PARTITION BY mo.PLAN_ID,mo.SR_INSTANCE_ID,category_set_id,mo.ORGANIZATION_ID,INVENTORY_ITEM_ID,BUYER_NAME,ACTION) count_action,
	NEW_DUE_DATE,
	ORDER_TYPE,
	order_type_text,
	organization_code,
	item_segments,
	mo.description,
	category_name,
	order_number,
	comments,
	planner_code,
	SUPPLIER_NAME,
	new_dock_date,
	promise_date,
	--ABS(promise_date - new_dock_date)   DAYS_DIFF,
	datediff(day, promise_date, new_dock_date) DAYS_DIFF,
	sum(quantity_rate) as quantity_rate,
	sum(amount) as amount,
	--new columns --
	OLD_DUE_DATE,
	OLD_NEED_BY_DATE OLD_DOCK_DATE,
	reschedule_days,
    list_price,
    delivery_price,
    NEW_ORDER_DATE,
    NEW_START_DATE,
    SOURCE_ORGANIZATION_ID,
    SOURCE_VENDOR_SITE_CODE,
    SCHEDULE_COMPRESSION_DAYS,
    subinventory_code,
    vmi_flag,
    days_from_today,
    mo.source_vendor_name,
	mo.USING_ASSEMBLY_SEGMENTS,
	mo.INTRANSIT_LEAD_TIME,
	po_line_id line_number,
	po.segment1 po_number,
	po.po_uom,
    sum(quantity_rate) * delivery_price po_amount,
	por.release_num release_number,
	nvl(po.shipment_num,por.shipment_num) shipment_num,
    decode(mo.order_type_text,
           'Purchase order',
           decode(substring(mo.order_number,
                          REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
                          REGEXP_INSTR(mo.order_number, '[)]', 1, 1) - REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1),
                  ' ',
                  'STANDARD',
                  'BLANKET'),
           NULL) po_type,
    nvl(cvmi.cvmi_flag,
        'N') cvmi_flag,
    sum(cic.material_cost) material_cost,
    sum(cic.material_cost) * sum(quantity) extended_material_cost,
    sum(decode(substring(mo.order_number,
                  REGEXP_INSTR(mo.order_number, '[(]', 1, 1)+ 1,
                  1),
           ' ',nvl(po.po_qty_due,0),nvl(por.po_qty_due,0))) po_qty_due,
    sum(decode(substring(mo.order_number,
                  REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
                  1),
           ' ',nvl(po.po_qty,0),nvl(por.po_qty,0)))  po_qty,
      delivery_price * sum(decode(substring(mo.order_number,
                  REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
                  1),
           ' ',nvl(po.po_qty,0),nvl(por.po_qty,0)))  po_price,
      delivery_price *    sum(decode(substring(mo.order_number,
                  REGEXP_INSTR(mo.order_number, '[(]', 1, 1)+ 1,
                  1),
           ' ',nvl(po.po_qty_due,0),nvl(por.po_qty_due,0))) open_po_price,
	BUILD_IN_WIP_FLAG,
	LOT_NUMBER,
	REQUEST_DATE,
	NEED_BY_DATE,
	quantity ,
	--columns added to support drilldown
	decode(order_type_text
	        ,'Purchase order',(substring(mo.order_number,
                                      1,
                                      REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1))
			,'Sales Orders',(substring(mo.order_number,
                                      1,
                                      REGEXP_INSTR(mo.order_number, '[.]', 1, 1) - 1))
			,mo.order_number
		   ) drill_order_number,
	decode(order_type_text
	        ,'Purchase order',(substring(mo.order_number,REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
                               (REGEXP_INSTR(mo.order_number, '[)]', 1, 1) - 1 
                                - REGEXP_INSTR(mo.order_number, '[(]', 1, 1))))
		   ) drill_release_num,
	 decode(order_type_text
	        ,'Purchase order',mo.po_line_id::varchar
			,'Sales Orders',(substring(mo.order_number,REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
                               (REGEXP_INSTR(mo.order_number, '[.]', 1, 3)  
                                - REGEXP_INSTR(mo.order_number, '[(]', 1, 1)- 1)))
			,mo.po_line_id::varchar) drill_line_num,
	 decode(order_type_text
	        ,'Purchase order',substring(mo.order_number,
                                           REGEXP_INSTR(mo.order_number, '[(]', 1, 3) + 1,
                                           1)
			,'Sales Orders',(substring(mo.order_number,REGEXP_INSTR(mo.order_number, '[.]', 1, 3) + 1,
                               (REGEXP_INSTR(mo.order_number, '[)]', 1, 1)  
                                - REGEXP_INSTR(mo.order_number, '[.]', 1, 3)-1)))) drill_shipment_num,
	 decode(mo.order_type_text, 'Sales Orders', 'Sales orders', 'Forecast', 'Forecast',
           'Planned order demand', 'Dependent demand', 'Work order demand', 'Dependent demand', 'Non-standard job demand',
           'Dependent demand', 'Purchase requisition scrap', 'Expected scrap', 'Purchase order scrap', 'Expected scrap',
           'Planned order scrap', 'Expected scrap', 'Intransit shipment scrap', 'Expected scrap', 'Work Order scrap',
           'Expected scrap', 'Non-standard job', 'Work orders', 'Nonstandard job by-product', 'Work orders',
           'Work order co-product/by-product', 'Work orders', 'Work order', 'Work orders', 'Purchase order',
           'Purchase orders', 'Purchase requisition', 'Requisitions/ CVMI Consumtion Plan', 'Intransit shipment', 'In Transit',
           'PO in receiving', 'In Receiving', 'Intransit receipt', 'In Receiving', 'Planned order',
           'Planned orders', 'Planned order co-product/by-product', 'Planned orders', 'On Hand', 'Beginning on hand') order_type_entity,
	 decode(mo.order_type_text,'Forecast',(substring(mo.order_number,1,
                       REGEXP_INSTR(mo.order_number, '[/]', 1, 1) - 1))) forecast_set,
     decode(mo.order_type_text,'Forecast',(substring(mo.order_number,
                       REGEXP_INSTR(mo.order_number, '[/]', 1, 1)+1))) forecast_designator	                         
FROM bec_ods.msc_orders_v mo,
cvmi_flag_t cvmi,
cic_cost cic,
po_quantity_details po,
por_quantity_details por,
bec_dwh.DIM_ASCP_PLANS plans
where
category_set_id = 9
AND cvmi.segment1(+) = mo.item_segments
AND cvmi.organization_id(+) = mo.organization_id
AND cvmi.vendor_name(+) = mo.source_vendor_name
and cic.organization_id(+) = mo.organization_id
AND cic.segment1(+) = mo.item_segments
and po.line_num(+) = mo.po_line_id
AND po.shipment_num(+)::varchar = substring(mo.order_number,
                                           REGEXP_INSTR(mo.order_number, '[(]', 1, 3) + 1,
                                           1)
AND po.segment1(+)::varchar = case when (REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1)<0 then null else (substring(mo.order_number,
                                      1,
                                      REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1)) end
AND por.line_num(+) = mo.po_line_id
AND por.release_num(+)::varchar = case when ((REGEXP_INSTR(mo.order_number, '[)]', 1, 1) - 1 
                                         - REGEXP_INSTR(mo.order_number, '[(]', 1, 1)))<0 then null else (substring(mo.order_number,
                                         REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
                                         (REGEXP_INSTR(mo.order_number, '[)]', 1, 1) - 1 
                                         - REGEXP_INSTR(mo.order_number, '[(]', 1, 1)))) end
AND por.shipment_num(+)::varchar = substring(mo.order_number,
                                           REGEXP_INSTR(mo.order_number, '[(]', 1, 3) + 1,
                                           1)
AND por.segment1(+)::varchar = case when (REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1)<0 then null else (substring(mo.order_number,
                                      1,
                                      REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1)) end
and mo.plan_id = plans.plan_id
and mo.sr_instance_id = plans.sr_instance_id
and plans.LOAD_FLG = 'Y'								  
group by 
INVENTORY_ITEM_ID,
mo.ORGANIZATION_ID,
mo.PLAN_ID,
mo.SR_INSTANCE_ID,
category_set_id,
mo.COMPILE_DESIGNATOR,
buyer_name,
ACTION,
transaction_id,
NEW_DUE_DATE,
ORDER_TYPE,
order_type_text,
organization_code,
item_segments,
mo.description,
category_name,
order_number,
comments,
planner_code,
SUPPLIER_NAME,
new_dock_date,
promise_date,
new_dock_date - promise_date,
OLD_DUE_DATE,
reschedule_days,
new_processing_days,
list_price,
delivery_price,
NEW_ORDER_DATE,
NEW_START_DATE,
OLD_NEED_BY_DATE,
SOURCE_ORGANIZATION_ID,
SOURCE_VENDOR_SITE_CODE,
SCHEDULE_COMPRESSION_DAYS,
subinventory_code,
vmi_flag,
days_from_today,
mo.source_vendor_name,
cvmi.cvmi_flag,
mo.USING_ASSEMBLY_SEGMENTS,
mo.INTRANSIT_LEAD_TIME,
po_line_id ,
por.release_num,
po.segment1,
po.po_uom,
nvl(po.shipment_num,por.shipment_num),
source_table,
creation_date,
rescheduled_flag,
ship_date,
release_time_fence_code,
within_rel_time_fence,
purchasing_enabled_flag,
old_need_by_date,
BUILD_IN_WIP_FLAG,
LOT_NUMBER,
REQUEST_DATE,
NEED_BY_DATE,
quantity
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
	dw_table_name = 'fact_ascp_supply_demand'
	and batch_name = 'ascp';

commit;