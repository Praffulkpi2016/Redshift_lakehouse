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

drop table if exists bec_dwh.FACT_INV_ONHAND;

create table bec_dwh.FACT_INV_ONHAND distkey(TRANSACTION_ID) sortkey(TRANSACTION_ID,inventory_item_id,organization_id)
as
SELECT  DISTINCT 
TRANSACTION_ID,
inventory_item_id,
organization_id,
cost_type_id,
locator_id,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||TRANSACTION_ID   TRANSACTION_ID_KEY,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||inventory_item_id   INVENTORY_ITEM_ID_KEY,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||organization_id   ORGANIZATION_ID_KEY,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cost_type_id   COST_TYPE_ID_KEY,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||locator_id   LOCATOR_ID_KEY,
part_number,
part_desc,
primary_unit_of_measure,
primary_uom_code,
inventory_item_status_code,
PLANNING_MAKE_BUY_CODE,
MRP_PLANNING_CODE,
material_cost, 
material_overhead_cost, 
resource_cost,
outside_processing_cost, 
overhead_cost, 
item_cost,
subinventory,
serial_number, 
lot_number,
QUANTITY,
EXT_MATERIAL_COST,
EXT_MATERIAL_OVERHEAD_COST,
EXT_RESOURCE_COST,
EXT_OUTSIDE_PROCESSING_COST,
EXT_OVERHEAD_COST,
EXTENDED_COST, 
transaction_date ,
TRANSACTION_TYPE, 
ATTRIBUTE1,
attribute3, 
attribute4, 
rcv_attr4,
rcv_attr5, 
rcv_attr6,
transaction_source_type_id, 
transaction_source_type_name,
transaction_source_id,			 
source_line_id, 
trx_source_line_id,
organization_name, 
revision, 
move_order_line_id,
subinventory_type,
vmi_flag,
onhand_by_subinventory,
(select SUM (qty.primary_transaction_quantity) from bec_ods.mtl_onhand_quantities_detail qty where is_deleted_flg<>'Y'
and qty.inventory_item_id=inv_onhand.inventory_item_id and qty.organization_id=inv_onhand.organization_id) as org_onhand_qty,
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
    )
    || '-' || nvl(TRANSACTION_ID,0)
	|| '-' || nvl(inventory_item_id,0)
	|| '-' || nvl(organization_id,0)
	|| '-' || nvl(serial_number,'NA')
	|| '-' || nvl(lot_number,'NA') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
(
SELECT   
	         miq.create_transaction_id TRANSACTION_ID,
   			 miq.inventory_item_id,
			 miq.organization_id,
			 ctc.cost_type_id,
			 miq.locator_id,
			 msi.segment1 AS part_number,
             msi.description part_desc,
             msi.primary_unit_of_measure,
			 msi.primary_uom_code,
             msi.inventory_item_status_code,
             DECODE (msi.planning_make_buy_code, 1, 'Make', 2, 'Buy') PLANNING_MAKE_BUY_CODE,
--           mc.segment1 CAT_SEG1, 
--			 mc.segment2 CAT_SEG2, 
--			 mc.segment1 || '.' || mc.segment2 CATEGORY,
             (SELECT mfl.meaning
                FROM (select * from bec_ods.fnd_lookup_values where is_deleted_flg<>'Y') mfl
               WHERE mfl.lookup_type = 'MRP_PLANNING_CODE'
                 AND mfl.lookup_code = msi.mrp_planning_code) MRP_PLANNING_CODE,
             ctc.material_cost, 
			 ctc.material_overhead_cost, 
			 ctc.resource_cost,
             ctc.outside_processing_cost, 
			 ctc.overhead_cost, 
			 ctc.item_cost,
             miq.subinventory_code AS subinventory,
             --mit.segment1|| '.' || mit.segment2  || '.' || mit.segment3 AS LOCATOR,
             NULL serial_number, 
			 miq.lot_number,
             SUM (miq.primary_transaction_quantity) QUANTITY,
             SUM (miq.primary_transaction_quantity) * ctc.material_cost EXT_MATERIAL_COST,
             SUM (miq.primary_transaction_quantity) * ctc.material_overhead_cost EXT_MATERIAL_OVERHEAD_COST,
             SUM (miq.primary_transaction_quantity) * ctc.resource_cost EXT_RESOURCE_COST,
             SUM (miq.primary_transaction_quantity) * ctc.outside_processing_cost EXT_OUTSIDE_PROCESSING_COST,
             SUM (miq.primary_transaction_quantity) * ctc.overhead_cost EXT_OVERHEAD_COST,
             SUM (miq.primary_transaction_quantity) * ctc.item_cost EXTENDED_COST, 
			 mmt.transaction_date ,
             mtt.transaction_type_name TRANSACTION_TYPE, 
			 mmt.attribute1 ATTRIBUTE1,
             mmt.attribute3 attribute3, 
			 mmt.attribute4 attribute4, 
			 rct.attribute4 rcv_attr4,
             rct.attribute5 rcv_attr5, 
			 rct.attribute6 rcv_attr6,
             mmt.transaction_source_type_id, 
			 mts.transaction_source_type_name,
			DECODE
                  (mts.transaction_source_type_name,
                   'Job or Schedule', (SELECT wip_entity_name
                                         FROM (select * from bec_ods.wip_entities where is_deleted_flg<>'Y')wip_entities
                                        WHERE wip_entity_id =
                                                     mmt.transaction_source_id),
                	'RMA', cast((SELECT order_number
                             FROM (select * from bec_ods.oe_order_lines_all where is_deleted_flg<>'Y') ola,
                                  (select * from bec_ods.oe_order_headers_all where is_deleted_flg<>'Y') oha
                            WHERE ola.line_id = mmt.trx_source_line_id
                              AND ola.header_id = oha.header_id) as varchar(50)),
                  cast (mmt.transaction_source_id as varchar(50))
                  ) transaction_source_id,			 
             mmt.source_line_id, 
			 mmt.trx_source_line_id,
             ood.organization_name, 
			 miq.revision, 
			 mmt.move_order_line_id,
             DECODE (sub.asset_inventory,
                     1, 'Asset',
                     2, 'Expense',
                     'asset_inventory'
                    ) subinventory_type,
             DECODE (miq.owning_organization_id,
                     miq.organization_id, 'N',
                     'Y'
                    ) vmi_flag,
			(select SUM (primary_transaction_quantity)
			from (select * from bec_ods.mtl_onhand_quantities_detail where is_deleted_flg<>'Y') mq
			where miq.inventory_item_id = mq.inventory_item_id
         AND miq.organization_id = mq.organization_id
and  miq.subinventory_code = mq.subinventory_code		 
			group by inventory_item_id,organization_id,subinventory_code) as onhand_by_subinventory
        FROM (select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') msi,
             --bec_ods.mtl_item_locations mit,
             (select * from bec_ods.mtl_onhand_quantities_detail where is_deleted_flg<>'Y') miq,
             (select * from bec_ods.mtl_material_transactions where is_deleted_flg<>'Y') mmt,
             (select * from bec_ods.mtl_transaction_types where is_deleted_flg<>'Y') mtt,
             (select * from bec_ods.cst_item_costs where is_deleted_flg<>'Y') ctc,
             --bec_ods.mtl_item_categories mic,
             --bec_ods.mtl_categories mc,
             (select * from bec_ods.rcv_transactions where is_deleted_flg<>'Y') rct,
             (select * from bec_ods.mtl_txn_source_types where is_deleted_flg<>'Y') mts,
             (select * from bec_ods.org_organization_definitions where is_deleted_flg<>'Y') ood,
             (select * from bec_ods.mtl_secondary_inventories where is_deleted_flg<>'Y') sub
       WHERE msi.inventory_item_id = miq.inventory_item_id
         AND miq.organization_id = msi.organization_id
         --   AND miq.organization_id = miq.owning_organization_id
         AND miq.create_transaction_id = mmt.transaction_id(+)
         AND mmt.transaction_source_type_id = mts.transaction_source_type_id(+)
         --AND miq.locator_id = mit.inventory_location_id(+)
         AND msi.serial_number_control_code = 1
         AND mmt.transaction_type_id = mtt.transaction_type_id(+)
         AND mmt.rcv_transaction_id = rct.transaction_id(+)
         AND miq.organization_id = ctc.organization_id(+)
         AND miq.inventory_item_id = ctc.inventory_item_id(+)
         --AND msi.inventory_item_id = mic.inventory_item_id(+)
         --AND msi.organization_id = mic.organization_id(+)
         --AND mic.category_id = mc.category_id(+)
         --AND mic.category_set_id(+) = 1
         AND ctc.cost_type_id(+) = '1'
         AND msi.organization_id = ood.organization_id
         AND miq.subinventory_code = sub.secondary_inventory_name(+)
         AND miq.organization_id = sub.organization_id(+)		 
    GROUP BY miq.inventory_item_id,
			 miq.organization_id,
			 ctc.cost_type_id,
			 miq.locator_id,
			 msi.segment1,
             msi.description,
             msi.primary_unit_of_measure,
             msi.primary_uom_code,
             miq.subinventory_code,
             miq.lot_number,
             miq.create_transaction_id,
             mmt.transaction_date,
             mtt.transaction_type_name,
             msi.organization_id,
             mmt.attribute1,
             mmt.attribute3,
             mmt.attribute4,
             rct.attribute4,
             rct.attribute5,
             rct.attribute6,
             msi.inventory_item_status_code,
             msi.planning_make_buy_code,
             --mc.segment1,
             --mc.segment2,
             msi.mrp_planning_code,
             ctc.material_cost,
             ctc.material_overhead_cost,
             ctc.resource_cost,
             ctc.outside_processing_cost,
             ctc.overhead_cost,
             ctc.item_cost,
             mmt.transaction_source_type_id,
             mts.transaction_source_type_name,
             mmt.transaction_source_id,
             mmt.source_line_id,
             mmt.trx_source_line_id,
             ood.organization_name,
             miq.revision,
             mmt.move_order_line_id,
             DECODE (sub.asset_inventory,
                     1, 'Asset',
                     2, 'Expense',
                     'asset_inventory'),
             DECODE (miq.owning_organization_id,
                     miq.organization_id, 'N', 'Y')
    UNION ALL
    SELECT DISTINCT 
	                msn.last_transaction_id TRANSACTION_ID,
					msi.inventory_item_id,
			        msi.organization_id,
					ctc.cost_type_id,
					msn.current_locator_id locator_id,
			        msi.segment1 AS part_number, 
	                msi.description part_desc,
                    msi.primary_unit_of_measure, 
					msi.primary_uom_code,
                    msi.inventory_item_status_code,
                    DECODE (msi.planning_make_buy_code, 1, 'Make', 2, 'Buy') PLANNING_MAKE_BUY_CODE,
                    --mc.segment1 CAT_SEG1, 
					--mc.segment2 CAT_SEG2,
                    --mc.segment1 || '.' || mc.segment2 CATEGORY,
                    (SELECT mfl.meaning
                       FROM (select * from bec_ods.fnd_lookup_values where is_deleted_flg<>'Y')  mfl
                      WHERE mfl.lookup_type = 'MRP_PLANNING_CODE'
                        AND mfl.lookup_code = msi.mrp_planning_code) MRP_PLANNING_CODE,
                    ctc.material_cost, 
					ctc.material_overhead_cost,
                    ctc.resource_cost, 
					ctc.outside_processing_cost,
                    ctc.overhead_cost, 
					ctc.item_cost,
                    msn.current_subinventory_code AS subinventory,
                    --(mil.segment1 || '.' || mil.segment2 || '.' || mil.segment3 ) LOCATOR,
                    msn.serial_number, 
					msn.lot_number, 
					1 quantity,
                    ctc.material_cost EXT_MATERIAL_COST, 
					ctc.material_overhead_cost EXT_MATERIAL_OVERHEAD_COST,
                    ctc.resource_cost EXT_RESOURCE_COST, 
					ctc.outside_processing_cost EXT_OUTSIDE_PROCESSING_COST,
                    ctc.overhead_cost EXT_OVERHEAD_COST, 
					ctc.item_cost EXTENDED_COST, 
                    mut.transaction_date, 
					mtt.transaction_type_name TRANSACTION_TYPE,
					mmt.attribute1, 
					mmt.attribute3,
                    mmt.attribute4, 
					rct.attribute4 rcv_attr4, 
					rct.attribute5 rcv_attr5,
                    rct.attribute6 rcv_attr6, 
					mmt.transaction_source_type_id,
                    mts.transaction_source_type_name,
				DECODE
                  (mts.transaction_source_type_name,
                   'Job or Schedule', (SELECT wip_entity_name
                                         FROM (select * from bec_ods.wip_entities where is_deleted_flg<>'Y')wip_entities
                                        WHERE wip_entity_id =
                                                     mmt.transaction_source_id),
                	'RMA', cast((SELECT order_number
                             FROM (select * from bec_ods.oe_order_lines_all where is_deleted_flg<>'Y') ola,
                                  (select * from bec_ods.oe_order_headers_all where is_deleted_flg<>'Y') oha
                            WHERE ola.line_id = mmt.trx_source_line_id
                              AND ola.header_id = oha.header_id) as varchar(50)),
                  cast (mmt.transaction_source_id as varchar(50))
                  ) transaction_source_id,			 
                    mmt.source_line_id, 
					mmt.trx_source_line_id,
                    ood.organization_name, 
					mmt.revision,
                    mmt.move_order_line_id,
                    DECODE (sub.asset_inventory,
                            1, 'Asset',
                            2, 'Expense',
                            'asset_inventory'
                           ) subinventory_type,
                    DECODE (msn.owning_organization_id,
                            msn.current_organization_id, 'N',
                            'Y'
                           ) vmi_flag,
						  0 as onhand_by_subinventory						   
               FROM (select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') msi,
                    (select * from bec_ods.mtl_serial_numbers where is_deleted_flg<>'Y') msn,
                    --bec_ods.mtl_item_locations mil,
                    (select * from bec_ods.mtl_material_transactions where is_deleted_flg<>'Y') mmt,
                    (select * from bec_ods.mtl_unit_transactions where is_deleted_flg<>'Y') mut,
                    (select * from bec_ods.mtl_transaction_types where is_deleted_flg<>'Y') mtt,
                    (select * from bec_ods.cst_item_costs where is_deleted_flg<>'Y') ctc,
--                    bec_ods.mtl_item_categories mic,
--                    bec_ods.mtl_categories mc,
                    (select * from bec_ods.rcv_transactions where is_deleted_flg<>'Y') rct,
                    (select * from bec_ods.mtl_txn_source_types where is_deleted_flg<>'Y') mts,
                    (select * from bec_ods.org_organization_definitions where is_deleted_flg<>'Y') ood,
                    (select * from bec_ods.mtl_secondary_inventories where is_deleted_flg<>'Y') sub
              WHERE msi.inventory_item_id = msn.inventory_item_id
                AND msn.current_organization_id = msi.organization_id
                --  AND msn.current_organization_id = msn.owning_organization_id
                AND msn.last_transaction_id = mut.transaction_id(+)
                AND mut.transaction_id = mmt.transaction_id(+)
                AND msn.serial_number = mut.serial_number(+)
                AND mmt.transaction_type_id = mtt.transaction_type_id(+)
                AND mmt.transaction_source_type_id = mts.transaction_source_type_id(+)
                --AND mil.inventory_location_id(+) = msn.current_locator_id
                AND msn.current_status = 3
                AND msi.serial_number_control_code <> 1
                AND mmt.rcv_transaction_id = rct.transaction_id(+)
                AND msn.current_organization_id = ctc.organization_id(+)
                AND msn.inventory_item_id = ctc.inventory_item_id(+)
--                AND msi.inventory_item_id = mic.inventory_item_id(+)
--                AND msi.organization_id = mic.organization_id(+)
--                AND mic.category_id = mc.category_id(+)
--                AND mic.category_set_id = 1
                AND ctc.cost_type_id(+) = '1'
                AND msn.current_subinventory_code = sub.secondary_inventory_name(+)
                AND msn.current_organization_id = sub.organization_id(+)
                AND msi.organization_id = ood.organization_id
   ORDER BY subinventory, part_number,  serial_number ASC ) inv_onhand;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_inv_onhand'
	and batch_name = 'inv';

commit;