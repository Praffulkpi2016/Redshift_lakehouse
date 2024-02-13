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

drop table if exists bec_dwh.FACT_INV_ONHAND_TMP2;

create table bec_dwh.FACT_INV_ONHAND_TMP2 diststyle all sortkey(TRANSACTION_ID,inventory_item_id,organization_id)
as
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
                AND msi.organization_id = ood.organization_id;
commit;


drop table if exists bec_dwh.qty cascade;

create table bec_dwh.qty diststyle all sortkey(inventory_item_id,organization_id) as
(select SUM (primary_transaction_quantity) as primary_transaction_quantity,inventory_item_id,organization_id 
from bec_ods.mtl_onhand_quantities_detail qty where is_deleted_flg<>'Y'
group by inventory_item_id,organization_id);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_inv_onhand_tmp2'
	and batch_name = 'inv';

commit;