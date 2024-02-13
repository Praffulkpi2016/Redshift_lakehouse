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

truncate table bec_dwh.FACT_INV_ONHAND_TMP1;

insert into bec_dwh.FACT_INV_ONHAND_TMP1
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
                FROM bec_ods.fnd_lookup_values mfl
               WHERE mfl.lookup_type = 'MRP_PLANNING_CODE' and mfl.is_deleted_flg<>'Y'
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
                                         FROM bec_ods.wip_entities 
                                        WHERE is_deleted_flg<>'Y'and wip_entity_id =
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
                     miq.organization_id, 'N', 'Y');
commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_inv_onhand_tmp1'
	and batch_name = 'inv';

commit;