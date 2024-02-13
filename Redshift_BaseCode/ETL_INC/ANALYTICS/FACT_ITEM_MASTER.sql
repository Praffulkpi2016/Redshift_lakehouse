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
-- Delete Records
--delete records from temp table
TRUNCATE bec_dwh.fact_item_master_tmp;
--Insert records into temp table

insert into bec_dwh.fact_item_master_tmp
(
select distinct inventory_item_id,organization_id
from
bec_ods.mtl_system_items_b
where 1=1
and kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_item_master' and batch_name = 'inv')
);
commit;
--delete records from fact table
delete from bec_dwh.FACT_ITEM_MASTER
WHERE exists (select 1 from bec_dwh.fact_item_master_tmp tmp
              where tmp.inventory_item_id = fact_item_master.inventory_item_id
			  and tmp.organization_id = fact_item_master.organization_id
             );
commit;
-- Insert records into fact table
insert into bec_dwh.fact_item_master 
(
SELECT DISTINCT
           mtl.inventory_item_id,
		   (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| mtl.inventory_item_id as INVENTORY_ITEM_ID_KEY,
           mtl.organization_id,
		   (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| mtl.organization_id as ORGANIZATION_ID_KEY,
           mtl.segment1 AS part_number,
           mtl.description,
           mtlp.organization_code,
           rcvh.routing_name,
           mtl.inventory_item_status_code,
		   bomr.revision_id,
           bomr.current_revision,
           mtl.item_type,
           mtl.planner_code,
		   mtl.buyer_id,
           mtl.primary_uom_code,
           (SELECT mfl.meaning
              FROM (select * from bec_ods.fnd_lookup_values where is_deleted_flg<>'Y') mfl
             WHERE     mfl.lookup_type = 'MRP_PLANNING_CODE'
                   AND mtl.mrp_planning_code = mfl.lookup_code)
              AS mrp_planning,
           mtl.max_minmax_quantity,
           mtl.min_minmax_quantity,
           (SELECT mfl.meaning
              FROM (select * from bec_ods.fnd_lookup_values where is_deleted_flg<>'Y') mfl
             WHERE     mfl.lookup_type = 'WIP_SUPPLY'
                   AND mtl.wip_supply_type = mfl.lookup_code)
              AS wip_supply_type,
           (SELECT mfl.meaning
              FROM (select * from bec_ods.fnd_lookup_values where is_deleted_flg<>'Y') mfl
             WHERE     mfl.lookup_type = 'MTL_PLANNING_MAKE_BUY'
                   AND mfl.lookup_code = mtl.planning_make_buy_code)
              AS planning_make_buy_code,
           mtl.preprocessing_lead_time,
           mtl.full_lead_time,
           mtl.postprocessing_lead_time,
           mtl.fixed_lead_time,
           mtl.variable_lead_time,
           mtl.cum_manufacturing_lead_time,
           mtl.cumulative_total_lead_time,
           mtl.lead_time_lot_size,
           mtl.shrinkage_rate,
           mtl.fixed_days_supply,
           mtl.fixed_order_quantity,
           mtl.fixed_lot_multiplier,
           ood.organization_name,
              mc.segment3
           || '.'
           || mc.segment4
           || '.'
           || mc.segment1
           || '.'
           || mc.segment2
           || '.'
           || mc.segment5
           || '.'
           || mc.segment6
              CATEGORY,
           DECODE (mtl.lot_control_code, 1, 'No Control', 'Full Control')
              lot_control,
           DECODE (mtl.serial_number_control_code,
                   2, 'Predefined',
                   1, 'No Control',
                   5, 'At Receipt',
                   NULL)
              serial_control,
           mtl.minimum_order_quantity,
           mtl.maximum_order_quantity,
           DECODE (mtl.planning_time_fence_code,
                   4, 'User Defined',
                   3, 'Total Lead_time')
              planning_time_fence_code,
           mtl.planning_time_fence_days,
           DECODE (mtl.release_time_fence_code, 4, 'User Defined', NULL)
              release_time_fence_code,
           mtl.release_time_fence_days,
           nvl(mtl.comms_nl_trackable_flag,'N') ib_trackable_flag,
           mtl.creation_date,
           mcat.segment1 FPA_Categ_Seg1,
           mcat.segment2 FPA_Categ_Seg2,
		   mir.revision, --Added for QuickSight
		   mtl.attribute5 as program_name, --New enhancement
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
		source_system = 'EBS')		
		|| '-' || nvl(mtl.inventory_item_id, 0)
		|| '-' || nvl(mtl.organization_id, 0)		
		|| '-' || nvl(bomr.revision_id, 0)		
				as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
    FROM   (select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') mtl,
           (select * from bec_ods.mtl_parameters where is_deleted_flg<>'Y') mtlp,
           (select * from bec_ods.rcv_routing_headers where is_deleted_flg<>'Y') rcvh,
           (select * from bec_ods.BOM_ITEM_CURRENT_REV_VIEW where is_deleted_flg<>'Y') bomr,
           (select * from bec_ods.org_organization_definitions where is_deleted_flg<>'Y') ood,
           (select * from bec_ods.mtl_categories_b where is_deleted_flg<>'Y') mc,
           (select * from bec_ods.mtl_item_categories where is_deleted_flg<>'Y') mic,
           (select * from bec_ods.mtl_item_revisions_b where is_deleted_flg<>'Y') mir,
           ( select mc.segment1, mc.segment2, mic.inventory_item_id, mic.organization_id
             from   (select * from bec_ods.mtl_categories_b where is_deleted_flg<>'Y') mc, (select * from bec_ods.mtl_item_categories where is_deleted_flg<>'Y') mic
             where  mic.category_id  = mc.category_id (+)
                 AND mic.category_set_id = 1100000081 ) mcat
     WHERE     mtl.organization_id = mtlp.organization_id(+)
           AND mtl.receiving_routing_id = rcvh.routing_header_id(+)
           AND mtl.inventory_item_id = bomr.inventory_item_id(+)
           AND mtl.organization_id = bomr.organization_id
           AND mtl.organization_id = ood.organization_id
           AND mtl.inventory_item_id = mic.inventory_item_id(+)
           AND mtl.organization_id = mic.organization_id(+)
           AND mic.category_id = mc.category_id(+)
           AND mic.category_set_id = 1
           AND mtl.inventory_item_id = mir.inventory_item_id
           AND mtl.organization_id = mir.organization_id
           AND bomr.revision_id = mir.revision_id(+)
           AND mtl.inventory_item_id = mcat.inventory_item_id(+)
           AND mtl.organization_id = mcat.organization_id(+)  
   ORDER BY mtl.segment1 ASC
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_item_master'
	and batch_name = 'inv';

commit;