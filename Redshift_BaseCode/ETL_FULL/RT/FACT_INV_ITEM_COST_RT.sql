/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh_rpt.FACT_INV_ITEM_COST_RT;

create table bec_dwh_rpt.FACT_INV_ITEM_COST_RT 
	diststyle all as (
SELECT 	cic.organization_id ,
	cic.inventory_item_id ,
	msi.part_number ,
	msi.description,
	msi.primary_uom_code,
	msi.inventory_item_status_code status,
	--DECODE (msi.planning_make_buy_code,1, 'Make',2, 'Buy', NULL) 
	msi.planning_make_buy_code TYPE1,	
	cct.cost_type,
	msi.planner_code,
	msi.organization_code,
	msi.organization_name,
    DECODE (cic.based_on_rollup_flag,
                  1, 'Yes',
                  2, 'No',
                  3, 'Default',
                  cic.based_on_rollup_flag::char
                 )  based_on_rollup_flag,
    cic.item_cost extended_cost,
    NVL (cic.material_cost, 0) tot_material_cost,
	cic.material_overhead_cost total_material_overhead,
    cic.resource_cost, 
	cic.overhead_cost, 
	cic.outside_processing_cost,
	mic.item_category_segment1,
	mic.item_category_segment2,
	mic.item_category_segment3,
	mic.item_category_segment4,
	nvl((SELECT sum(item_cost) 
     FROM bec_dwh.DIM_ITEM_COST_DETAILS ccd
     WHERE ccd.inventory_item_id = cic.inventory_item_id
     AND ccd.organization_id = cic.organization_id
     AND ccd.cost_element_id = 1
     AND ccd.cost_type_id = cic.cost_type_id
     AND ccd.resource_code = 'FinanceAdj'
	 ),0) mat_financeadj,
	 nvl((SELECT sum(item_cost) 
     FROM bec_dwh.DIM_ITEM_COST_DETAILS ccd
     WHERE ccd.inventory_item_id = cic.inventory_item_id
     AND ccd.organization_id = cic.organization_id
     AND ccd.cost_element_id = 1
     AND ccd.cost_type_id = cic.cost_type_id
     AND ccd.resource_code = 'Mat OSP'
	 ),0) mat_osp,
	 nvl((SELECT sum(item_cost) 
     FROM bec_dwh.DIM_ITEM_COST_DETAILS ccd
     WHERE ccd.inventory_item_id = cic.inventory_item_id
     AND ccd.organization_id = cic.organization_id
     AND ccd.cost_element_id = 1
     AND ccd.cost_type_id = cic.cost_type_id
     AND ccd.resource_code = 'Material'
	 ),0) mat_material,
	 nvl((SELECT sum(item_cost) 
     FROM bec_dwh.DIM_ITEM_COST_DETAILS ccd
     WHERE ccd.inventory_item_id = cic.inventory_item_id
     AND ccd.organization_id = cic.organization_id
     AND ccd.cost_element_id = 2
     AND ccd.cost_type_id = cic.cost_type_id
     AND ccd.resource_code = 'MOH'
	 ),0) MOH,
	  nvl((SELECT sum(item_cost) 
     FROM bec_dwh.DIM_ITEM_COST_DETAILS ccd
     WHERE ccd.inventory_item_id = cic.inventory_item_id
     AND ccd.organization_id = cic.organization_id
     AND ccd.cost_element_id = 2
     AND ccd.cost_type_id = cic.cost_type_id
     AND ccd.resource_code = 'FOH'
	 ),0) FOH,
	  nvl((SELECT sum(item_cost) 
     FROM bec_dwh.DIM_ITEM_COST_DETAILS ccd
     WHERE ccd.inventory_item_id = cic.inventory_item_id
     AND ccd.organization_id = cic.organization_id
     AND ccd.cost_element_id = 2
     AND ccd.cost_type_id = cic.cost_type_id
     AND ccd.resource_code = 'Fab L'
	 ),0) Fab,
	  nvl((SELECT sum(item_cost) 
     FROM bec_dwh.DIM_ITEM_COST_DETAILS ccd
     WHERE ccd.inventory_item_id = cic.inventory_item_id
     AND ccd.organization_id = cic.organization_id
     AND ccd.cost_element_id = 2
     AND ccd.cost_type_id = cic.cost_type_id
     AND ccd.resource_code = 'Assy-L'
	 ),0) aoh,
	  nvl((SELECT sum(item_cost) 
     FROM bec_dwh.DIM_ITEM_COST_DETAILS ccd
     WHERE ccd.inventory_item_id = cic.inventory_item_id
     AND ccd.organization_id = cic.organization_id
     AND ccd.cost_element_id = 2
     AND ccd.cost_type_id = cic.cost_type_id
     AND ccd.resource_code = 'Stk Yield'
	 ),0) StkYield,
	  nvl((SELECT sum(item_cost) 
     FROM bec_dwh.DIM_ITEM_COST_DETAILS ccd
     WHERE ccd.inventory_item_id = cic.inventory_item_id
     AND ccd.organization_id = cic.organization_id
     AND ccd.cost_element_id = 2
     AND ccd.cost_type_id = cic.cost_type_id
     AND ccd.resource_code = 'IFM Yield'
	 ),0) IFMYield,
	 msi.program_name --new enhancement
FROM
	 bec_Dwh.fact_item_master msi
	,(select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y')cic
	,(select * from bec_ods.cst_cost_types where is_deleted_flg <> 'Y') cct
	,(select * from bec_dwh.dim_inv_item_category_set where category_set_id= 1 and is_deleted_flg <> 'Y') mic
WHERE msi.inventory_item_id(+) = cic.inventory_item_id
AND   msi.organization_id(+)   = cic.organization_id
AND   cic.cost_type_id       = cct.cost_type_id
and   cic.ORGANIZATION_ID   = mic.ORGANIZATION_ID(+)
and   cic.INVENTORY_ITEM_ID = mic.INVENTORY_ITEM_ID(+)
 ) ;
 
 end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate() 
where
	dw_table_name = 'fact_inv_item_cost_rt'
	and batch_name = 'inv';

commit;