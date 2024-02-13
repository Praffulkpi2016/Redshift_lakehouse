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

drop table if exists bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG_INCR;

create table bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG_INCR as 
(
with		
wro as (
	select
		inventory_item_id,
		organization_id,
		wip_entity_id,
		operation_seq_num,
		quantity_per_assembly,
        required_quantity,
        quantity_issued,
        wip_supply_type
	from
		(select * from bec_ods.WIP_REQUIREMENT_OPERATIONS where is_deleted_flg <> 'Y')
	where
		operation_seq_num >= 0
		and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		where dw_table_name = 'fact_wo_value_variance_union1_stg'
		and batch_name = 'wip'))
		),
------------------------------------------------------------------------------------	
wdj as (
	select
		primary_item_id,
		organization_id,
		wip_entity_id,
		status_type,
		date_released,
        date_completed,
        date_closed,
        scheduled_start_date,
        class_code,
        quantity_completed,
        quantity_scrapped,
        start_quantity
	from
		(select * from bec_ods.wip_discrete_jobs where is_deleted_flg <> 'Y')
	where 1=1
	and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		where dw_table_name = 'fact_wo_value_variance_union1_stg'
		and batch_name = 'wip'))
	),
------------------------------------------------------------------------------------
we as (
select organization_id,
primary_item_id,
wip_entity_id,
wip_entity_name,
description
from (select * from bec_ods.wip_entities where is_deleted_flg <> 'Y')
where 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		where dw_table_name = 'fact_wo_value_variance_union1_stg'
		and batch_name = 'wip'))
),
------------------------------------------------------------------------------------
csc as(
select 
	inventory_item_id,
	organization_id,
	cost_update_id,
	standard_cost_revision_date,
	standard_cost	
from
	(select * from bec_ods.CST_STANDARD_COSTS where is_deleted_flg <> 'Y')
where 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		where dw_table_name = 'fact_wo_value_variance_union1_stg'
		and batch_name = 'wip'))
),
-------------------------------------------------------------------		
csc_max_id as(
select
	MAX (c2.cost_update_id) as max_cost_update_id,
	c2.inventory_item_id,
	c2.organization_id
from
	csc c2
inner join wro r
	on	nvl(c2.inventory_item_id,0) = nvl(r.inventory_item_id,0)
	and nvl(c2.organization_id,0) = nvl(r.organization_id,0)
inner join wdj d	
	on nvl(d.wip_entity_id,0) = nvl(r.wip_entity_id,0)
	and d.organization_id = r.organization_id
and	c2.standard_cost_revision_date <= NVL (d.date_closed,(SYSDATE))
group by c2.inventory_item_id,c2.organization_id
),
-------------------------------------------------------------------
cec as (
select
	inventory_item_id,
	organization_id,
	cost_update_id,
	cost_element_id,
	creation_date,
	standard_cost
from (select * from bec_ods.CST_ELEMENTAL_COSTS where is_deleted_flg <> 'Y')
where 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		where dw_table_name = 'fact_wo_value_variance_union1_stg'
		and batch_name = 'wip'))
),
------------------------------------------------------------------------------------
cec_max_date as (
SELECT MAX(e2.creation_date) as max_creation_date,e2.inventory_item_id,e2.organization_id,d.wip_entity_id
FROM cec e2
inner join wro r
on nvl(e2.inventory_item_id,0) = nvl(r.inventory_item_id,0)
AND nvl(e2.organization_id,0) = nvl(r.organization_id,0)
inner join wdj d	
on nvl(d.wip_entity_id,0) = nvl(r.wip_entity_id,0)
and d.organization_id = r.organization_id
AND e2.creation_date <= NVL (d.date_closed,(SYSDATE))
group by e2.inventory_item_id,e2.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------

cte1 as (
select 
	c1.inventory_item_id,
	c1.organization_id,
	c1.cost_update_id,
	d.wip_entity_id,
	c1.standard_cost	
from
	csc c1
inner join wro r
	on
	nvl(c1.inventory_item_id,0) = nvl(r.inventory_item_id,0)
	and nvl(c1.organization_id,0) = nvl(r.organization_id,0)
inner join wdj d
	on d.wip_entity_id = r.wip_entity_id
	and d.organization_id = r.organization_id
and	c1.standard_cost_revision_date <= NVL (d.date_closed,(SYSDATE))
and c1.cost_update_id in
(select	max_cost_update_id
from csc_max_id c2
where inventory_item_id = c1.inventory_item_id
and organization_id = c1.organization_id)
group by c1.inventory_item_id,
	c1.organization_id,
	c1.cost_update_id,
	d.wip_entity_id,
	c1.standard_cost
),
------------------------------------------------------------------------------------
cte2 as (
SELECT SUM (e1.standard_cost) as sum_standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
--WHERE cost_element_id = 1
) e1
inner join 
wro r
on e1.inventory_item_id = r.inventory_item_id
AND e1.organization_id = r.organization_id
inner join wdj d	
on d.wip_entity_id = r.wip_entity_id
and d.organization_id = r.organization_id
where e1.creation_date in (
	select max_creation_date
from cec_max_date
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),	
------------------------------------------------------------------------------------
cte3 as (
select
	SUM (e1.standard_cost) as sum_standard_cost,
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
from
	(
	select
		inventory_item_id,
		organization_id,
		creation_date,
		standard_cost
	from
		cec
	where
		cost_element_id = 1) e1
inner join wro r
on
nvl(e1.inventory_item_id,0) = nvl(r.inventory_item_id,0)
and nvl(e1.organization_id,0) = nvl(r.organization_id,0)
inner join wdj d	
on d.wip_entity_id = r.wip_entity_id
and d.organization_id = r.organization_id	
where e1.creation_date in (
select max_creation_date
from cec_max_date
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
),
------------------------------------------------------------------------------------
cte4 as (
select
	SUM (e1.standard_cost) as sum_standard_cost,
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
from
	(
	select
		inventory_item_id,
		organization_id,
		creation_date,
		standard_cost
	from
		cec
	where
		cost_element_id = 2) e1
inner join wro r
on
e1.inventory_item_id = r.inventory_item_id
and e1.organization_id = r.organization_id
inner join wdj d	
on d.wip_entity_id = r.wip_entity_id
and d.organization_id = r.organization_id
where
	e1.creation_date in (
select max_creation_date
from cec_max_date
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
),
------------------------------------------------------------------------------------
cte5 as (
select
	SUM (e1.standard_cost) as sum_standard_cost,
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
from
	(
	select
		inventory_item_id,
		organization_id,
		creation_date,
		standard_cost
	from
		cec
	where
		cost_element_id = 3) e1
inner join wro r
on
e1.inventory_item_id = r.inventory_item_id
and e1.organization_id = r.organization_id
inner join wdj d	
on d.wip_entity_id = r.wip_entity_id
and d.organization_id = r.organization_id
where
e1.creation_date in (
select max_creation_date
from cec_max_date
where organization_id = e1.organization_id
and inventory_item_id = e1.inventory_item_id
and wip_entity_id = d.wip_entity_id)
group by
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
),
------------------------------------------------------------------------------------
cte6 as (
select
	SUM (e1.standard_cost) as sum_standard_cost,
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
from
	(
	select
		inventory_item_id,
		organization_id,
		creation_date,
		standard_cost
	from
		cec
	where
		cost_element_id = 4) e1
inner join wro r
on
	e1.inventory_item_id = r.inventory_item_id
	and e1.organization_id = r.organization_id
inner join wdj d	
on d.wip_entity_id = r.wip_entity_id
and d.organization_id = r.organization_id
where
	e1.creation_date in (
select max_creation_date
from cec_max_date
where organization_id = e1.organization_id
and inventory_item_id = e1.inventory_item_id
and wip_entity_id = d.wip_entity_id)
group by
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
),
------------------------------------------------------------------------------------
cte7 as (
select
	SUM (e1.standard_cost) as sum_standard_cost,
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
from
	(
	select
		inventory_item_id,
		organization_id,
		creation_date,
		standard_cost
	from
		cec
	where
		cost_element_id = 5) e1
inner join wro r
on
	e1.inventory_item_id = r.inventory_item_id
	and e1.organization_id = r.organization_id
inner join wdj d	
on d.wip_entity_id = r.wip_entity_id	
and d.organization_id = r.organization_id
where
	e1.creation_date in (
select max_creation_date
from cec_max_date
where organization_id = e1.organization_id
and inventory_item_id = e1.inventory_item_id
and wip_entity_id = d.wip_entity_id)
group by
	e1.inventory_item_id,
	e1.organization_id,
	d.wip_entity_id
)
------------------------------------------------------------------------------------	
select  organization_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where
		source_system = 'EBS')|| '-' || organization_id as organization_id_key,
		primary_item_id,
		inventory_item_id,
		(select system_id from bec_etl_ctrl.etlsourceappid where
		source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_key,
		wip_entity_id,
		(select system_id from bec_etl_ctrl.etlsourceappid where
		source_system = 'EBS')|| '-' || wip_entity_id as wip_entity_id_key,
        work_order,
        wo_description,
		assembly_item,
		ASSEMBLY_DESCRIPTION,
		--WO_Status,
		status_type,
		cast(start_quantity as int) as start_quantity,
        cast(quantity_completed as int) as quantity_completed,
        cast(quantity_scrapped as int) as quantity_scrapped,
		date_released,
        date_completed,
        date_closed,
        JOB_START_DATE,
        class_code,
		item_number,
		item_description,
		cast(operation_seq_num as int) as operation_seq_num,
        quantity_per_assembly,
        required_quantity,
        quantity_issued,
		WIP_Supply_Type,
		cast(department_code as varchar) as department_code,
        cast(res_op_seq_num as int) as res_op_seq_num,
        cast(resource_seq_num as int) as resource_seq_num,
        cast(resource_code as varchar) as resource_code,
        cast(res_uom_code as varchar) as res_uom_code,
        cast(Res_usage_rate as int) as Res_usage_rate,
        cast(applied_resource_units as int) as applied_resource_units,
		item_transaction_value,
		item_cost_at_close,
		unit_item_cost_at_close,
		mtl_transaction_value,
		mtl_cost_at_close,
		unit_mtl_cost_at_close,
		mtl_oh_transaction_value,
		mtl_oh_cost_at_close,
		unit_mtl_oh_cost_at_close,
		res_transaction_value,
		res_cost_at_close,
		unit_res_cost_at_close,
		osp_transaction_value,
		osp_cost_at_close,
		unit_osp_cost_at_close,
		oh_transaction_value,
		oh_cost_at_close,
		unit_oh_cost_at_close,
		cast(cost_update_txn_value as int) as cost_update_txn_value,
        cast(Mtl_variance as int) as Mtl_variance,
        cast(mtl_oh_variance as int) as mtl_oh_variance,
        cast(Res_variance as int) as Res_variance,
        cast(osp_variance as int) as osp_variance,
        cast(oh_variance as int) as oh_variance,
        cast(net_value as int) as net_value,
        organization_id1,
        ALTERNATE_BOM,
        COMMENTS,
		decode(comments, 'Work Order Components', 
		(nvl(cast(mtl_transaction_value as int),0) - nvl(cast(item_cost_at_close as int),0 ))) mtl_value_difference,		
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
    || '-' || nvl(organization_id, 0)
	|| '-' || nvl(inventory_item_id, 0) 
	|| '-' || nvl(wip_entity_id, 0) 
	|| '-' || nvl(cast(operation_seq_num as int), 0) 
	|| '-' || nvl(cast(res_op_seq_num as int), 0) 
	|| '-' || nvl(cast(resource_seq_num as int), 0)
	|| '-' || nvl(WIP_Supply_Type, 'NA')
	|| '-' || nvl(ALTERNATE_BOM, 'NA')
	|| '-' || nvl(comments, 'NA') as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date		
 from
(
SELECT 
						 e.organization_id,
						 e.primary_item_id,
						 r.inventory_item_id,
						 e.wip_entity_id,
                         e.wip_entity_name work_order,
                         e.description wo_description,
                         (SELECT segment1
                            FROM (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')
                           WHERE organization_id = e.organization_id
                                 AND inventory_item_id = e.primary_item_id)
                            assembly_item,
                         (SELECT description
                            FROM (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')
                           WHERE organization_id = e.organization_id
                                 AND inventory_item_id = e.primary_item_id)
                            ASSEMBLY_DESCRIPTION,
                          /*DECODE (d.status_type::VARCHAR,
                               1, 'Unreleased',
                               3, 'Released',
                               12, 'Closed',
                               d.status_type::VARCHAR)::VARCHAR(30)
                          WO_Status,*/
						  d.status_type,
                         NULL start_quantity,
                         NULL quantity_completed,
                         NULL quantity_scrapped,
                         d.date_released,
                         d.date_completed,
                         d.date_closed,
			 			 d.scheduled_start_date JOB_START_DATE,
                         d.class_code,
                         (SELECT segment1
                            FROM (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')
                           WHERE organization_id = r.organization_id
                                 AND inventory_item_id = r.inventory_item_id)
                            item_number,
                         (SELECT description
                            FROM (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')
                           WHERE organization_id = r.organization_id
                                 AND inventory_item_id = r.inventory_item_id)
                            item_description,
                         r.operation_seq_num,
                         r.quantity_per_assembly,
                         r.required_quantity,
                         r.quantity_issued,
                         DECODE (cast(r.wip_supply_type as int)::VARCHAR,
                                 1, 'Push',
                                 2, 'Assembly Pull',
                                 3, 'Operation Pull',
                                 4, 'Bulk',
                                 5, 'Vendor',
                                 6, 'Phantom',
                                 7, 'Based on Bill',
                                 cast(r.wip_supply_type as int)::VARCHAR)::VARCHAR(30) WIP_Supply_Type,
                         NULL department_code,
                         NULL res_op_seq_num,
                         NULL resource_seq_num,
                         NULL resource_code,
                         NULL res_uom_code,
                         NULL Res_usage_rate,
                         NULL applied_resource_units,
                         --y.transaction_type_name,
                         (SELECT SUM (base_transaction_value)
                            FROM (select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                 (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') t
                           WHERE a.transaction_id = t.transaction_id
                                 AND t.transaction_source_type_id = 5
                                 AND t.transaction_source_id =
                                        e.wip_entity_id
                                 AND r.inventory_item_id =
                                        t.inventory_item_id
                                 --AND A.COST_ELEMENT_ID = 1
                                 AND a.accounting_line_type = 7)
                            item_transaction_value,
                         ( (CASE
                               WHEN cte1.standard_cost  = 0
                               THEN
                                  0
                               ELSE
                                  cte2.sum_standard_cost
                            END)
                          * r.required_quantity)
                            item_cost_at_close,
                         CASE
                            WHEN cte1.standard_cost = 0
                            THEN
                               0
                            ELSE
                               cte2.sum_standard_cost
                         END
                            unit_item_cost_at_close,
                         (SELECT SUM (base_transaction_value)
                            FROM (select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                 (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') t
                           WHERE a.transaction_id = t.transaction_id
                                 AND t.transaction_source_type_id = 5
                                 AND t.transaction_source_id =
                                        e.wip_entity_id
                                 AND r.inventory_item_id =
                                        t.inventory_item_id
                                 AND A.COST_ELEMENT_ID = 1
                                 AND a.accounting_line_type = 7)
                            mtl_transaction_value,
                         ( (CASE
                               WHEN cte1.standard_cost = 0
                               THEN
                                  0
                               ELSE
                                  cte3.sum_standard_cost
                            END)
                          * r.required_quantity)
                            mtl_cost_at_close,
                         CASE
                            WHEN cte1.standard_cost = 0
                            THEN
                               0
                            ELSE
                               cte3.sum_standard_cost
                         END
                            unit_mtl_cost_at_close,
                         (SELECT SUM (base_transaction_value)
                            FROM (select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                 (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') t
                           WHERE a.transaction_id = t.transaction_id
                                 AND t.transaction_source_type_id = 5
                                 AND t.transaction_source_id =
                                        e.wip_entity_id
                                 AND r.inventory_item_id =
                                        t.inventory_item_id
                                 AND A.COST_ELEMENT_ID = 2
                                 AND a.accounting_line_type = 7)
                            mtl_oh_transaction_value,
                         ( (CASE
                               WHEN cte1.standard_cost = 0
                               THEN
                                  0
                               ELSE
                                  cte4.sum_standard_cost
                            END)
                          * r.required_quantity)
                            mtl_oh_cost_at_close,
                         CASE
                            WHEN cte1.standard_cost = 0
                            THEN
                               0
                            ELSE
                               cte4.sum_standard_cost
                         END
                            unit_mtl_oh_cost_at_close,
                         (SELECT SUM (base_transaction_value)
                            FROM (select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                 (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') t
                           WHERE a.transaction_id = t.transaction_id
                                 AND t.transaction_source_type_id = 5
                                 AND t.transaction_source_id =
                                        e.wip_entity_id
                                 AND r.inventory_item_id =
                                        t.inventory_item_id
                                 AND A.COST_ELEMENT_ID = 3
                                 AND a.accounting_line_type = 7)
                            res_transaction_value,
                         ( (CASE
                               WHEN cte1.standard_cost = 0
                               THEN
                                  0
                               ELSE
                                  cte5.sum_standard_cost
                            END)
                          * r.required_quantity)
                            res_cost_at_close,
                         CASE
                            WHEN cte1.standard_cost = 0
                            THEN
                               0
                            ELSE
                               cte5.sum_standard_cost
                         END
                            unit_res_cost_at_close,
                         (SELECT SUM (base_transaction_value)
                            FROM (select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                 (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') t
                           WHERE a.transaction_id = t.transaction_id
                                 AND t.transaction_source_type_id = 5
                                 AND t.transaction_source_id =
                                        e.wip_entity_id
                                 AND r.inventory_item_id =
                                        t.inventory_item_id
                                 AND A.COST_ELEMENT_ID = 4
                                 AND a.accounting_line_type = 7)
                            osp_transaction_value,
                         ( (CASE
                               WHEN cte1.standard_cost = 0
                               THEN
                                  0
                               ELSE
                                  cte6.sum_standard_cost
                            END)
                          * r.required_quantity)
                            osp_cost_at_close,
                         CASE
                            WHEN cte1.standard_cost = 0
                            THEN
                               0
                            ELSE
                               cte6.sum_standard_cost
                         END
                            unit_osp_cost_at_close,
                         (SELECT SUM (base_transaction_value)
                            FROM (select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                 (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') t
                           WHERE a.transaction_id = t.transaction_id
                                 AND t.transaction_source_type_id = 5
                                 AND t.transaction_source_id =
                                        e.wip_entity_id
                                 AND r.inventory_item_id =
                                        t.inventory_item_id
                                 AND A.COST_ELEMENT_ID = 5
                                 AND a.accounting_line_type = 7)
                            oh_transaction_value,
                         ( (CASE
                               WHEN cte1.standard_cost = 0
                               THEN
                                  0
                               ELSE
                                  cte7.sum_standard_cost
                            END)
                          * r.required_quantity)
                            oh_cost_at_close,
                         CASE
                            WHEN cte1.standard_cost = 0
                            THEN
                               0
                            ELSE
                               cte7.sum_standard_cost
                         END
                            unit_oh_cost_at_close,
                         NULL cost_update_txn_value,
                         NULL Mtl_variance,
                         NULL mtl_oh_variance,
                         NULL Res_variance,
                         NULL osp_variance,
                         NULL oh_variance,
                         NULL net_value,
                         d.organization_id organization_id1,
                         NULL ALTERNATE_BOM,
                         'Work Order Components' COMMENTS
                    FROM                       
                         we e
                         inner join wdj d
                         on d.wip_entity_id = e.wip_entity_id
                         inner join wro r
                         on d.wip_entity_id = r.wip_entity_id
                         AND d.organization_id = r.organization_id
                         left outer join cte1 cte1 
                         ON r.inventory_item_id = cte1.inventory_item_id
						 AND r.organization_id = cte1.organization_id
			             AND e.wip_entity_id = cte1.wip_entity_id
			             left outer join cte2 cte2 
                         ON r.inventory_item_id = cte2.inventory_item_id
						 AND r.organization_id = cte2.organization_id
			             AND r.wip_entity_id = cte2.wip_entity_id
			             left outer join cte3 cte3 
                         ON r.inventory_item_id = cte3.inventory_item_id
						 AND r.organization_id = cte3.organization_id
			             AND r.wip_entity_id = cte3.wip_entity_id
			             left outer join cte4 cte4 
                         ON r.inventory_item_id = cte4.inventory_item_id
						 AND r.organization_id = cte4.organization_id
			             AND r.wip_entity_id = cte4.wip_entity_id
			             left outer join cte5 cte5 
                         ON r.inventory_item_id = cte5.inventory_item_id
						 AND r.organization_id = cte5.organization_id
			             AND r.wip_entity_id = cte5.wip_entity_id
			             left outer join cte6 cte6 
                         ON r.inventory_item_id = cte6.inventory_item_id
						 AND r.organization_id = cte6.organization_id
			             AND r.wip_entity_id = cte6.wip_entity_id
			             left outer join cte7 cte7 
                         ON r.inventory_item_id = cte7.inventory_item_id
						 AND r.organization_id = cte7.organization_id
			             AND r.wip_entity_id = cte7.wip_entity_id)
);
commit;

-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
delete from bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG
where (
nvl(organization_id, 0)
,nvl(inventory_item_id, 0) 
,nvl(wip_entity_id, 0) 
,nvl(cast(operation_seq_num as int), 0) 
,nvl(cast(res_op_seq_num as int), 0) 
,nvl(cast(resource_seq_num as int), 0)
,nvl(WIP_Supply_Type, 'NA')
,nvl(ALTERNATE_BOM, 'NA')
,nvl(comments, 'NA')
)
IN
(select  
nvl(ods.organization_id, 0),
nvl(ods.inventory_item_id, 0) 
,nvl(ods.wip_entity_id, 0) 
,nvl(cast(ods.operation_seq_num as int), 0) 
,nvl(cast(ods.res_op_seq_num as int), 0) 
,nvl(cast(ods.resource_seq_num as int), 0)
,nvl(ods.WIP_Supply_Type, 'NA')
,nvl(ods.ALTERNATE_BOM, 'NA')
,nvl(ods.comments, 'NA')
from bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG_INCR ods, bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG dw
 where 1=1
 and dw.dw_load_id =
 		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS'
    )
    || '-' || nvl(ods.organization_id, 0)
	|| '-' || nvl(ods.inventory_item_id, 0) 
	|| '-' || nvl(ods.wip_entity_id, 0) 
	|| '-' || nvl(cast(ods.operation_seq_num as int), 0) 
	|| '-' || nvl(cast(ods.res_op_seq_num as int), 0) 
	|| '-' || nvl(cast(ods.resource_seq_num as int), 0)
	|| '-' || nvl(ods.WIP_Supply_Type, 'NA')
	|| '-' || nvl(ods.ALTERNATE_BOM, 'NA')
	|| '-' || nvl(ods.comments, 'NA')
);
commit;
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
insert into bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG
select * from bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG_INCR;
commit;
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'fact_wo_value_variance_union1_stg' and batch_name = 'wip';

COMMIT;