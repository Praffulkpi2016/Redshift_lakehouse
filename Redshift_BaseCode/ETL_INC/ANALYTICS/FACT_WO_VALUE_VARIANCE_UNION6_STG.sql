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

drop table if exists bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG_INCR;

create table bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG_INCR as
(select * ,
row_number() over(partition by dw_load_id) as rownumber
from (
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
		where dw_table_name = 'fact_wo_value_variance_union6_stg'
		and batch_name = 'wip'))
	),
------------------------------------------------------------------------------------
wor as (
select
	organization_id,
	wip_entity_id,
	resource_id,
	department_id,
	operation_seq_num,
	resource_seq_num,
	uom_code,
	usage_rate_or_amount,
	applied_resource_units
from
	(select * from bec_ods.WIP_OPERATION_RESOURCES where is_deleted_flg <> 'Y')
	where 1=1		and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		where dw_table_name = 'fact_wo_value_variance_union6_stg'
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
			where 1=1	and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		where dw_table_name = 'fact_wo_value_variance_union6_stg'
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
		where dw_table_name = 'fact_wo_value_variance_union6_stg'
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
		where dw_table_name = 'fact_wo_value_variance_union6_stg'
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
and	c2.standard_cost_revision_date <= NVL (d.date_closed,(GETDATE()))
group by c2.inventory_item_id,c2.organization_id
),
-------------------------------------------------------------------
csc_max_id1 as (
select
	MAX (c2.cost_update_id) as max_cost_update_id1,
	c2.inventory_item_id,c2.organization_id--,d.wip_entity_id
from
	csc c2
inner join wdj d	
	on nvl(c2.inventory_item_id,0) = nvl(d.primary_item_id,0)
	and nvl(c2.organization_id,0) = nvl(d.organization_id,0)
and c2.standard_cost_revision_date <= NVL (d.date_closed,(GETDATE()))
group by c2.inventory_item_id,c2.organization_id--,d.wip_entity_id
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
AND e2.creation_date <= NVL (d.date_closed,(GETDATE()))
group by e2.inventory_item_id,e2.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cec_max_date1 as (
SELECT MAX(e2.creation_date) as max_creation_date1,e2.inventory_item_id,e2.organization_id,d.wip_entity_id
FROM cec e2
inner join wdj d
on nvl(e2.inventory_item_id,0) = nvl(d.primary_item_id,0)
and nvl(e2.organization_id,0) = nvl(d.organization_id,0)
and e2.creation_date <= NVL (d.date_closed,(GETDATE()))
group by e2.inventory_item_id,e2.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
bsb as (
SELECT
	b.ASSEMBLY_ITEM_ID,
	b.ORGANIZATION_ID,
	b.ALTERNATE_BOM_DESIGNATOR,
	b.LAST_UPDATE_DATE,
	b.BILL_SEQUENCE_ID
	FROM
	(select * from bec_ods.bom_structures_b where is_deleted_flg <> 'Y') b
WHERE b.obj_name IS NULL
AND nvl(b.effectivity_control, 1) <= 3
		and (b.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		where dw_table_name = 'fact_wo_value_variance_union6_stg'
		and batch_name = 'wip'))
),
------------------------------------------------------------------------------------
bcb as (
SELECT
	COMPONENT_ITEM_ID,
	LAST_UPDATE_DATE,
	COMPONENT_QUANTITY,
	INCLUDE_IN_COST_ROLLUP,
	BILL_SEQUENCE_ID,
	WIP_SUPPLY_TYPE
FROM
	(select * from bec_ods.bom_components_b where is_deleted_flg <> 'Y')
WHERE
	obj_name IS NULL
	AND overlapping_changes IS null
--	AND include_in_cost_rollup = 1
		and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
		where dw_table_name = 'fact_wo_value_variance_union6_stg'
		and batch_name = 'wip'))
	),
------------------------------------------------------------------------------------
csc_max_id2 as (
select
	MAX (c2.cost_update_id) as max_cost_update_id2,c2.inventory_item_id,c2.organization_id--,d.wip_entity_id
from
	csc c2
inner join bsb b
on c2.organization_id = b.organization_id
inner join bcb c
on 	b.bill_sequence_id = c.bill_sequence_id
and c2.inventory_item_id = c.component_item_id	
inner join wdj d
	on	b.assembly_item_id  = d.primary_item_id
	and b.organization_id = d.organization_id
where	c2.standard_cost_revision_date <= NVL (d.date_closed,(GETDATE()))
group by c2.inventory_item_id,c2.organization_id--,d.wip_entity_id
),
-------------------------------------------------------------------
cte15 as (
select 
	c1.inventory_item_id,
	c1.organization_id,
	c1.cost_update_id,
	d.wip_entity_id,
	c1.standard_cost	
from
	csc c1
inner join bsb b
on c1.organization_id = b.organization_id
inner join bcb c
on b.bill_sequence_id = c.bill_sequence_id
and c1.inventory_item_id = c.component_item_id
inner join wdj d
on	b.assembly_item_id  = d.primary_item_id
and b.organization_id = d.organization_id
where c1.standard_cost_revision_date <= NVL (d.date_closed,(GETDATE()))
and c1.cost_update_id in
(select
	max_cost_update_id2
from
	csc_max_id2
	where inventory_item_id = c1.inventory_item_id
	and organization_id = c1.organization_id
--	and wip_entity_id = d.wip_entity_id
	)	
group by c1.inventory_item_id,
	c1.organization_id,
	c1.cost_update_id,
	d.wip_entity_id,
	c1.standard_cost	
),	
------------------------------------------------------------------------------------
cec_max_date2 as (
SELECT MAX(e2.creation_date) as max_creation_date2,e2.inventory_item_id,e2.organization_id,d.wip_entity_id
FROM cec e2
inner join bsb b
on e2.organization_id = b.organization_id
inner join bcb c
on b.bill_sequence_id = c.bill_sequence_id
and e2.inventory_item_id = c.component_item_id
inner join wdj d
on b.assembly_item_id = d.primary_item_id
AND b.organization_id = d.organization_id
where e2.creation_date <= NVL (d.date_closed,(GETDATE()))
group by e2.inventory_item_id,e2.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte_16 as (
SELECT e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,cost_element_id,creation_date,standard_cost
from cec
--WHERE cost_element_id = 1
) e1
inner join bsb b
on e1.organization_id = b.organization_id
inner join bcb c
on b.bill_sequence_id = c.bill_sequence_id
and e1.inventory_item_id = c.component_item_id
inner join wdj d
on b.assembly_item_id = d.primary_item_id
AND b.organization_id = d.organization_id
where e1.creation_date IN
(SELECT max_creation_date2
FROM cec_max_date2
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id
)
group by e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte16 as (
select sum(standard_cost) as sum_standard_cost,inventory_item_id,organization_id,wip_entity_id
from cte_16
group by inventory_item_id,organization_id,wip_entity_id
),
------------------------------------------------------------------------------------
cte_17 as (
SELECT e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 1
) e1
inner join bsb b
on e1.organization_id = b.organization_id
inner join bcb c
on b.bill_sequence_id = c.bill_sequence_id
and e1.inventory_item_id = c.component_item_id
inner join wdj d
on b.assembly_item_id = d.primary_item_id
AND b.organization_id = d.organization_id
where e1.creation_date IN
(SELECT max_creation_date2
FROM cec_max_date2
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id
)
group by e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte17 as (
SELECT SUM (standard_cost) as sum_standard_cost,inventory_item_id,organization_id,wip_entity_id
from cte_17
group by inventory_item_id,organization_id,wip_entity_id
),
------------------------------------------------------------------------------------
cte_18 as (
SELECT e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 2
) e1
inner join bsb b
on e1.organization_id = b.organization_id
inner join bcb c
on b.bill_sequence_id = c.bill_sequence_id
and e1.inventory_item_id = c.component_item_id
inner join wdj d
on b.assembly_item_id = d.primary_item_id
AND b.organization_id = d.organization_id
where e1.creation_date IN
(SELECT max_creation_date2 FROM cec_max_date2
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte18 as (
SELECT SUM (standard_cost) as sum_standard_cost,inventory_item_id,organization_id,wip_entity_id
from cte_18
group by inventory_item_id,organization_id,wip_entity_id
),
------------------------------------------------------------------------------------
cte_19 as (
SELECT e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 3
) e1
inner join bsb b
on e1.organization_id = b.organization_id
inner join bcb c
on b.bill_sequence_id = c.bill_sequence_id
and e1.inventory_item_id = c.component_item_id
inner join wdj d
on b.assembly_item_id = d.primary_item_id
AND b.organization_id = d.organization_id
where e1.creation_date IN
(SELECT max_creation_date2 FROM cec_max_date2
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte19 as (
SELECT SUM (standard_cost) as sum_standard_cost,inventory_item_id,organization_id,wip_entity_id
from cte_19
group by inventory_item_id,organization_id,wip_entity_id
),
------------------------------------------------------------------------------------
cte_20 as (
SELECT e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 4
) e1
inner join bsb b
on e1.organization_id = b.organization_id
inner join bcb c
on b.bill_sequence_id = c.bill_sequence_id
and e1.inventory_item_id = c.component_item_id
inner join wdj d
on b.assembly_item_id = d.primary_item_id
AND b.organization_id = d.organization_id
where e1.creation_date IN 
(SELECT max_creation_date2 FROM cec_max_date2
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte20 as (
SELECT SUM (standard_cost) as sum_standard_cost,inventory_item_id,organization_id,wip_entity_id
from cte_20
group by inventory_item_id,organization_id,wip_entity_id
),
------------------------------------------------------------------------------------
cte_21 as (
SELECT e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 5
) e1
inner join bsb b
on e1.organization_id = b.organization_id
inner join bcb c
on b.bill_sequence_id = c.bill_sequence_id
and e1.inventory_item_id = c.component_item_id
inner join wdj d
on b.assembly_item_id = d.primary_item_id
AND b.organization_id = d.organization_id
where e1.creation_date IN
(SELECT max_creation_date2
FROM cec_max_date2
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id
)
group by e1.standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte21 as (
SELECT SUM (standard_cost) as sum_standard_cost,inventory_item_id,organization_id,wip_entity_id
from cte_21
group by inventory_item_id,organization_id,wip_entity_id
),
------------------------------------------------------------------------------------
cte22 as (
SELECT  e.organization_id,
						 e.primary_item_id,
						 c.component_item_id as inventory_item_id,
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
                         WHERE organization_id = b.organization_id
                               AND inventory_item_id = c.component_item_id)
                          item_number,
                       (SELECT description
                          FROM (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')
                         WHERE organization_id = b.organization_id
                               AND inventory_item_id = c.component_item_id)
                          item_description,
                       NULL operation_seq_num,
                       c.component_quantity quantity_per_assembly,
                       NULL required_quantity,
                       NULL quantity_issued,
                      DECODE (cast(c.wip_supply_type as int)::varchar,
                               1, 'Push',
                               2, 'Assembly Pull',
                               3, 'Operation Pull',
                               4, 'Bulk',
                               5, 'Vendor',
                               6, 'Phantom',
                               7, 'Based on Bill',
                               cast(c.wip_supply_type as int)::varchar)::varchar(30)
                          WIP_Supply_Type,
                       NULL department_code,
                       NULL res_op_seq_num,
                       NULL resource_seq_num,
                       NULL resource_code,
                       NULL res_uom_code,
                       NULL Res_usage_rate,
                       NULL applied_resource_units,
                       NULL item_transaction_value,
                       ( (CASE
                             WHEN cte15.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte16.sum_standard_cost
                          END)
                        * c.component_quantity)
                          item_cost_at_close,
                       CASE
                          WHEN cte15.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte16.sum_standard_cost
                       END
                          unit_item_cost_at_close,
                       NULL mtl_transaction_value,
                       ( (CASE
                             WHEN cte15.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte17.sum_standard_cost
                          END)
                        * c.component_quantity)
                          mtl_cost_at_close,
                       CASE
                          WHEN cte15.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte17.sum_standard_cost
                       END
                          unit_mtl_cost_at_close,
                       NULL mtl_oh_transaction_value,
                       ( (CASE
                             WHEN cte15.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte18.sum_standard_cost
                          END)
                        * c.component_quantity)
                          mtl_oh_cost_at_close,
                       CASE
                          WHEN cte15.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte18.sum_standard_cost
                       END
                          unit_mtl_oh_cost_at_close,
                       NULL res_transaction_value,
                       ( (CASE
                             WHEN cte15.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte19.sum_standard_cost
                          END)
                        * c.component_quantity)
                          res_cost_at_close,
                       CASE
                          WHEN cte15.standard_cost =
                                  0
                          THEN
                             0
                          ELSE
                             cte19.sum_standard_cost
                       END
                          unit_res_cost_at_close,
                       NULL osp_transaction_value,
                       ( (CASE
                             WHEN cte15.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte20.sum_standard_cost
                          END)
                        * c.component_quantity)
                          osp_cost_at_close,
                       CASE
                          WHEN cte15.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte20.sum_standard_cost
                       END
                          unit_osp_cost_at_close,
                       NULL oh_transaction_value,
                       ( (CASE
                             WHEN cte15.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte21.sum_standard_cost
                          END)
                        * c.component_quantity) oh_cost_at_close,
                       CASE
                          WHEN cte15.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte21.sum_standard_cost
                       END
                          unit_oh_cost_at_close,
                       NULL cost_update_txn_value,
                       NULL mtl_variance,
                       NULL mtl_oh_variance,
                       NULL res_variance,
                       NULL osp_variance,
                       NULL oh_variance,
                       NULL net_value,
                       d.organization_id as organization_id1,
                       b.alternate_bom_designator ALTERNATE_BOM,
                       'BOM Components not in Work Order' COMMENTS/*,
ROW_NUMBER () OVER (PARTITION BY 
e.organization_id,e.primary_item_id,c.component_item_id,e.wip_entity_id
ORDER BY e.organization_id,e.primary_item_id) AS RN	*/			   
--                       e1.creation_date
					   FROM 
					   bsb b
					   inner join bcb c
					   on b.bill_sequence_id = c.bill_sequence_id
					   inner join wdj d
					   on b.assembly_item_id = d.primary_item_id
                       AND b.organization_id = d.organization_id
                       inner join we e
					   on d.wip_entity_id = e.wip_entity_id
					   AND c.include_in_cost_rollup = 1 
					   AND ((SELECT COUNT (1)
                                FROM (select * from bec_ods.WIP_REQUIREMENT_OPERATIONS where is_deleted_flg <> 'Y')
                               WHERE wip_entity_id = e.wip_entity_id
                                     AND inventory_item_id =
                                            c.component_item_id) = 0)
                       left outer join cte15
                       ON e.wip_entity_id = cte15.wip_entity_id
				   	   and c.component_item_id = cte15.inventory_item_id 
                       AND b.organization_id = cte15.organization_id
                       left outer join cte16
				   	   on e.wip_entity_id = cte16.wip_entity_id
				   	   and c.component_item_id = cte16.inventory_item_id
                       AND b.organization_id = cte16.organization_id
                       left outer join cte17
				   	   on e.wip_entity_id = cte17.wip_entity_id
				   	   and c.component_item_id = cte17.inventory_item_id
                       AND b.organization_id = cte17.organization_id
                       left outer join cte18
				   	   on e.wip_entity_id = cte18.wip_entity_id
				   	   and c.component_item_id = cte18.inventory_item_id
                       AND b.organization_id = cte18.organization_id 
                       left outer join cte19
				   	   on e.wip_entity_id = cte19.wip_entity_id
				   	   and c.component_item_id = cte19.inventory_item_id
                       AND b.organization_id = cte19.organization_id 
                       left outer join cte20
				   	   on e.wip_entity_id = cte20.wip_entity_id
				   	   and c.component_item_id = cte20.inventory_item_id
                       AND b.organization_id = cte20.organization_id 
                       left outer join cte21
				   	   on e.wip_entity_id = cte21.wip_entity_id
				   	   and c.component_item_id = cte21.inventory_item_id
                       AND b.organization_id = cte21.organization_id
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
        cast(required_quantity as int) as required_quantity,
        cast(quantity_issued as int) as quantity_issued,
		WIP_Supply_Type,
		cast(department_code as varchar) as department_code,
        cast(res_op_seq_num as int) as res_op_seq_num,
        cast(resource_seq_num as int) as resource_seq_num,
        cast(resource_code as varchar) as resource_code,
        cast(res_uom_code as varchar) as res_uom_code,
        cast(Res_usage_rate as int) as Res_usage_rate,
        cast(applied_resource_units as int) as applied_resource_units,
		cast(item_transaction_value as int) as item_transaction_value,
		item_cost_at_close,
		unit_item_cost_at_close,
		cast(mtl_transaction_value as int) as mtl_transaction_value,
		mtl_cost_at_close,
		unit_mtl_cost_at_close,
		cast(mtl_oh_transaction_value as int) as mtl_oh_transaction_value,
		mtl_oh_cost_at_close,
		unit_mtl_oh_cost_at_close,
		cast(res_transaction_value as int) as res_transaction_value,
		res_cost_at_close,
		unit_res_cost_at_close,
		cast(osp_transaction_value as int) as osp_transaction_value,
		osp_cost_at_close,
		unit_osp_cost_at_close,
		cast(oh_transaction_value as int) as oh_transaction_value,
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
SELECT  cte22.organization_id,
		cte22.primary_item_id,
		cte22.inventory_item_id,
		cte22.wip_entity_id,
        cte22.work_order,
        cte22.wo_description,
        cte22.assembly_item,
        cte22.ASSEMBLY_DESCRIPTION,
      --cte22.WO_Status,
        cte22.status_type,
        cte22.start_quantity,
        cte22.quantity_completed,
        cte22.quantity_scrapped,
        cte22.date_released,
        cte22.date_completed,
        cte22.date_closed,
		cte22.JOB_START_DATE,
        cte22.class_code,
        cte22.item_number,
        cte22.item_description,
        cte22.operation_seq_num,
        cte22.quantity_per_assembly,
        cte22.required_quantity,
        cte22.quantity_issued,
        cte22.WIP_Supply_Type,
        cte22.department_code,
        cte22.res_op_seq_num,
        cte22.resource_seq_num,
        cte22.resource_code,
        cte22.res_uom_code,
        cte22.Res_usage_rate,
        cte22.applied_resource_units,
        cte22.item_transaction_value,
        cte22.item_cost_at_close,
        cte22.unit_item_cost_at_close,
        cte22.mtl_transaction_value,
        cte22.mtl_cost_at_close,
        cte22.unit_mtl_cost_at_close,
        cte22.mtl_oh_transaction_value,
        cte22.mtl_oh_cost_at_close,
        cte22.unit_mtl_oh_cost_at_close,
        cte22.res_transaction_value,
        cte22.res_cost_at_close,
        cte22.unit_res_cost_at_close,
        cte22.osp_transaction_value,
        cte22.osp_cost_at_close,
        cte22.unit_osp_cost_at_close,
        cte22.oh_transaction_value,
        cte22.oh_cost_at_close,
        cte22.unit_oh_cost_at_close,
        cte22.cost_update_txn_value,
        cte22.mtl_variance,
        cte22.mtl_oh_variance,
        cte22.res_variance,
        cte22.osp_variance,
        cte22.oh_variance,
        cte22.net_value,
        cte22.organization_id1,
        cte22.ALTERNATE_BOM,
        cte22.COMMENTS
 from cte22 cte22
 inner join cec e1
on e1.inventory_item_id = cte22.inventory_item_id
AND e1.organization_id = cte22.organization_id1
inner join cec_max_date2 e2
on e2.inventory_item_id = cte22.inventory_item_id
AND e2.organization_id = cte22.organization_id1
and e2.wip_entity_id = cte22.wip_entity_id
where e1.creation_date = e2.max_creation_date2
)
)
);
commit;
--------------------------------------------------------------------
--------------------------------------------------------------------
delete from bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG
where (dw_load_id,rownumber)
in 
(select ods.dw_load_id,ods.rownumber from bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG_INCR ods,
bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG dw 
where 1=1
and dw.dw_load_id = ods.dw_load_id and dw.rownumber = ods.rownumber);
commit;
---------------------------------------------------------------------------
----------------------------------------------------------------------------
insert into bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG
select * from bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG_INCR;
commit;
---------------------------------------------------------------------------
----------------------------------------------------------------------------
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'fact_wo_value_variance_union6_stg' and batch_name = 'wip';

COMMIT;