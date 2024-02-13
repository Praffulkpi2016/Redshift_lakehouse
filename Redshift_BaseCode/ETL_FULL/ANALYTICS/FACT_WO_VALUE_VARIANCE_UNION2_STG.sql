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

drop table if exists bec_dwh.FACT_WO_VALUE_VARIANCE_UNION2_STG;

create table bec_dwh.FACT_WO_VALUE_VARIANCE_UNION2_STG diststyle all 
sortkey(organization_id,primary_item_id,wip_entity_id)
as
------------------------------------------------------------------------------------
with mta as (
	select
		transaction_id,
		base_transaction_value,
		accounting_line_type,
		cost_element_id
	from
		(select * from bec_ods.MTL_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y')
	where
		accounting_line_type = 7
		),
------------------------------------------------------------------------------------		
mmt as (
	select
		transaction_id,
		transaction_source_type_id,
		transaction_source_id,
		inventory_item_id
	from
		(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y')
	where
		transaction_source_type_id = 5),
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
),
------------------------------------------------------------------------------------
we as (
select organization_id,
primary_item_id,
wip_entity_id,
wip_entity_name,
description
from (select * from bec_ods.wip_entities where is_deleted_flg <> 'Y')
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
and c2.standard_cost_revision_date <= NVL (d.date_closed,(SYSDATE))
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
cec_max_date1 as (
SELECT MAX(e2.creation_date) as max_creation_date1,e2.inventory_item_id,e2.organization_id,d.wip_entity_id
FROM cec e2
inner join wdj d
on nvl(e2.inventory_item_id,0) = nvl(d.primary_item_id,0)
and nvl(e2.organization_id,0) = nvl(d.organization_id,0)
and e2.creation_date <= NVL (d.date_closed,(SYSDATE))
group by e2.inventory_item_id,e2.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte8 as (
select 
	c1.inventory_item_id,
	c1.organization_id,
	c1.cost_update_id,
	d.wip_entity_id,
	c1.standard_cost	
from
	csc c1
inner join wdj d
	on  c1.inventory_item_id = d.primary_item_id
	and c1.organization_id = d.organization_id
    and c1.standard_cost_revision_date <= NVL (d.date_closed,(SYSDATE))
	and c1.cost_update_id IN
(select
	max_cost_update_id1
from
	csc_max_id1 
	where inventory_item_id = c1.inventory_item_id
	and organization_id = c1.organization_id
	--and wip_entity_id = d.wip_entity_id
	)
),	
------------------------------------------------------------------------------------
cte9 as (
SELECT SUM (e1.standard_cost) as sum_standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
--WHERE cost_element_id = 1
) e1
inner join wdj d
on e1.inventory_item_id = d.primary_item_id
AND e1.organization_id = d.organization_id
AND e1.creation_date IN
(SELECT max_creation_date1
FROM cec_max_date1 
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id
)
group by e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),	
------------------------------------------------------------------------------------
cte10 as (
SELECT SUM (e1.standard_cost) as sum_standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 1
) e1
inner join wdj d
on e1.inventory_item_id = d.primary_item_id
AND e1.organization_id = d.organization_id
AND e1.creation_date IN
(SELECT max_creation_date1
FROM cec_max_date1
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte11 as (
SELECT SUM (e1.standard_cost) as sum_standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 2
) e1
inner join wdj d
on e1.inventory_item_id = d.primary_item_id
AND e1.organization_id = d.organization_id
AND e1.creation_date IN
(SELECT max_creation_date1
FROM cec_max_date1
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte12 as (
SELECT SUM (e1.standard_cost) as sum_standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 3
) e1
inner join wdj d
on e1.inventory_item_id = d.primary_item_id
AND e1.organization_id = d.organization_id
AND e1.creation_date IN
(SELECT max_creation_date1
FROM cec_max_date1
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),
------------------------------------------------------------------------------------
cte13 as (
SELECT SUM (e1.standard_cost) as sum_standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 4
) e1
inner join wdj d
on e1.inventory_item_id = d.primary_item_id
AND e1.organization_id = d.organization_id
AND e1.creation_date IN
(SELECT max_creation_date1
FROM cec_max_date1
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by e1.inventory_item_id,e1.organization_id,d.wip_entity_id
),	
------------------------------------------------------------------------------------
cte14 as (
SELECT SUM (e1.standard_cost) as sum_standard_cost,e1.inventory_item_id,e1.organization_id,d.wip_entity_id
FROM (select inventory_item_id,organization_id,creation_date,standard_cost
from cec
WHERE cost_element_id = 5
) e1
inner join wdj d
on e1.inventory_item_id = d.primary_item_id
AND e1.organization_id = d.organization_id
AND e1.creation_date IN
(SELECT max_creation_date1
FROM cec_max_date1
where inventory_item_id = e1.inventory_item_id
and organization_id = e1.organization_id
and wip_entity_id = d.wip_entity_id)
group by e1.inventory_item_id,e1.organization_id,d.wip_entity_id
)
------------------------------------------------------------------------------------
select  organization_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where
		source_system = 'EBS')|| '-' || organization_id as organization_id_key,
		primary_item_id,
		cast(inventory_item_id as int) as inventory_item_id,
		(select system_id from bec_etl_ctrl.etlsourceappid where
		source_system = 'EBS')|| '-' || nvl(cast(inventory_item_id as int),0) as inventory_item_id_key,
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
        cast(quantity_per_assembly as int) as quantity_per_assembly,
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
	|| '-' || nvl(cast(inventory_item_id as int), 0) 
	|| '-' || nvl(wip_entity_id, 0) 
	|| '-' || nvl(cast(operation_seq_num as int), 0) 
	|| '-' || nvl(cast(res_op_seq_num as int), 0) 
	|| '-' || nvl(cast(resource_seq_num as int), 0)
	|| '-' || nvl(WIP_Supply_Type, 'NA')
	|| '-' || nvl(ALTERNATE_BOM, 'NA')
	|| '-' || nvl(comments, 'NA') as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
 from (
SELECT 
						 e.organization_id,
						 e.primary_item_id,
						 null as inventory_item_id,
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
                         WHERE organization_id = d.organization_id
                               AND inventory_item_id = d.primary_item_id)
                          item_number,
                       (SELECT description
                          FROM (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')
                         WHERE organization_id = d.organization_id
                               AND inventory_item_id = d.primary_item_id)
                          item_description,
                       NULL operation_seq_num,
                       NULL quantity_per_assembly,
                       NULL required_quantity,
                       NULL quantity_issued,
                       NULL WIP_Supply_Type,
                       NULL department_code,
                       NULL res_op_seq_num,
                       NULL resource_seq_num,
                       NULL resource_code,
                       NULL res_uom_code,
                       NULL Res_usage_rate,
                       NULL applied_resource_units,
                       --y.transaction_type_name,
                       (SELECT SUM (base_transaction_value)
                          FROM mta a,
                               mmt t
                         WHERE     a.transaction_id = t.transaction_id
                               AND t.transaction_source_id = e.wip_entity_id
                               AND t.inventory_item_id = d.primary_item_id
                               --AND A.COST_ELEMENT_ID = 1
                               )
                          item_transaction_value,
                       ( (CASE
                             WHEN 
                             cte8.standard_cost = 0
                             THEN
                                0
                             else
                             cte9.sum_standard_cost
                          END)
                        * d.quantity_completed)
                          item_cost_at_close,
                       CASE
                          when cte8.standard_cost = 0
                          THEN
                             0
                          else
                          cte9.sum_standard_cost
                       END
                          unit_item_cost_at_close,
                          (SELECT SUM (base_transaction_value)
                          FROM mta a,
                               mmt t
                         WHERE     a.transaction_id = t.transaction_id
                               AND t.transaction_source_id = e.wip_entity_id
                               AND t.inventory_item_id = d.primary_item_id
                               AND A.COST_ELEMENT_ID = 1
                               ) mtl_transaction_value,
                       ( (CASE
                             WHEN cte8.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte10.sum_standard_cost
                          END)
                        * d.quantity_completed)
                          mtl_cost_at_close,
                       CASE
                          WHEN cte8.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte10.sum_standard_cost
                       END
                          unit_mtl_cost_at_close,
                          (SELECT SUM (base_transaction_value)
                          FROM mta a,
                               mmt t
                         WHERE     a.transaction_id = t.transaction_id
                               AND t.transaction_source_id = e.wip_entity_id
                               AND t.inventory_item_id = d.primary_item_id
                               AND A.COST_ELEMENT_ID = 2
                               ) mtl_oh_transaction_value,
                       ( (CASE
                             WHEN cte8.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte11.sum_standard_cost
                          END)
                        * d.quantity_completed)
                          mtl_oh_cost_at_close,
                       CASE
                          WHEN cte8.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte11.sum_standard_cost
                       END
                          unit_mtl_oh_cost_at_close,
                          (SELECT SUM (base_transaction_value)
                          FROM mta a,
                               mmt t
                         WHERE     a.transaction_id = t.transaction_id
                               AND t.transaction_source_id = e.wip_entity_id
                               AND t.inventory_item_id = d.primary_item_id
                               AND A.COST_ELEMENT_ID = 3
                               ) res_transaction_value,
                       ( (CASE
                             WHEN cte8.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte12.sum_standard_cost
                          END)
                        * d.quantity_completed)
                          res_cost_at_close,
                       CASE
                          WHEN cte8.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte12.sum_standard_cost
                       END
                          unit_res_cost_at_close,
                          (SELECT SUM (base_transaction_value)
                          FROM mta a,
                               mmt t
                         WHERE     a.transaction_id = t.transaction_id
                               AND t.transaction_source_id = e.wip_entity_id
                               AND t.inventory_item_id = d.primary_item_id
                               AND A.COST_ELEMENT_ID = 4
                               ) osp_transaction_value,
                       ( (CASE
                             WHEN cte8.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte13.sum_standard_cost
                          END)
                        * d.quantity_completed)
                          osp_cost_at_close,
                       CASE
                          WHEN cte8.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte13.sum_standard_cost
                       END
                          unit_osp_cost_at_close,
                          (SELECT SUM (base_transaction_value)
                          FROM mta a,
                               mmt t
                         WHERE     a.transaction_id = t.transaction_id
                               AND t.transaction_source_id = e.wip_entity_id
                               AND t.inventory_item_id = d.primary_item_id
                               AND A.COST_ELEMENT_ID = 5
                               ) oh_transaction_value,
                       ( (CASE
                             WHEN cte8.standard_cost = 0
                             THEN
                                0
                             ELSE
                                cte14.sum_standard_cost
                          END)
                        * d.quantity_completed)
                          oh_cost_at_close,
                       CASE
                          WHEN cte8.standard_cost = 0
                          THEN
                             0
                          ELSE
                             cte14.sum_standard_cost
                       END
                          unit_oh_cost_at_close,
                       NULL cost_update_txn_value,
                       NULL Mtl_variance,
                       NULL mtl_oh_variance,
                       NULL Res_variance,
                       NULL osp_variance,
                       NULL oh_variance,
                       NULL net_value,
                       d.organization_id as organization_id1,
                       NULL ALTERNATE_BOM,
                       'Work Order Assembly' COMMENTS
                  FROM 
				 we e
				 inner join  wdj d
                 ON d.wip_entity_id = e.wip_entity_id
				 left outer join cte8
				 ON e.primary_item_id = cte8.inventory_item_id
				 and e.organization_id = cte8.organization_id
				 and e.wip_entity_id = cte8.wip_entity_id
				 left outer join cte9
				 ON e.organization_id = cte9.organization_id
			     and e.primary_item_id = cte9.inventory_item_id
			     and e.wip_entity_id = cte9.wip_entity_id
				 left outer join cte10
				 ON d.organization_id = cte10.organization_id
			     and d.primary_item_id = cte10.inventory_item_id
			     and e.wip_entity_id = cte10.wip_entity_id
				 left outer join cte11
				 ON d.organization_id = cte11.organization_id
			     and d.primary_item_id = cte11.inventory_item_id
			     and e.wip_entity_id = cte11.wip_entity_id
				 left outer join cte12
				 ON d.organization_id = cte12.organization_id
			     and d.primary_item_id = cte12.inventory_item_id
			     and e.wip_entity_id = cte12.wip_entity_id
				 left outer join cte13
				 ON d.organization_id = cte13.organization_id
			     and d.primary_item_id = cte13.inventory_item_id
			     and e.wip_entity_id = cte13.wip_entity_id
				 left outer join cte14
				 ON d.organization_id = cte14.organization_id
			     and d.primary_item_id = cte14.inventory_item_id
			     and e.wip_entity_id = cte14.wip_entity_id
);
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'fact_wo_value_variance_union2_stg' and batch_name = 'wip';

COMMIT;		     