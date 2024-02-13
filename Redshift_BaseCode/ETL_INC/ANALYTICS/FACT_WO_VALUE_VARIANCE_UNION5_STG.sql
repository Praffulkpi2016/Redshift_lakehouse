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
delete from bec_dwh.FACT_WO_VALUE_VARIANCE_UNION5_STG
where 
(nvl(organization_id, 0)
,nvl(cast(inventory_item_id as int), 0) 
,nvl(wip_entity_id, 0) 
,nvl(cast(operation_seq_num as int), 0) 
,nvl(cast(res_op_seq_num as int), 0) 
,nvl(cast(resource_seq_num as int), 0)
,nvl(WIP_Supply_Type, 'NA')
,nvl(ALTERNATE_BOM, 'NA')
,nvl(comments, 'NA')
)
IN
(
select 
nvl(ods.organization_id, 0)
,nvl(cast(ods.inventory_item_id as int), 0) 
,nvl(ods.wip_entity_id, 0) 
,nvl(cast(ods.operation_seq_num as int), 0) 
,nvl(cast(ods.res_op_seq_num as int), 0) 
,nvl(cast(ods.resource_seq_num as int), 0)
,nvl(ods.WIP_Supply_Type, 'NA')
,nvl(ods.ALTERNATE_BOM, 'NA')
,nvl(ods.comments, 'NA')
from bec_dwh.FACT_WO_VALUE_VARIANCE_UNION5_STG dw, 
(
select  organization_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where
		source_system = 'EBS')|| '-' || organization_id as organization_id_key,
		primary_item_id,
		cast(inventory_item_id as int),
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
		start_quantity,
        quantity_completed,
        quantity_scrapped,
		date_released,
        date_completed,
        date_closed,
        JOB_START_DATE,
        class_code,
		cast(item_number as varchar) as item_number,
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
		cast(item_transaction_value as int) as item_transaction_value,
		cast(item_cost_at_close as int) as item_cost_at_close,
		cast(unit_item_cost_at_close as int) as unit_item_cost_at_close,
		cast(mtl_transaction_value as int) as mtl_transaction_value,
		cast(mtl_cost_at_close as int) as mtl_cost_at_close,
		cast(unit_mtl_cost_at_close as int) as unit_mtl_cost_at_close,
		cast(mtl_oh_transaction_value as int) as mtl_oh_transaction_value,
		cast(mtl_oh_cost_at_close as int) as mtl_oh_cost_at_close,
		cast(unit_mtl_oh_cost_at_close as int) as unit_mtl_oh_cost_at_close,
		cast(res_transaction_value as int) as res_transaction_value,
		cast(res_cost_at_close as int) as res_cost_at_close,
		cast(unit_res_cost_at_close as int) as unit_res_cost_at_close,
		cast(osp_transaction_value as int) as osp_transaction_value,
		cast(osp_cost_at_close as int) as osp_cost_at_close,
		cast(unit_osp_cost_at_close as int) as unit_osp_cost_at_close,
		cast(oh_transaction_value as int) as oh_transaction_value,
		cast(oh_cost_at_close as int) as oh_cost_at_close,
		cast(unit_oh_cost_at_close as int) unit_oh_cost_at_close,
		cast(cost_update_txn_value as int) as cost_update_txn_value,
        Mtl_variance,
        mtl_oh_variance,
        Res_variance,
        osp_variance,
        oh_variance,
        net_value,
        organization_id1,
        ALTERNATE_BOM,
        COMMENTS,
		decode(comments, 'Work Order Components', 
		(nvl(cast(mtl_transaction_value as int),0) - nvl(cast(item_cost_at_close as int),0 ))) mtl_value_difference,		
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
 from
(
SELECT  e.organization_id,
	 e.primary_item_id,
	 null as inventory_item_id,
	 e.wip_entity_id,
    e.wip_entity_name work_order,
    e.description wo_description,
    (SELECT segment1
       FROM bec_ods.mtl_system_items_b 
      WHERE organization_id = e.organization_id
            AND inventory_item_id = e.primary_item_id)
       assembly_item,
    (SELECT description
       FROM bec_ods.mtl_system_items_b 
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
    d.start_quantity,
    d.quantity_completed,
    d.quantity_scrapped,
    d.date_released,
    d.date_completed,
    d.date_closed,
	d.scheduled_start_date JOB_START_DATE,
    d.class_code,
    NULL item_number,
    NULL item_description,
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
    NULL item_transaction_value,
    NULL item_cost_at_close,
    NULL unit_item_cost_at_close,
    NULL mtl_transaction_value,
    NULL mtl_cost_at_close,
    NULL unit_mtl_cost_at_close,
    NULL mtl_oh_transaction_value,
    NULL mtl_oh_cost_at_close,
    NULL unit_mtl_oh_cost_at_close,
    NULL res_transaction_value,
    NULL res_cost_at_close,
    NULL unit_res_cost_at_close,
    NULL osp_transaction_value,
    NULL osp_cost_at_close,
    NULL unit_osp_cost_at_close,
    NULL oh_transaction_value,
    NULL oh_cost_at_close,
    NULL unit_oh_cost_at_close,
    NULL cost_update_txn_value,
    (SELECT SUM (base_transaction_value)
       FROM bec_ods.WIP_TRANSACTION_ACCOUNTS  a,
            bec_ods.wip_transactions  t
      WHERE     a.transaction_id = t.transaction_id
            AND t.wip_entity_id = e.wip_entity_id
            AND t.transaction_type = 6
            AND A.COST_ELEMENT_ID = 1
            AND a.accounting_line_type IN (7, 8))
       mtl_variance,
      (SELECT SUM (base_transaction_value)
       FROM bec_ods.WIP_TRANSACTION_ACCOUNTS  a,
            bec_ods.wip_transactions  t
      WHERE     a.transaction_id = t.transaction_id
            AND t.wip_entity_id = e.wip_entity_id
            AND t.transaction_type = 6
            AND A.COST_ELEMENT_ID = 2
            AND a.accounting_line_type IN (7, 8))
       mtl_oh_variance,
    (SELECT SUM (base_transaction_value)
       FROM bec_ods.WIP_TRANSACTION_ACCOUNTS  a,
            bec_ods.wip_transactions  t
      WHERE     a.transaction_id = t.transaction_id
            AND t.wip_entity_id = e.wip_entity_id
            AND t.transaction_type = 6
            AND A.COST_ELEMENT_ID = 3
            AND a.accounting_line_type IN (7, 8))
       res_variance,
    (SELECT SUM (base_transaction_value)
       FROM bec_ods.WIP_TRANSACTION_ACCOUNTS  a,
            bec_ods.wip_transactions  t
      WHERE     a.transaction_id = t.transaction_id
            AND t.wip_entity_id = e.wip_entity_id
            AND t.transaction_type = 6
            AND A.COST_ELEMENT_ID = 4
            AND a.accounting_line_type IN (7, 8))
       osp_variance,
    (SELECT SUM (base_transaction_value)
       FROM bec_ods.WIP_TRANSACTION_ACCOUNTS  a,
            bec_ods.wip_transactions  t
      WHERE     a.transaction_id = t.transaction_id
            AND t.wip_entity_id = e.wip_entity_id
            AND t.transaction_type = 6
            AND A.COST_ELEMENT_ID = 5
            AND a.accounting_line_type IN (7, 8))
       oh_variance,
    (SELECT SUM (base_transaction_value)
       FROM bec_ods.WIP_TRANSACTION_ACCOUNTS  a,
            bec_ods.wip_transactions  t
      WHERE     a.transaction_id = t.transaction_id
            AND t.wip_entity_id = e.wip_entity_id
            AND t.transaction_type = 6
            AND NVL (A.COST_ELEMENT_ID, -999) = -999
            AND a.accounting_line_type IN (7, 8))
       net_value, 
    d.organization_id as organization_id1,
    NULL ALTERNATE_BOM,
    'Work Order Variance' COMMENTS
                   FROM bec_ods.wip_entities  e,
   				        bec_ods.wip_discrete_jobs  d
                  WHERE d.wip_entity_id = e.wip_entity_id
		and (e.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
												where dw_table_name = 'fact_wo_value_variance_union5_stg'
												and batch_name = 'wip')
					   or d.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
												where dw_table_name = 'fact_wo_value_variance_union5_stg'
												and batch_name = 'wip') 
												OR e.IS_DELETED_FLG = 'Y'
												OR d.IS_DELETED_FLG = 'Y'			)
)
)ods
where 1=1
and dw.dw_load_id 
= 		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS'
    )
    || '-' || nvl(ods.organization_id, 0)
	|| '-' || nvl(cast(ods.inventory_item_id as int), 0) 
	|| '-' || nvl(ods.wip_entity_id, 0) 
	|| '-' || nvl(cast(ods.operation_seq_num as int), 0) 
	|| '-' || nvl(cast(ods.res_op_seq_num as int), 0) 
	|| '-' || nvl(cast(ods.resource_seq_num as int), 0)
	|| '-' || nvl(ods.WIP_Supply_Type, 'NA')
	|| '-' || nvl(ods.ALTERNATE_BOM, 'NA')
	|| '-' || nvl(ods.comments, 'NA')
);
commit;
----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
insert into bec_dwh.fact_wo_value_variance_union5_stg
(
organization_id
,organization_id_key
,primary_item_id
,inventory_item_id
,inventory_item_id_key
,wip_entity_id
,wip_entity_id_key
,work_order
,wo_description
,assembly_item
,assembly_description
,status_type
,start_quantity
,quantity_completed
,quantity_scrapped
,date_released
,date_completed
,date_closed
,job_start_date
,class_code
,item_number
,item_description
,operation_seq_num
,quantity_per_assembly
,required_quantity
,quantity_issued
,wip_supply_type
,department_code
,res_op_seq_num
,resource_seq_num
,resource_code
,res_uom_code
,res_usage_rate
,applied_resource_units
,item_transaction_value
,item_cost_at_close
,unit_item_cost_at_close
,mtl_transaction_value
,mtl_cost_at_close
,unit_mtl_cost_at_close
,mtl_oh_transaction_value
,mtl_oh_cost_at_close
,unit_mtl_oh_cost_at_close
,res_transaction_value
,res_cost_at_close
,unit_res_cost_at_close
,osp_transaction_value
,osp_cost_at_close
,unit_osp_cost_at_close
,oh_transaction_value
,oh_cost_at_close
,unit_oh_cost_at_close
,cost_update_txn_value
,mtl_variance
,mtl_oh_variance
,res_variance
,osp_variance
,oh_variance
,net_value
,organization_id1
,alternate_bom
,comments
,mtl_value_difference
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
select  organization_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where
		source_system = 'EBS')|| '-' || organization_id as organization_id_key,
		primary_item_id,
		cast(inventory_item_id as int),
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
		start_quantity,
        quantity_completed,
        quantity_scrapped,
		date_released,
        date_completed,
        date_closed,
        JOB_START_DATE,
        class_code,
		cast(item_number as varchar) as item_number,
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
		cast(item_transaction_value as int) as item_transaction_value,
		cast(item_cost_at_close as int) as item_cost_at_close,
		cast(unit_item_cost_at_close as int) as unit_item_cost_at_close,
		cast(mtl_transaction_value as int) as mtl_transaction_value,
		cast(mtl_cost_at_close as int) as mtl_cost_at_close,
		cast(unit_mtl_cost_at_close as int) as unit_mtl_cost_at_close,
		cast(mtl_oh_transaction_value as int) as mtl_oh_transaction_value,
		cast(mtl_oh_cost_at_close as int) as mtl_oh_cost_at_close,
		cast(unit_mtl_oh_cost_at_close as int) as unit_mtl_oh_cost_at_close,
		cast(res_transaction_value as int) as res_transaction_value,
		cast(res_cost_at_close as int) as res_cost_at_close,
		cast(unit_res_cost_at_close as int) as unit_res_cost_at_close,
		cast(osp_transaction_value as int) as osp_transaction_value,
		cast(osp_cost_at_close as int) as osp_cost_at_close,
		cast(unit_osp_cost_at_close as int) as unit_osp_cost_at_close,
		cast(oh_transaction_value as int) as oh_transaction_value,
		cast(oh_cost_at_close as int) as oh_cost_at_close,
		cast(unit_oh_cost_at_close as int) unit_oh_cost_at_close,
		cast(cost_update_txn_value as int) as cost_update_txn_value,
        Mtl_variance,
        mtl_oh_variance,
        Res_variance,
        osp_variance,
        oh_variance,
        net_value,
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
 from
(
SELECT  e.organization_id,
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
                        d.start_quantity,
                        d.quantity_completed,
                        d.quantity_scrapped,
                        d.date_released,
                        d.date_completed,
                        d.date_closed,
						d.scheduled_start_date JOB_START_DATE,
                        d.class_code,
                        NULL item_number,
                        NULL item_description,
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
                        NULL item_transaction_value,
                        NULL item_cost_at_close,
                        NULL unit_item_cost_at_close,
                        NULL mtl_transaction_value,
                        NULL mtl_cost_at_close,
                        NULL unit_mtl_cost_at_close,
                        NULL mtl_oh_transaction_value,
                        NULL mtl_oh_cost_at_close,
                        NULL unit_mtl_oh_cost_at_close,
                        NULL res_transaction_value,
                        NULL res_cost_at_close,
                        NULL unit_res_cost_at_close,
                        NULL osp_transaction_value,
                        NULL osp_cost_at_close,
                        NULL unit_osp_cost_at_close,
                        NULL oh_transaction_value,
                        NULL oh_cost_at_close,
                        NULL unit_oh_cost_at_close,
                        NULL cost_update_txn_value,
                        (SELECT SUM (base_transaction_value)
                           FROM (select * from bec_ods.WIP_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                (select * from bec_ods.wip_transactions where is_deleted_flg <> 'Y') t
                          WHERE     a.transaction_id = t.transaction_id
                                AND t.wip_entity_id = e.wip_entity_id
                                AND t.transaction_type = 6
                                AND A.COST_ELEMENT_ID = 1
                                AND a.accounting_line_type IN (7, 8))
                           mtl_variance,
                          (SELECT SUM (base_transaction_value)
                           FROM (select * from bec_ods.WIP_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                (select * from bec_ods.wip_transactions where is_deleted_flg <> 'Y') t
                          WHERE     a.transaction_id = t.transaction_id
                                AND t.wip_entity_id = e.wip_entity_id
                                AND t.transaction_type = 6
                                AND A.COST_ELEMENT_ID = 2
                                AND a.accounting_line_type IN (7, 8))
                           mtl_oh_variance,
                        (SELECT SUM (base_transaction_value)
                           FROM (select * from bec_ods.WIP_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                (select * from bec_ods.wip_transactions where is_deleted_flg <> 'Y') t
                          WHERE     a.transaction_id = t.transaction_id
                                AND t.wip_entity_id = e.wip_entity_id
                                AND t.transaction_type = 6
                                AND A.COST_ELEMENT_ID = 3
                                AND a.accounting_line_type IN (7, 8))
                           res_variance,
                        (SELECT SUM (base_transaction_value)
                           FROM (select * from bec_ods.WIP_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                (select * from bec_ods.wip_transactions where is_deleted_flg <> 'Y') t
                          WHERE     a.transaction_id = t.transaction_id
                                AND t.wip_entity_id = e.wip_entity_id
                                AND t.transaction_type = 6
                                AND A.COST_ELEMENT_ID = 4
                                AND a.accounting_line_type IN (7, 8))
                           osp_variance,
                        (SELECT SUM (base_transaction_value)
                           FROM (select * from bec_ods.WIP_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                (select * from bec_ods.wip_transactions where is_deleted_flg <> 'Y') t
                          WHERE     a.transaction_id = t.transaction_id
                                AND t.wip_entity_id = e.wip_entity_id
                                AND t.transaction_type = 6
                                AND A.COST_ELEMENT_ID = 5
                                AND a.accounting_line_type IN (7, 8))
                           oh_variance,
                        (SELECT SUM (base_transaction_value)
                           FROM (select * from bec_ods.WIP_TRANSACTION_ACCOUNTS where is_deleted_flg <> 'Y') a,
                                (select * from bec_ods.wip_transactions where is_deleted_flg <> 'Y') t
                          WHERE     a.transaction_id = t.transaction_id
                                AND t.wip_entity_id = e.wip_entity_id
                                AND t.transaction_type = 6
                                AND NVL (A.COST_ELEMENT_ID, -999) = -999
                                AND a.accounting_line_type IN (7, 8))
                           net_value, 
                        d.organization_id as organization_id1,
                        NULL ALTERNATE_BOM,
                        'Work Order Variance' COMMENTS
                   FROM (select * from bec_ods.wip_entities where is_deleted_flg <> 'Y') e,
   				        (select * from bec_ods.wip_discrete_jobs where is_deleted_flg <> 'Y') d
                  WHERE d.wip_entity_id = e.wip_entity_id
		and (e.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
												where dw_table_name = 'fact_wo_value_variance_union5_stg'
												and batch_name = 'wip')
					   or d.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
												where dw_table_name = 'fact_wo_value_variance_union5_stg'
												and batch_name = 'wip')
					)
)
);
commit;
end;
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'fact_wo_value_variance_union5_stg' and batch_name = 'wip';

COMMIT;