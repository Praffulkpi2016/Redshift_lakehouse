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
	drop  table if exists bec_dwh.FACT_RAPA_STG5;
	create table bec_dwh.FACT_RAPA_STG5 diststyle all sortkey(
	inventory_item_id,organization_id ) as ( 
		select 
		inventory_item_id,
		organization_id,
		cost_type,
		plan_name,
		data_type,
		order_group,
		order_type_text,
		po_number,
		--c.organization_id,
		--organization_code,
		consumption_date1,
		aging_period,
		consumption_date,
		item,
		description,
		planning_make_buy_code,
		NULL as category_name,
		consumption_quantity,
		NULL::numeric(38, 10) as primary_quantity,
		unit_price,
		uom_code,
		consumption_value,
		po_line_type,
		vendor_name,
		material_cost,
		extended_cost,
		consigned_ppv,
		source_organization_id,
		planner_code,
		buyer_name,
		primary_unit_of_measure,
		release_num,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id as organization_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || source_organization_id as source_organization_id_key,
		-- audit columns
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
			|| '-' || nvl(inventory_item_id, 0)   || '-' || nvl(organization_id, 0)
		as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
		from
		(
			SELECT 
			msi.inventory_item_id,
			c.organization_id,
			'Pending'               cost_type,
			NULL                    plan_name,
			'Receipt Actual'        data_type,
			'CVMI Consumption'      order_group,
			'CVMI Consumption'      order_type_text,
			po_number,
			--c.organization_id,
			--organization_code,
			trunc(consumption_date) consumption_date1,
			0                       aging_period,
			trunc(consumption_date) consumption_date,
			c.item,
			c.description,
			NULL                    planning_make_buy_code,
			-- NULL                    category_name,
			consumption_quantity,
			NULL,
			unit_price,
			primary_unit_of_measure uom_code,
			consumption_value,
			NULL                    po_line_type,
			vendor_name,
			material_cost,
			extended_cost,
			consigned_ppv,
			NULL::Numeric(15,0)                    source_organization_id,
			planner_code,
			agent_name buyer_name,
			primary_unit_of_measure                    ,
			c.release_num
			FROM
			 (select * from  bec_ods.bec_cvmi_consumption_view where is_deleted_flg <> 'Y') c
			,(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi
			,(select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh
			,(select * from bec_ods.po_agents_v   where is_deleted_flg <> 'Y') poa
			where  c.item  = msi.segment1
			AND c.organization_id = msi.organization_id
			AND c.po_number = poh.segment1(+)
			AND poh.agent_id = poa.agent_id(+)
			
			UNION ALL 
			
			SELECT 
			msi.inventory_item_id,
			c.organization_id,
			'Frozen'                cost_type,
			NULL                    plan_name,
			'Receipt Actual'        data_type,
			'CVMI Consumption'      order_group,
			'CVMI Consumption'      order_type_text,
			po_number,
			-- c.organization_id,
			-- organization_code,
			trunc(consumption_date) consumption_date1,
			0                       aging_period,
			trunc(consumption_date) consumption_date,
			c.item,
			c.description,
			NULL                    planning_make_buy_code,
			--NULL                    category_name,
			consumption_quantity,
			NULL,
			unit_price,
			primary_unit_of_measure uom_code,
			consumption_value,
			NULL                    po_line_type,
			vendor_name,
			material_cost,
			extended_cost,
			consigned_ppv,
			NULL::Numeric(15,0)                    source_organization_id,
			planner_code,
			agent_name buyer_name,
			msi.primary_unit_of_measure                    ,
			c.release_num
			FROM
			(select * from  bec_ods.bec_cvmi_consumption_view where is_deleted_flg <> 'Y') c
			,(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi
			,(select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh
			,(select * from bec_ods.po_agents_v where is_deleted_flg <> 'Y')   poa
			where  c.item  = msi.segment1
			AND c.organization_id = msi.organization_id
			AND c.po_number = poh.segment1(+)
			AND poh.agent_id = poa.agent_id(+)    
			
		)		
		
			);
			
			END;
			update 
			bec_etl_ctrl.batch_dw_info 
			set 
			load_type = 'I', 
			last_refresh_date = getdate() 
			where 
			dw_table_name = 'fact_rapa_stg5' 
			and batch_name = 'ascp';
			commit;
						