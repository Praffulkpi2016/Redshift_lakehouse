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
	drop  table if exists bec_dwh.FACT_RAPA_STG3;
	create table bec_dwh.FACT_RAPA_STG3 diststyle all sortkey(
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
		promised_date1,
		aging_period,
		promised_date,
		purchase_item,
		item_description,
		planning_make_buy_code,
		NULL as category_name,
 		po_open_qty,
		 primary_quantity,
		unit_price,
		primary_unit_of_measure,
		po_open_amount,
		po_line_type,
		vendor_name,
		mtl_cost,
		ext_mtl_cost,
		VARIANCE,
		source_organization_id,
		planner_code,
		buyer_name,
 		transactional_uom_code,
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
			SELECT inventory_item_id
			,ship_to_organization_id organization_id
			,cost_type
			,plan_name
			,data_type
			,order_group
			,order_type_text
			,po_number
			-- ,ship_to_organization_id
			,promised_date1
			,aging_period
			-- ,pd_cnt
			--,DATEDIFF(days,DATEADD(day, adj::integer  ,promised_date1),SYSDATE)
			,(CASE
				WHEN PD_CNT = 1 THEN
				DATEADD(day, adj::integer  ,promised_date1) 
				WHEN PD_CNT = 2 AND DATEDIFF(days,DATEADD(day, adj::integer  ,promised_date1),SYSDATE) >= 0 THEN
				SYSDATE
				WHEN PD_CNT = 2 AND DATEDIFF(days,DATEADD(day, adj::integer  ,promised_date1),SYSDATE) < 0 THEN 
					DATEADD(day, adj::integer  ,promised_date1)		 
				ELSE
					trunc(need_by_date)
				END
			) promised_date
			-- ,mod(postprocessing_lead_time,5)
			,purchase_item
			,item_description
			,planning_make_buy_code
			-- ,category
			,po_open_qty
			,( (nvl(ordered_quantity *conversion_rate,0)):: numeric(38, 10) -(nvl(quantity_received * conversion_rate,0))):: numeric(38, 10)  as primary_quantity
			,unit_price
			,primary_unit_of_measure
			,po_open_amount
			,po_line_type
			,vendor_name
			,mtl_cost
			,mtl_cost*( ( nvl(ordered_quantity *conversion_rate,0)) :: numeric(38, 10) - (nvl(quantity_received * conversion_rate,0)) :: numeric(38, 10)) as ext_mtl_cost
			,po_open_amount - (mtl_cost*( (nvl(ordered_quantity *conversion_rate,0)):: numeric(38, 10) - 
			(nvl(quantity_received * conversion_rate,0)) :: numeric(38, 10) )) as VARIANCE
			,source_organization_id
			,planner_code
			,buyer_name
			--,vendor_country_code
			,transactional_uom_code
			,release_num
			FROM (
				SELECT 
				msib.inventory_item_id, 
				'Frozen' cost_type,
				NULL  plan_name,
				'Receipt Forecast'  data_type,
				'Open PO'           order_group,
				(CASE
				 WHEN promised_date >= trunc(sysdate) THEN
				    'Open PO'
				 WHEN promised_date < trunc(sysdate) THEN
				    'Past Due POs'
				 WHEN promised_date IS NULL THEN
				    'Blank Promised Date POs'
				 END
				 ) order_type_text,
				--'Open PO'           order_type_text,
				po_number,
				ship_to_organization_id,
				--  Null              org_code,
				trunc(promised_date)                                                  promised_date1,
				need_by_date,
				0                                                                     aging_period,
				postprocessing_lead_time,
				trunc(postprocessing_lead_time/5) weeks,
				nvl((CASE
					WHEN (date_part(dayofweek,promised_date)  + mod(postprocessing_lead_time,5) >= 7) THEN
						((trunc(postprocessing_lead_time/5)*7) + 2 + mod(postprocessing_lead_time,5))::numeric
						else
						((trunc(postprocessing_lead_time/5)*7)  + mod(postprocessing_lead_time,5))::numeric
					END		 
				),0) as ADJ ,  
				(CASE
					WHEN (promised_date >= SYSDATE ) THEN
					1
					WHEN (promised_date < SYSDATE ) THEN
						2
						else
						3
					END		 
				) as pd_cnt ,
				--	5 days1,
				purchase_item,
				pov.item_description,
				NULL                                                                  planning_make_buy_code,
				Null category,
				( nvl(ordered_quantity, 0) - nvl(quantity_received, 0) )              po_open_qty,		
				ordered_quantity,
				quantity_received, 		
				(nvl((
					SELECT
					conversion_rate
					FROM
					bec_ods.mtl_uom_conversions muc
					WHERE is_deleted_flg <> 'Y' and 
                    unit_of_measure = pov.po_uom
				AND muc.inventory_item_id = msib.inventory_item_id
				), nvl((
					SELECT
					conversion_rate
					FROM
					bec_ods.mtl_uom_conversions
					WHERE is_deleted_flg <> 'Y' and
                    unit_of_measure = pov.po_uom
				AND inventory_item_id = 0
				), 1)) / nvl((
					SELECT
					conversion_rate
					FROM
					bec_ods.mtl_uom_conversions muc
					WHERE is_deleted_flg <> 'Y' and
                    unit_of_measure = pov.std_uom
				AND muc.inventory_item_id = msib.inventory_item_id
				), nvl((
					SELECT
					conversion_rate
					FROM
					bec_ods.mtl_uom_conversions muc
					WHERE is_deleted_flg <> 'Y' and
                    unit_of_measure = pov.std_uom
					AND muc.inventory_item_id = 0
				), 1))) as conversion_rate,		
				pov.unit_price,
				pov.std_uom     primary_unit_of_measure,
				( nvl(ordered_quantity, 0) - nvl(quantity_received, 0) ) * unit_price po_open_amount,
				po_line_type,
				null as vendor_name,
				(SELECT CIC.material_cost
				 FROM bec_ods.cst_item_costs  cic
				 WHERE cost_type_id = 1
		         AND cic.organization_id = msib.organization_id
		         AND cic.inventory_item_id = msib.inventory_item_id )    mtl_cost,
				NULL::Numeric(15,0)                                                                 source_organization_id,
				msib.planner_code                                                     planner_code,
				im_buyer buyer_name,
				pov.po_uom                                                            transactional_uom_code,
				release_num
				FROM
				bec_dwh.FACT_PO_SHIPMENT_DETAILS pov,
				(select * from  bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')     msib
				--(select * from  bec_ods.cst_item_costs     where is_deleted_flg <> 'Y')      cic,
				--(select * from  bec_ods.cst_cost_types     where is_deleted_flg <> 'Y')      ct
				WHERE
				authorization_status IN ( 'APPROVED', 'PRE-APPROVED', 'REQUIRES REAPPROVAL' )
				--    AND ship_to_organization_id IN ( 106, 245, 265, 285 )
				AND cvmi_flag = 'N'
				--AND promised_date >= trunc(sysdate)
				AND shipment_status IN ( 'OPEN', 'CLOSED FOR INVOICE' )
				AND purchase_item IS NOT NULL
				AND pov.purchase_item = msib.segment1
				AND msib.organization_id = pov.ship_to_organization_id
				/*AND cic.organization_id(+) = msib.organization_id
				AND cic.inventory_item_id(+) = msib.inventory_item_id
				AND cic.cost_type_id         = ct.cost_type_id(+)
				AND (ct.cost_type_id(+) = 1 AND ct.cost_type_id(+) = 3)*/
				/*AND PO_NUMBER in ( '10197714', -->=sysdate
					'10000092', --promisedate is NULL
					'004387')  --pd is < sysdate	
				*/
             UNION all
               SELECT 
				msib.inventory_item_id, 
				'Pending' cost_type,
				NULL  plan_name,
				'Receipt Forecast'  data_type,
				'Open PO'           order_group,
				(CASE
				 WHEN promised_date >= trunc(sysdate) THEN
				    'Open PO'
				 WHEN promised_date < trunc(sysdate) THEN
				    'Past Due POs'
				 WHEN promised_date IS NULL THEN
				    'Blank Promised Date POs'
				 END
				 ) order_type_text,
				--'Open PO'           order_type_text,
				po_number,
				ship_to_organization_id,
				--  Null              org_code,
				trunc(promised_date)                                                  promised_date1,
				need_by_date,
				0                                                                     aging_period,
				postprocessing_lead_time,
				trunc(postprocessing_lead_time/5) weeks,
				nvl((CASE
					WHEN (date_part(dayofweek,promised_date)  + mod(postprocessing_lead_time,5) >= 7) THEN
						((trunc(postprocessing_lead_time/5)*7) + 2 + mod(postprocessing_lead_time,5))::numeric
						else
						((trunc(postprocessing_lead_time/5)*7)  + mod(postprocessing_lead_time,5))::numeric
					END		 
				),0) as ADJ ,  
				(CASE
					WHEN (promised_date >= SYSDATE ) THEN
					1
					WHEN (promised_date < SYSDATE ) THEN
						2
						else
						3
					END		 
				) as pd_cnt ,
				--	5 days1,
				purchase_item,
				pov.item_description,
				NULL                                                                  planning_make_buy_code,
				Null category,
				( nvl(ordered_quantity, 0) - nvl(quantity_received, 0) )              po_open_qty,		
				ordered_quantity,
				quantity_received, 		
				(nvl((
					SELECT
					conversion_rate
					FROM
					bec_ods.mtl_uom_conversions muc
					WHERE is_deleted_flg <> 'Y' and 
                    unit_of_measure = pov.po_uom
				AND muc.inventory_item_id = msib.inventory_item_id
				), nvl((
					SELECT
					conversion_rate
					FROM
					bec_ods.mtl_uom_conversions
					WHERE is_deleted_flg <> 'Y' and
                    unit_of_measure = pov.po_uom
				AND inventory_item_id = 0
				), 1)) / nvl((
					SELECT
					conversion_rate
					FROM
					bec_ods.mtl_uom_conversions muc
					WHERE is_deleted_flg <> 'Y' and
                    unit_of_measure = pov.std_uom
				AND muc.inventory_item_id = msib.inventory_item_id
				), nvl((
					SELECT
					conversion_rate
					FROM
					bec_ods.mtl_uom_conversions muc
					WHERE is_deleted_flg <> 'Y' and
                    unit_of_measure = pov.std_uom
					AND muc.inventory_item_id = 0
				), 1))) as conversion_rate,		
				pov.unit_price,
				pov.std_uom     primary_unit_of_measure,
				( nvl(ordered_quantity, 0) - nvl(quantity_received, 0) ) * unit_price po_open_amount,
				po_line_type,
				null as vendor_name,
				(SELECT CIC.material_cost
				 FROM bec_ods.cst_item_costs  cic
				 WHERE cost_type_id = 1
		         AND cic.organization_id = msib.organization_id
		         AND cic.inventory_item_id = msib.inventory_item_id )    mtl_cost,
				NULL::Numeric(15,0)                                                                 source_organization_id,
				msib.planner_code                                                     planner_code,
				im_buyer buyer_name,
				pov.po_uom                                                            transactional_uom_code,
				release_num
				FROM
				bec_dwh.FACT_PO_SHIPMENT_DETAILS pov,
				(select * from  bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y')     msib
				--(select * from  bec_ods.cst_item_costs     where is_deleted_flg <> 'Y')      cic,
				--(select * from  bec_ods.cst_cost_types     where is_deleted_flg <> 'Y')      ct
				WHERE
				authorization_status IN ( 'APPROVED', 'PRE-APPROVED', 'REQUIRES REAPPROVAL' )
				--    AND ship_to_organization_id IN ( 106, 245, 265, 285 )
				AND cvmi_flag = 'N'
				--AND promised_date >= trunc(sysdate)
				AND shipment_status IN ( 'OPEN', 'CLOSED FOR INVOICE' )
				AND purchase_item IS NOT NULL
				AND pov.purchase_item = msib.segment1
				AND msib.organization_id = pov.ship_to_organization_id			 
			)		
		)
	);
	
END;
update 
bec_etl_ctrl.batch_dw_info 
set 
load_type = 'I', 
last_refresh_date = getdate() 
where 
dw_table_name = 'fact_rapa_stg3' 
and batch_name = 'ascp';
commit;