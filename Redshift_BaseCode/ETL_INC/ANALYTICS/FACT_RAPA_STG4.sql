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
	Truncate table bec_dwh.FACT_RAPA_STG4;
	Insert Into bec_dwh.FACT_RAPA_STG4
	( 
		select 
		inventory_item_id,
		organization_id,
		cost_type, 
		plan_name, 
		data_type, 
		order_group, 
		order_type_text, 
		order_number, 
		--organization_id, 
		--organization_code, 
		promised_date, 
		aging_period, 
		receipt_date, 
		part_number, 
		item_description, 
		planning_make_buy_code, 
		category as category_name, 
		rcv_quantity_received, 
		primary_quantity, 
		po_unit_price, 
		primary_unit_of_measure, 
		extended_po_rcv_price, 
		po_line_type, 
		vendor_name, 
		mtl_cost,
		ext_mtl_cost,
		variance,
		source_organization_id, 
		planner_code, 
		buyer_name, 
		-- vendor_country_code, 
		po_unit_of_measure, 
		po_release_number
		,(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_key,
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
			select * from ( with CTE_Main_STG as (
				select *,decode(
					po_unit_of_measure, 
					Decodest1,
					rcv_quantity_received, 
					Decodest2
				) ext_mtl_cost_cacl
				from (SELECT 
					'Frozen'::VARCHAR cost_type, 
					null::VARCHAR plan_name, 
					'Receipt Actual'::VARCHAR data_type, 
					'PO Receipt'::VARCHAR order_group, 
					'PO Receipt'::VARCHAR order_type_text, 
					po_number order_number, 
					r.organization_id, 
					organization_code, 
					trunc(promised_date) as promised_date, 
					0 aging_period, 
					receipt_date, 
					item_name part_number, 
					item_description, 
					'Buy'::VARCHAR planning_make_buy_code, 
					category, 
					msi.inventory_item_id,
					rcv_quantity_received, 
					rcv_quantity_received * (
						nvl(
							(
								SELECT 
								conversion_rate 
								FROM 
								bec_ods.mtl_uom_conversions muc 
								WHERE muc.is_deleted_flg <> 'Y'
										and
								muc.unit_of_measure = r.po_unit_of_measure 
								AND muc.inventory_item_id = msi.inventory_item_id
							), 
							nvl(
								(
									SELECT 
									conversion_rate 
									FROM 
									bec_ods.mtl_uom_conversions 
									WHERE is_deleted_flg <> 'Y'
										and
									unit_of_measure = r.po_unit_of_measure 
									AND inventory_item_id = 0
								), 
								1
							)
							) / nvl(
								(
									SELECT 
									conversion_rate 
									FROM 
									bec_ods.mtl_uom_conversions muc 
									WHERE muc.is_deleted_flg <> 'Y'
										and
									muc.unit_of_measure = r.primary_unit_of_measure 
									AND muc.inventory_item_id = msi.inventory_item_id
								), 
								nvl(
									(
										SELECT 
										conversion_rate 
										FROM 
										bec_ods.mtl_uom_conversions 
										WHERE is_deleted_flg <> 'Y'
										and unit_of_measure = r.primary_unit_of_measure 
										AND inventory_item_id = 0
									), 
									1
								)
							)
					) primary_quantity, 
					(
						SELECT 
						price_override 
						FROM 
						bec_ods.po_line_locations_all pll 
						WHERE pll.is_deleted_flg<>'Y'
						and
						pll.line_location_id = r.line_location_id
					) po_unit_price, 
					r.primary_unit_of_measure primary_unit_of_measure, 
					rcv_quantity_received * (
						SELECT 
						price_override 
						FROM 
						bec_ods.po_line_locations_all pll 
						WHERE pll.is_deleted_flg<>'Y'
						and pll.line_location_id = r.line_location_id
					) extended_po_rcv_price, 
					po_line_type, 
					vendor_name, 
					null::Numeric(15,0) source_organization_id, 
					msi.planner_code, 
					r.buyer_name, 
					r.po_unit_of_measure, 
					po_release_number,
					(
						SELECT 
						unit_of_measure 
						FROM 
						bec_ods.mtl_units_of_measure_tl 
						WHERE is_deleted_flg<>'Y'
						and uom_code = r.primary_uom_code
					) Decodest1,
					(
						SELECT 
						MAX(primary_quantity) 
						FROM 
						bec_ods.mtl_material_transactions mmt, 
						(select * from bec_ods.po_line_locations_all where is_deleted_flg<>'Y' )poll 
						WHERE 
						mmt.transaction_source_id = poll.po_header_id 
						AND poll.line_location_id = r.line_location_id 
						AND mmt.transaction_date = r.receipt_date 
						AND mmt.transaction_quantity = r.rcv_quantity_received
					) Decodest2,
					rcv_quantity_received * (
						SELECT 
						price_override 
						FROM 
						bec_ods.po_line_locations_all pll 
						WHERE pll.is_deleted_flg<>'Y'
						and pll.line_location_id = r.line_location_id
					) variance_cacl,
					row_number() OVER (PARTITION BY po_number) as ROWNUMBER 
					FROM 
					bec_ods.bec_actual_po_recpt1 r, 
					(select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y')msi 
					WHERE 
					r.item_name = msi.segment1 
					AND r.organization_id = msi.organization_id --    and receipt_date between '01-JUL-2019' and '01-JAN-2020'
					AND item_name IS NOT NULL --  and po_number = '10148551'
					AND r.cvmi_flag = 'N' 
					AND r.organization_id IN (106, 245, 265, 285)
					union all
					SELECT 
					'Pending'::VARCHAR cost_type, 
					null::VARCHAR plan_name, 
					'Receipt Actual'::VARCHAR data_type, 
					'PO Receipt'::VARCHAR order_group, 
					'PO Receipt'::VARCHAR order_type_text, 
					po_number order_number, 
					r.organization_id, 
					organization_code, 
					trunc(promised_date) as promised_date, 
					0 aging_period, 
					receipt_date, 
					item_name part_number, 
					item_description, 
					'Buy'::VARCHAR planning_make_buy_code, 
					category, 
					msi.inventory_item_id,
					rcv_quantity_received, 
					rcv_quantity_received * (
						nvl(
							(
								SELECT 
								conversion_rate 
								FROM 
								bec_ods.mtl_uom_conversions muc 
								WHERE muc.is_deleted_flg <> 'Y'
										and 
								muc.unit_of_measure = r.po_unit_of_measure 
								AND muc.inventory_item_id = msi.inventory_item_id
							), 
							nvl(
								(
									SELECT 
									conversion_rate 
									FROM 
									bec_ods.mtl_uom_conversions 
									WHERE is_deleted_flg <> 'Y'
										and 
									unit_of_measure = r.po_unit_of_measure 
									AND inventory_item_id = 0
								), 
								1
							)
							) / nvl(
								(
									SELECT 
									conversion_rate 
									FROM 
									bec_ods.mtl_uom_conversions muc 
									WHERE muc.is_deleted_flg <> 'Y'
										and 
									muc.unit_of_measure = r.primary_unit_of_measure 
									AND muc.inventory_item_id = msi.inventory_item_id
								), 
								nvl(
									(
										SELECT 
										conversion_rate 
										FROM 
										bec_ods.mtl_uom_conversions 
										WHERE is_deleted_flg <> 'Y'
										and unit_of_measure = r.primary_unit_of_measure 
										AND inventory_item_id = 0
									), 
									1
								)
							)
					) primary_quantity, 
					(
						SELECT 
						price_override 
						FROM 
						bec_ods.po_line_locations_all pll 
						WHERE pll.is_deleted_flg <> 'Y'
						and 
						pll.line_location_id = r.line_location_id
					) po_unit_price, 
					r.primary_unit_of_measure primary_unit_of_measure, 
					rcv_quantity_received * (
						SELECT 
						price_override 
						FROM 
						bec_ods.po_line_locations_all pll 
						WHERE pll.is_deleted_flg <> 'Y'
						and pll.line_location_id = r.line_location_id
					) extended_po_rcv_price, 
					po_line_type, 
					vendor_name, 
					null::Numeric(15,0) source_organization_id, 
					msi.planner_code, 
					r.buyer_name, 
					r.po_unit_of_measure, 
					po_release_number,
					(
						SELECT 
						unit_of_measure 
						FROM 
						bec_ods.mtl_units_of_measure_tl 
						WHERE is_deleted_flg <> 'Y'
						and uom_code = r.primary_uom_code
					) Decodest1,
					(
						SELECT 
						MAX(primary_quantity) 
						FROM 
						(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt, 
						(select * from bec_ods.po_line_locations_all where is_deleted_flg <> 'Y') poll 
						WHERE 
						mmt.transaction_source_id = poll.po_header_id 
						AND poll.line_location_id = r.line_location_id 
						AND mmt.transaction_date = r.receipt_date 
						AND mmt.transaction_quantity = r.rcv_quantity_received
					) Decodest2,
					rcv_quantity_received * (
						SELECT 
						price_override 
						FROM 
						bec_ods.po_line_locations_all pll 
						WHERE pll.is_deleted_flg <> 'Y'
						and pll.line_location_id = r.line_location_id
					) variance_cacl,
					row_number() OVER (PARTITION BY po_number) as ROWNUMBER 
					FROM 
					bec_ods.bec_actual_po_recpt1 r, 
					(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi 
					WHERE 
					r.item_name = msi.segment1 
					AND r.organization_id = msi.organization_id --    and receipt_date between '01-JUL-2019' and '01-JAN-2020'
					AND item_name IS NOT NULL --  and po_number = '10148551'
					AND r.cvmi_flag = 'N' 
					AND r.organization_id IN (106, 245, 265, 285)
				)
			)
			
			Select inventory_item_id,
			organization_id,
			cost_type, 
			plan_name, 
			data_type, 
			order_group, 
			order_type_text, 
			order_number, 
			--organization_id, 
			--organization_code, 
			promised_date, 
			aging_period, 
			trunc(receipt_date) receipt_date, 
			part_number, 
			item_description, 
			planning_make_buy_code, 
			category, 
			rcv_quantity_received, 
			primary_quantity, 
			po_unit_price, 
			primary_unit_of_measure, 
			extended_po_rcv_price, 
			po_line_type, 
			vendor_name, 
			mtl_cost,
			(mtl_cost * ext_mtl_cost_cacl) as ext_mtl_cost,
			(variance_cacl - ext_mtl_cost) as variance,
			source_organization_id, 
			planner_code, 
			buyer_name, 
			-- vendor_country_code, 
			po_unit_of_measure, 
			po_release_number
			-- cost_update_id,
			--ROWNUMBER
			from (select --cms.inventory_item_id, cms.organization_id,
				cms.cost_type, 
				cms.plan_name, 
				cms.data_type, 
				cms.order_group, 
				cms.order_type_text, 
				cms.order_number, 
				cms.organization_id, 
				--cms.organization_code, 
				cms.promised_date, 
				cms.aging_period, 
				cms.receipt_date, 
				cms.part_number, 
				cms.item_description, 
				cms.planning_make_buy_code, 
				cms.category, 
				cms.inventory_item_id,
				cms.rcv_quantity_received, 
				cms.primary_quantity, 
				cms.po_unit_price, 
				cms.primary_unit_of_measure, 
				cms.extended_po_rcv_price, 
				cms.po_line_type, 
				cms.vendor_name, 
				cms.source_organization_id, 
				cms.planner_code, 
				cms.buyer_name, 
				-- cms.vendor_country_code, 
				cms.po_unit_of_measure, 
				cms.po_release_number,
				cms.ext_mtl_cost_cacl,
				cms.variance_cacl,
				max(maxc.cost_update_id) cost_update_id,
				(SELECT 
					standard_cost - nvl(material_overhead, 0) 
					FROM 
					bec_ods.cst_cost_history_v ch 
					WHERE 
					cost_update_id = max(maxc.cost_update_id) 
					AND ch.inventory_item_id = cms.inventory_item_id 
					AND ch.last_update_date <= cms.receipt_date 
					AND organization_id = cms.organization_id
				) mtl_cost,
				cms.ROWNUMBER
				from CTE_Main_STG cms
				left join bec_ods.cst_cost_history_v maxc 
				on maxc.inventory_item_id = cms.inventory_item_id 
				AND maxc.organization_id = cms.organization_id
				AND maxc.last_update_date <= cms.receipt_date 
				group by --cms.inventory_item_id, cms.organization_id,
				cms.cost_type, 
				cms.plan_name, 
				cms.data_type, 
				cms.order_group, 
				cms.order_type_text, 
				cms.order_number, 
				cms.organization_id, 
				cms.organization_code, 
				cms.promised_date, 
				cms.aging_period, 
				cms.receipt_date, 
				cms.part_number, 
				cms.item_description, 
				cms.planning_make_buy_code, 
				cms.category, 
				cms.inventory_item_id,
				cms.rcv_quantity_received, 
				cms.primary_quantity, 
				cms.po_unit_price, 
				cms.primary_unit_of_measure, 
				cms.extended_po_rcv_price, 
				cms.po_line_type, 
				cms.vendor_name, 
				cms.source_organization_id, 
				cms.planner_code, 
				cms.buyer_name, 
				-- cms.vendor_country_code, 
				cms.po_unit_of_measure, 
				cms.po_release_number,
				cms.ext_mtl_cost_cacl,
				cms.variance_cacl,
			cms.ROWNUMBER)
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
dw_table_name = 'fact_rapa_stg4' 
and batch_name = 'ascp';
commit;
