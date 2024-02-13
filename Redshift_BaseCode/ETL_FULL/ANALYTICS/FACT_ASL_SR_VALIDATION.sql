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
	drop table if exists bec_dwh.FACT_ASL_SR_VALIDATION;
	
	CREATE TABLE  bec_dwh.FACT_ASL_SR_VALIDATION 
	diststyle all sortkey(asl_id) 
	as 
	(  
		SELECT   
		asl_id,
		asl_status_id, 
		buyer_id,
		using_organization_id,
		item_id,
		owning_organization_id,
		ship_bucket_pattern_id,
		plan_bucket_pattern_id,
		scheduler_id,
		inventory_item_id,
		vendor_id,
		vendor_site_id,
		organization_id,
		global_flag,
		disable_flag,
		part_number,
		description,
		list_price_per_unit,
		fixed_days_supply,
		fixed_lot_multiplier,
		std_lot_size,
		release_time_fence_days,
		inventory_planning_code,
		planner_code,
		mrp_planning_code,
		default_subinventory,
		document_type_code,
		document_header_id,
		document_line_id, 
		document_num,
		line_num,
		document_status,
		start_date,
		end_Date,
		purchasing_unit_of_measure,
		release_method,
		document_sourcing_method,
		enable_authorizations_flag,
		enable_autoschedule_flag,
		enable_plan_schedule_flag,
		enable_vmi_auto_replenish_flag,
		enable_vmi_flag,
		vmi_replenishment_approval,
		vmi_max_qty,
		vmi_min_days,
		vmi_min_qty,
		vmi_max_days,
		consigned_from_supplier_flag,
		consigned_billing_cycle,
		consume_on_aging_flag,
		aging_period,
		last_billing_date,
		fixed_lot_multiple,
		fixed_order_quantity,
		forecast_horizon,
		min_order_qty,
		plan_schedule_type,
		ship_schedule_type,
		price_update_tolerance,
		processing_lead_time,
		replenishment_method,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || asl_id as asl_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || asl_status_id as asl_status_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || buyer_id as buyer_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || using_organization_id as using_organization_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || item_id as item_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || owning_organization_id as owning_organization_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || ship_bucket_pattern_id as ship_bucket_pattern_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || plan_bucket_pattern_id as plan_bucket_pattern_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || scheduler_id as scheduler_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || vendor_id as vendor_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || vendor_site_id as vendor_site_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id as organization_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || document_header_id as document_header_id_KEY,
		( select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || document_line_id as document_line_id_KEY,
 		'N' AS IS_DELETED_FLG,	 
		(
			SELECT
            system_id
			FROM
            bec_etl_ctrl.etlsourceappid
			WHERE
            source_system = 'EBS'
		)                   AS source_app_id,
		(
			SELECT
            system_id
			FROM
            bec_etl_ctrl.etlsourceappid
			WHERE
            source_system = 'EBS'
		) 
		
		||'-'||nvl(item_id,0)
		||'-'||nvl(asl_id,0) 
		||'-'||nvl(document_header_id,0)
		||'-'||nvl(document_line_id,0) 
		||'-'||nvl(using_organization_id,0)
		||'-'||nvl(owning_organization_id,0) 
		AS dw_load_id,
		getdate()           AS dw_insert_date,
		getdate()           AS dw_update_date
		from
		(  SELECT    distinct
			paa.asl_id,
			pasl.asl_status_id, 
			msi.buyer_id,
			paa.using_organization_id,
			pasl.item_id,
			pasl.owning_organization_id,
			paa.ship_bucket_pattern_id,
			paa.plan_bucket_pattern_id,
			paa.scheduler_id,
			msi.inventory_item_id,
			pasl.vendor_id,
			pasl.vendor_site_id,
			msi.organization_id,
			-- ood.organization_code                                                       owning_organization_code,
			-- pas.status,
			decode(pasl.using_organization_id, - 1, 'Y', 'N')                           global_flag,
			pasl.disable_flag,
			-- aps.vendor_name,
			-- apss.vendor_site_code,
			msi.segment1                                                                part_number,
			msi.description,
			msi.list_price_per_unit,
			msi.fixed_days_supply,
			msi.fixed_lot_multiplier,
			msi.std_lot_size,
			msi.release_time_fence_days,
			--  poa.agent_name                                                              buyer,
			ml1.meaning                                                                 inventory_planning_code,
			msi.planner_code,
			ml2.meaning                                                                 mrp_planning_code,
			dsub.subinventory_code                                                      default_subinventory,
			pad.document_type_code,
			pad.document_header_id,
			pad.document_line_id,
			--pad.document_status,
			ph.clm_document_number as document_num,
			nvl(pl.line_num_display, pl.line_num::char) as line_num,
			ph.status_lookup_code,
			decode(pad.document_type_code, 'QUOTATION', ph.status_lookup_code, 'BLANKET', ph.authorization_status,
			'CONTRACT', ph.authorization_status) as document_status,
			ph.start_date, --, po_headers.start_date
			ph.end_date, -- pad.effective_to,   po_headers.end_date
			paa.purchasing_unit_of_measure,
			plc.meaning                                                         release_method,
			paa.document_sourcing_method,
			paa.enable_authorizations_flag,
			paa.enable_autoschedule_flag,
			paa.enable_plan_schedule_flag,
			paa.enable_vmi_auto_replenish_flag,
			paa.enable_vmi_flag,
			paa.vmi_replenishment_approval,
			paa.vmi_max_qty,
			paa.vmi_min_days,
			paa.vmi_min_qty,
			paa.vmi_max_days,
			paa.consigned_from_supplier_flag,
			paa.consigned_billing_cycle,
			paa.consume_on_aging_flag,
			paa.aging_period,
			paa.last_billing_date,
			paa.fixed_lot_multiple,
			paa.fixed_order_quantity,
			paa.forecast_horizon,
			paa.min_order_qty,
			-- cbp.bucket_pattern_name                                                     plan_bucket_pattern,
			plc1.meaning                                                        plan_schedule_type,
			plc2.meaning                                                        ship_schedule_type,
			paa.price_update_tolerance,
			paa.processing_lead_time,
			decode(paa.replenishment_method, 1, 'Min-Max Quantities', 2, 'Min-Max Days',
			3, 'Min Qty and Fixed Order Qty', 4, 'Min Days and Fixed Order Qty') replenishment_method
			-- ppf.full_name                                                               scheduler_name
			--cbp1.bucket_pattern_name                                                    ship_bucket_pattern
			FROM
			(SELECT * FROM BEC_ODS.po_asl_attributes  WHERE IS_DELETED_FLG <> 'Y')          paa,
			(SELECT * FROM BEC_ODS.po_approved_supplier_list  WHERE IS_DELETED_FLG <> 'Y')  pasl,
			(SELECT * FROM BEC_ODS.po_asl_documents        WHERE IS_DELETED_FLG <> 'Y')     pad,
			(SELECT * FROM BEC_ODS.po_headers_all        WHERE IS_DELETED_FLG <> 'Y')     ph,
			(SELECT * FROM BEC_ODS.po_lines_all        WHERE IS_DELETED_FLG <> 'Y')     pl,
			(SELECT * FROM BEC_ODS.mtl_system_items_b   WHERE IS_DELETED_FLG <> 'Y')        msi,
			-- ap_suppliers                 aps,
				-- ap_supplier_sites_all        apss,
				--po_asl_statuses              pas,
			-- po_agents_v                  poa,
			(SELECT * FROM  BEC_ODS.fnd_lookup_values      WHERE IS_DELETED_FLG <> 'Y')            ml1,
			(SELECT * FROM   BEC_ODS.fnd_lookup_values       WHERE IS_DELETED_FLG <> 'Y')           ml2,
 			(SELECT * FROM  BEC_ODS.mtl_item_sub_defaults  WHERE IS_DELETED_FLG <> 'Y')      dsub, 
			(SELECT * FROM BEC_ODS.fnd_lookup_values       WHERE IS_DELETED_FLG <> 'Y')       plc,
			(SELECT * FROM BEC_ODS.fnd_lookup_values      WHERE IS_DELETED_FLG <> 'Y')        plc1,
			(SELECT * FROM BEC_ODS.fnd_lookup_values      WHERE IS_DELETED_FLG <> 'Y')        plc2
			WHERE
            pasl.item_id = msi.inventory_item_id
			AND pasl.owning_organization_id = msi.organization_id
			AND pasl.asl_id = paa.asl_id
			--AND pasl.vendor_id = aps.vendor_id
				--AND aps.vendor_id = apss.vendor_id
			--AND pasl.vendor_site_id = apss.vendor_site_id
			AND pasl.asl_id = pad.asl_id
			AND pad.document_header_id = ph.po_header_id(+)
			AND ph.po_header_id = pl.po_header_id(+)
			AND (pad.document_line_id is NULL OR pad.document_line_id = pl.po_line_id)
			--AND pasl.asl_status_id = pas.status_id
			-- AND msi.buyer_id = poa.agent_id (+)
			AND ml1.lookup_type (+) = 'MTL_MATERIAL_PLANNING'
			AND ml1.language(+)       = 'US'
			AND msi.inventory_planning_code = ml1.lookup_code
			AND ml2.lookup_type (+) = 'MRP_PLANNING_CODE'
			AND ml2.language(+)       = 'US'
			AND msi.mrp_planning_code = ml2.lookup_code
			AND msi.organization_id = dsub.organization_id (+)
			AND msi.inventory_item_id = dsub.inventory_item_id (+)
			AND plc2.lookup_type (+) = 'SHIP_SCHEDULE_SUBTYPE'
			AND plc2.language(+)       = 'US'
			AND paa.ship_schedule_type = plc2.lookup_code (+)
			AND plc1.lookup_type (+) = 'PLAN_SCHEDULE_SUBTYPE'
			AND plc1.language(+)       = 'US'
			AND paa.plan_schedule_type = plc1.lookup_code (+)
			AND paa.release_generation_method = plc.lookup_code (+)
			AND plc.lookup_type (+) = 'DOC GENERATION METHOD'
			AND plc.language(+)       = 'US'
			-- AND paa.scheduler_id = ppf.person_id (+)
		)
	); 
end; 

UPDATE bec_etl_ctrl.batch_dw_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
dw_table_name  = 'fact_asl_sr_validation'
and batch_name = 'po';

commit;