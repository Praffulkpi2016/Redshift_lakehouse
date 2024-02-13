/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.PO_ASL_ATTRIBUTES;

insert into	bec_ods_stg.PO_ASL_ATTRIBUTES
   (asl_id,
	using_organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	document_sourcing_method,
	release_generation_method,
	purchasing_unit_of_measure,
	enable_plan_schedule_flag,
	enable_ship_schedule_flag,
	plan_schedule_type,
	ship_schedule_type,
	plan_bucket_pattern_id,
	ship_bucket_pattern_id,
	enable_autoschedule_flag,
	scheduler_id,
	enable_authorizations_flag,
	vendor_id,
	vendor_site_id,
	item_id,
	category_id,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	price_update_tolerance,
	processing_lead_time,
	min_order_qty,
	fixed_lot_multiple,
	delivery_calendar,
	country_of_origin_code,
	enable_vmi_flag,
	vmi_min_qty,
	vmi_max_qty,
	enable_vmi_auto_replenish_flag,
	vmi_replenishment_approval,
	consigned_from_supplier_flag,
	last_billing_date,
	consigned_billing_cycle,
	consume_on_aging_flag,
	aging_period,
	replenishment_method,
	vmi_min_days,
	vmi_max_days,
	fixed_order_quantity,
	forecast_horizon,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
	asl_id,
	using_organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	document_sourcing_method,
	release_generation_method,
	purchasing_unit_of_measure,
	enable_plan_schedule_flag,
	enable_ship_schedule_flag,
	plan_schedule_type,
	ship_schedule_type,
	plan_bucket_pattern_id,
	ship_bucket_pattern_id,
	enable_autoschedule_flag,
	scheduler_id,
	enable_authorizations_flag,
	vendor_id,
	vendor_site_id,
	item_id,
	category_id,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	price_update_tolerance,
	processing_lead_time,
	min_order_qty,
	fixed_lot_multiple,
	delivery_calendar,
	country_of_origin_code,
	enable_vmi_flag,
	vmi_min_qty,
	vmi_max_qty,
	enable_vmi_auto_replenish_flag,
	vmi_replenishment_approval,
	consigned_from_supplier_flag,
	last_billing_date,
	consigned_billing_cycle,
	consume_on_aging_flag,
	aging_period,
	replenishment_method,
	vmi_min_days,
	vmi_max_days,
	fixed_order_quantity,
	forecast_horizon,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.PO_ASL_ATTRIBUTES
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(ASL_ID,0),nvl(USING_ORGANIZATION_ID, 0 ),kca_seq_id) in 
	(select nvl(ASL_ID,0) as ASL_ID,nvl(USING_ORGANIZATION_ID, 0 ) as USING_ORGANIZATION_ID,max(kca_seq_id) from bec_raw_dl_ext.PO_ASL_ATTRIBUTES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(ASL_ID,0),nvl(USING_ORGANIZATION_ID, 0 ))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'po_asl_attributes')
);
end;
