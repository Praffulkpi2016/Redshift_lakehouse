/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.XXBEC_RCV_MSL_V
where (ORG_ID,ITEM_ID,SHIPMENT_HEADER_ID) in (
select stg.ORG_ID,stg.ITEM_ID,stg.SHIPMENT_HEADER_ID
from bec_ods.XXBEC_RCV_MSL_V ods, bec_ods_stg.XXBEC_RCV_MSL_V stg
where ods.ORG_ID = stg.ORG_ID
and  ods.ITEM_ID = stg.ITEM_ID
and ods.SHIPMENT_HEADER_ID = stg.SHIPMENT_HEADER_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.XXBEC_RCV_MSL_V
       ( source_type,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	charge_account_id,
	comments,
	deliver_to_location_id,
	deliver_to_person_id,
	destination_context,
	destination_type_code,
	destination_type,
	employee_id,
	from_organization_id,
	item_description,
	item_id,
	item_revision,
	hazard_class,
	un_number,
	line_num,
	item_category_id,
	locator_id,
	need_by_date,
	packing_slip,
	quantity_received,
	quantity_shipped,
	requisition_line_id,
	requisition_header_id,
	order_num,
	order_line_num,
	req_distribution_id,
	shipment_header_id,
	shipment_line_id,
	shipment_line_status_code,
	source_document_code,
	source_document_type,
	to_organization_id,
	to_subinventory,
	transfer_cost,
	transportation_account_id,
	transportation_cost,
	unit_of_measure,
	uom_conversion_rate,
	routing_header_id,
	routing_name,
	reason_id,
	reason_name,
	location_code,
	deliver_to_person,
	po_header_id,
	po_line_id,
	po_line_location_id,
	po_release_id,
	release_num,
	vendor_name,
	vendor_site_code,
	ship_to_location_id,
	primary_unit_of_measure,
	vendor_id,
	bar_code_label,
	truck_num,
	container_num,
	vendor_lot_num,
	secondary_quantity_received,
	secondary_quantity_shipped,
	secondary_unit_of_measure,
	qc_grade,
	org_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
(
	select
		source_type,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	charge_account_id,
	comments,
	deliver_to_location_id,
	deliver_to_person_id,
	destination_context,
	destination_type_code,
	destination_type,
	employee_id,
	from_organization_id,
	item_description,
	item_id,
	item_revision,
	hazard_class,
	un_number,
	line_num,
	item_category_id,
	locator_id,
	need_by_date,
	packing_slip,
	quantity_received,
	quantity_shipped,
	requisition_line_id,
	requisition_header_id,
	order_num,
	order_line_num,
	req_distribution_id,
	shipment_header_id,
	shipment_line_id,
	shipment_line_status_code,
	source_document_code,
	source_document_type,
	to_organization_id,
	to_subinventory,
	transfer_cost,
	transportation_account_id,
	transportation_cost,
	unit_of_measure,
	uom_conversion_rate,
	routing_header_id,
	routing_name,
	reason_id,
	reason_name,
	location_code,
	deliver_to_person,
	po_header_id,
	po_line_id,
	po_line_location_id,
	po_release_id,
	release_num,
	vendor_name,
	vendor_site_code,
	ship_to_location_id,
	primary_unit_of_measure,
	vendor_id,
	bar_code_label,
	truck_num,
	container_num,
	vendor_lot_num,
	secondary_quantity_received,
	secondary_quantity_shipped,
	secondary_unit_of_measure,
	qc_grade,
	org_id,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.XXBEC_RCV_MSL_V
	where kca_operation in ('INSERT','UPDATE') 
	and (ORG_ID,ITEM_ID,SHIPMENT_HEADER_ID,kca_seq_id) in 
	(select ORG_ID,ITEM_ID,SHIPMENT_HEADER_ID,max(kca_seq_id) from bec_ods_stg.XXBEC_RCV_MSL_V 
     where kca_operation in ('INSERT','UPDATE')
     group by ORG_ID,ITEM_ID,SHIPMENT_HEADER_ID)
);

commit;



-- Soft delete
update bec_ods.XXBEC_RCV_MSL_V set IS_DELETED_FLG = 'N';
commit;
update bec_ods.XXBEC_RCV_MSL_V set IS_DELETED_FLG = 'Y'
where (ORG_ID,ITEM_ID,SHIPMENT_HEADER_ID)  in
(
select ORG_ID,ITEM_ID,SHIPMENT_HEADER_ID from bec_raw_dl_ext.XXBEC_RCV_MSL_V
where (ORG_ID,ITEM_ID,SHIPMENT_HEADER_ID,KCA_SEQ_ID)
in 
(
select ORG_ID,ITEM_ID,SHIPMENT_HEADER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.XXBEC_RCV_MSL_V
group by ORG_ID,ITEM_ID,SHIPMENT_HEADER_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'xxbec_rcv_msl_v';

commit;