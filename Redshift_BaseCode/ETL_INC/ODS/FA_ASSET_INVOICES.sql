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

delete
from
	bec_ods.FA_ASSET_INVOICES
where
	NVL(SOURCE_LINE_ID, 0) in 
	(
	select
		NVL(stg.SOURCE_LINE_ID, 0) as SOURCE_LINE_ID
	from
		bec_ods.FA_ASSET_INVOICES ods,
		bec_ods_stg.FA_ASSET_INVOICES stg
	where
		NVL(ods.SOURCE_LINE_ID, 0) = NVL(stg.SOURCE_LINE_ID, 0)
			and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.FA_ASSET_INVOICES (
	asset_id,
	po_vendor_id,
	asset_invoice_id,
	fixed_assets_cost,
	date_effective,
	date_ineffective,
	invoice_transaction_id_in,
	invoice_transaction_id_out,
	deleted_flag,
	po_number,
	invoice_number,
	payables_batch_name,
	payables_code_combination_id,
	feeder_system_name,
	create_batch_date,
	create_batch_id,
	invoice_date,
	payables_cost,
	post_batch_id,
	invoice_id,
	ap_distribution_line_number,
	payables_units,
	split_merged_code,
	description,
	parent_mass_addition_id,
	last_update_date,
	last_updated_by,
	created_by,
	creation_date,
	last_update_login,
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
	attribute_category_code,
	unrevalued_cost,
	merged_code,
	split_code,
	merge_parent_mass_additions_id,
	split_parent_mass_additions_id,
	project_asset_line_id,
	project_id,
	task_id,
	source_line_id,
	depreciate_in_group_flag,
	material_indicator_flag,
	prior_source_line_id,
	invoice_distribution_id,
	invoice_line_number,
	po_distribution_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
		asset_id,
		po_vendor_id,
		asset_invoice_id,
		fixed_assets_cost,
		date_effective,
		date_ineffective,
		invoice_transaction_id_in,
		invoice_transaction_id_out,
		deleted_flag,
		po_number,
		invoice_number,
		payables_batch_name,
		payables_code_combination_id,
		feeder_system_name,
		create_batch_date,
		create_batch_id,
		invoice_date,
		payables_cost,
		post_batch_id,
		invoice_id,
		ap_distribution_line_number,
		payables_units,
		split_merged_code,
		description,
		parent_mass_addition_id,
		last_update_date,
		last_updated_by,
		created_by,
		creation_date,
		last_update_login,
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
		attribute_category_code,
		unrevalued_cost,
		merged_code,
		split_code,
		merge_parent_mass_additions_id,
		split_parent_mass_additions_id,
		project_asset_line_id,
		project_id,
		task_id,
		source_line_id,
		depreciate_in_group_flag,
		material_indicator_flag,
		prior_source_line_id,
		invoice_distribution_id,
		invoice_line_number,
		po_distribution_id,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.FA_ASSET_INVOICES
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(SOURCE_LINE_ID, 0), 
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(SOURCE_LINE_ID, 0) as SOURCE_LINE_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.FA_ASSET_INVOICES
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(SOURCE_LINE_ID, 0))	
	);

commit;

-- Soft delete
update bec_ods.FA_ASSET_INVOICES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_ASSET_INVOICES set IS_DELETED_FLG = 'Y'
where (SOURCE_LINE_ID)  in
(
select SOURCE_LINE_ID from bec_raw_dl_ext.FA_ASSET_INVOICES
where (SOURCE_LINE_ID,KCA_SEQ_ID)
in 
(
select SOURCE_LINE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_ASSET_INVOICES
group by SOURCE_LINE_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;
 

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'fa_asset_invoices';

commit;