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

truncate table bec_ods_stg.AP_DISTRIBUTION_SET_LINES_ALL ;

insert into	bec_ods_stg.ap_distribution_set_lines_all
    (distribution_set_id,
	dist_code_combination_id,
	last_update_date,
	last_updated_by,
	set_of_books_id,
	percent_distribution,
	type_1099,
	vat_code,
	description,
	last_update_login,
	creation_date,
	created_by,
	distribution_set_line_number,
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
	project_accounting_context,
	task_id,
	project_id,
	expenditure_organization_id,
	expenditure_type,
	org_id,
	award_id,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)

(
	select
		distribution_set_id,
		dist_code_combination_id,
		last_update_date,
		last_updated_by,
		set_of_books_id,
		percent_distribution,
		type_1099,
		vat_code,
		description,
		last_update_login,
		creation_date,
		created_by,
		distribution_set_line_number,
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
		project_accounting_context,
		task_id,
		project_id,
		expenditure_organization_id,
		expenditure_type,
		org_id,
		award_id,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.AP_DISTRIBUTION_SET_LINES_ALL
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (distribution_set_id,distribution_set_line_number,KCA_SEQ_ID) in 
	(select distribution_set_id,distribution_set_line_number,max(KCA_SEQ_ID) from bec_raw_dl_ext.AP_DISTRIBUTION_SET_LINES_ALL 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by distribution_set_id,distribution_set_line_number)
     and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ap_distribution_set_lines_all')
);
end;