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

truncate table bec_ods_stg.AP_AGING_PERIOD_LINES;

insert into	bec_ods_stg.ap_aging_period_lines
   (aging_period_line_id,
	last_updated_by,
	last_update_date,
	created_by,
	creation_date,
	aging_period_id,
	period_sequence_num,
	days_start,
	days_to,
	type,
	report_heading1,
	report_heading2,
	report_heading3,
	new_line,
	base_date,
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
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		aging_period_line_id,
		last_updated_by,
		last_update_date,
		created_by,
		creation_date,
		aging_period_id,
		period_sequence_num,
		days_start,
		days_to,
		type,
		report_heading1,
		report_heading2,
		report_heading3,
		new_line,
		base_date,
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
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.AP_AGING_PERIOD_LINES
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (aging_period_line_id,kca_seq_id) in 
	(select aging_period_line_id,max(kca_seq_id) from bec_raw_dl_ext.AP_AGING_PERIOD_LINES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by aging_period_line_id)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ap_aging_period_lines')
);
end;