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

truncate table bec_ods_stg.AR_AGING_BUCKET_LINES_B;

insert into	bec_ods_stg.AR_AGING_BUCKET_LINES_B
   (aging_bucket_line_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	aging_bucket_id,
	bucket_sequence_num,
	days_start,
	days_to,
	"type",
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
	zd_edition_name,
	zd_sync,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		aging_bucket_line_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	aging_bucket_id,
	bucket_sequence_num,
	days_start,
	days_to,
	"type",
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
	zd_edition_name,
	zd_sync,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.AR_AGING_BUCKET_LINES_B
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (aging_bucket_line_id,kca_seq_id) in 
	(select aging_bucket_line_id,max(kca_seq_id) from bec_raw_dl_ext.AR_AGING_BUCKET_LINES_B 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by aging_bucket_line_id)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ar_aging_bucket_lines_b')
);
end;