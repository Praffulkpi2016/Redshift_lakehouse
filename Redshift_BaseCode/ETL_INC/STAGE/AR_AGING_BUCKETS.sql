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

truncate table bec_ods_stg.AR_AGING_BUCKETS;

insert into	bec_ods_stg.AR_AGING_BUCKETS
   (aging_bucket_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	bucket_name,
	status,
	aging_type,
	description,
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
	ZD_EDITION_NAME,
	ZD_SYNC,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		aging_bucket_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	bucket_name,
	status,
	aging_type,
	description,
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
	ZD_EDITION_NAME,
	ZD_SYNC,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.AR_AGING_BUCKETS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (aging_bucket_id,kca_seq_id) in 
	(select aging_bucket_id,max(kca_seq_id) from bec_raw_dl_ext.AR_AGING_BUCKETS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by aging_bucket_id)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ar_aging_buckets')
);
end;