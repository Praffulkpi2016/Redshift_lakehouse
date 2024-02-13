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

delete from bec_ods.AR_AGING_BUCKET_LINES_B
where aging_bucket_line_id in (
select stg.aging_bucket_line_id
from bec_ods.AR_AGING_BUCKET_LINES_B ods, bec_ods_stg.AR_AGING_BUCKET_LINES_B stg
where ods.aging_bucket_line_id = stg.aging_bucket_line_id
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.AR_AGING_BUCKET_LINES_B
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
	kca_operation,
	IS_DELETED_FLG,
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.AR_AGING_BUCKET_LINES_B
	where kca_operation IN ('INSERT','UPDATE') 
	and (aging_bucket_line_id,kca_seq_id) in 
	(select aging_bucket_line_id,max(kca_seq_id) from bec_ods_stg.AR_AGING_BUCKET_LINES_B 
     where kca_operation IN ('INSERT','UPDATE')
     group by aging_bucket_line_id)
);

commit;

-- Soft delete
update bec_ods.AR_AGING_BUCKET_LINES_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AR_AGING_BUCKET_LINES_B set IS_DELETED_FLG = 'Y'
where (aging_bucket_line_id )  in
(
select aging_bucket_line_id  from bec_raw_dl_ext.AR_AGING_BUCKET_LINES_B
where (aging_bucket_line_id ,KCA_SEQ_ID)
in 
(
select aging_bucket_line_id ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AR_AGING_BUCKET_LINES_B
group by aging_bucket_line_id 
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ar_aging_bucket_lines_b';

commit;