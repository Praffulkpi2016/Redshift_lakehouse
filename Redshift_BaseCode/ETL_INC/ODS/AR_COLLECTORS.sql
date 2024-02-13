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

delete from bec_ods.AR_COLLECTORS
where collector_id in (
select stg.collector_id from bec_ods.AR_COLLECTORS ods, bec_ods_stg.AR_COLLECTORS stg
where ods.collector_id = stg.collector_id and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.AR_COLLECTORS
       (
    collector_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	creation_date,
	created_by,
	"name",
	employee_id,
	description,
	status,
	inactive_date,
	alias,
	telephone_number,
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
	resource_id,
	resource_type,
	zd_edition_name,
	zd_sync, 
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
(
	SELECT
	collector_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	creation_date,
	created_by,
	"name",
	employee_id,
	description,
	status,
	inactive_date,
	alias,
	telephone_number,
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
	resource_id,
	resource_type, 
	zd_edition_name,
	zd_sync,
	kca_operation,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.AR_COLLECTORS
	where kca_operation IN ('INSERT','UPDATE') 
	and (collector_id,kca_seq_id) in 
	(select collector_id,max(kca_seq_id) from bec_ods_stg.AR_COLLECTORS 
     where kca_operation IN ('INSERT','UPDATE')
     group by collector_id)
);

commit;

-- Soft delete
update bec_ods.AR_COLLECTORS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AR_COLLECTORS set IS_DELETED_FLG = 'Y'
where (collector_id )  in
(
select collector_id  from bec_raw_dl_ext.AR_COLLECTORS
where (collector_id ,KCA_SEQ_ID)
in 
(
select collector_id ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AR_COLLECTORS
group by collector_id 
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ar_collectors';

commit;