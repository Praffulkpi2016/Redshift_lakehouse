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

delete from bec_ods.ap_distribution_sets_all
where distribution_set_id in (
select stg.distribution_set_id 
from bec_ods.ap_distribution_sets_all ods, bec_ods_stg.ap_distribution_sets_all stg
where ods.distribution_set_id = stg.distribution_set_id
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.ap_distribution_sets_all
    (distribution_set_id,
	distribution_set_name,
	last_update_date,
	last_updated_by,
	description,
	total_percent_distribution,
	inactive_date,
	last_update_login,
	creation_date,
	created_by,
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
	org_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	distribution_set_id,
	distribution_set_name,
	last_update_date,
	last_updated_by,
	description,
	total_percent_distribution,
	inactive_date,
	last_update_login,
	creation_date,
	created_by,
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
	org_id,
	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.ap_distribution_sets_all
	where kca_operation IN ('INSERT','UPDATE') 
	and (distribution_set_id,KCA_SEQ_ID) in 
	(select distribution_set_id,max(KCA_SEQ_ID) from bec_ods_stg.ap_distribution_sets_all 
     where kca_operation IN ('INSERT','UPDATE')
     group by distribution_set_id)	
	);

commit;

-- Soft delete
update bec_ods.ap_distribution_sets_all set IS_DELETED_FLG = 'N';
update bec_ods.ap_distribution_sets_all set IS_DELETED_FLG = 'Y'
where (distribution_set_id)  in
(
select distribution_set_id from bec_raw_dl_ext.ap_distribution_sets_all
where (distribution_set_id,KCA_SEQ_ID)
in 
(
select distribution_set_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_distribution_sets_all
group by distribution_set_id
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
	ods_table_name = 'ap_distribution_sets_all';

commit;