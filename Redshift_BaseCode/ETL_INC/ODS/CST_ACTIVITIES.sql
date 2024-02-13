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

delete from bec_ods.cst_activities
where activity_id in (
select stg.activity_id from bec_ods.cst_activities ods, bec_ods_stg.cst_activities stg
where ods.activity_id = stg.activity_id and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.cst_activities
        (activity_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        activity,
        organization_id,
        description,
        default_basis_type,
        disable_date,
        output_uom,
        value_added_activity_flag,
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
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        zd_edition_name,
        zd_sync,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)
(
	select
		activity_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        activity,
        organization_id,
        description,
        default_basis_type,
        disable_date,
        output_uom,
        value_added_activity_flag,
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
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        zd_edition_name,
        zd_sync,
        KCA_OPERATION,
        'N' AS IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.cst_activities
		where kca_operation IN ('INSERT','UPDATE') 
	and (activity_id,kca_seq_id) in 
	(select activity_id,max(kca_seq_id) from bec_ods_stg.cst_activities 
     where kca_operation IN ('INSERT','UPDATE')
     group by activity_id)
);

commit;

-- Soft delete
update bec_ods.cst_activities set IS_DELETED_FLG = 'N';
commit;
update bec_ods.cst_activities set IS_DELETED_FLG = 'Y'
where (activity_id)  in
(
select activity_id from bec_raw_dl_ext.cst_activities
where (activity_id,KCA_SEQ_ID)
in 
(
select activity_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.cst_activities
group by activity_id
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
	ods_table_name = 'cst_activities';

commit;