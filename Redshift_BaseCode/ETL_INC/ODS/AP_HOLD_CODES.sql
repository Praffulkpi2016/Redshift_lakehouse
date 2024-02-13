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

delete from bec_ods.ap_hold_codes
where hold_lookup_code in (
select stg.hold_lookup_code 
from bec_ods.ap_hold_codes ods, bec_ods_stg.ap_hold_codes stg
where ods.hold_lookup_code = stg.hold_lookup_code
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;


-- Insert records

insert into	bec_ods.ap_hold_codes
    (hold_lookup_code,
	hold_type,
	description,
	last_update_date,
	last_updated_by,
	user_releaseable_flag,
	user_updateable_flag,
	inactive_date,
	postable_flag,
	last_update_login,
	creation_date,
	created_by,
	external_description,
	hold_instruction,
	wait_before_notify_days,
	reminder_days,
	initiate_workflow_flag,
	zd_edition_name,
	zd_sync,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	hold_lookup_code,
	hold_type,
	description,
	last_update_date,
	last_updated_by,
	user_releaseable_flag,
	user_updateable_flag,
	inactive_date,
	postable_flag,
	last_update_login,
	creation_date,
	created_by,
	external_description,
	hold_instruction,
	wait_before_notify_days,
	reminder_days,
	initiate_workflow_flag,
	zd_edition_name,
	zd_sync,
	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.ap_hold_codes
where kca_operation IN ('INSERT','UPDATE') 
	and (hold_lookup_code,KCA_SEQ_ID) in 
	(select hold_lookup_code,max(KCA_SEQ_ID) from bec_ods_stg.ap_hold_codes 
     where kca_operation IN ('INSERT','UPDATE')
     group by hold_lookup_code)
);

commit;

-- Soft delete
update bec_ods.ap_hold_codes set IS_DELETED_FLG = 'N';
update bec_ods.ap_hold_codes set IS_DELETED_FLG = 'Y'
where (hold_lookup_code)  in
(
select hold_lookup_code from bec_raw_dl_ext.ap_hold_codes
where (hold_lookup_code,KCA_SEQ_ID)
in 
(
select hold_lookup_code,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_hold_codes
group by hold_lookup_code
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
	ods_table_name = 'ap_hold_codes';
 