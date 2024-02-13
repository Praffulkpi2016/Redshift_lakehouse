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

delete from bec_ods.ap_holds_all
where hold_id in (
select stg.hold_id 
from bec_ods.ap_holds_all ods, bec_ods_stg.ap_holds_all stg
where ods.hold_id = stg.hold_id
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.ap_holds_all
    (invoice_id,
	line_location_id,
	hold_lookup_code,
	last_update_date,
	last_updated_by,
	held_by,
	hold_date,
	hold_reason,
	release_lookup_code,
	release_reason,
	status_flag,
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
	responsibility_id,
	rcv_transaction_id,
	hold_details,
	line_number,
	hold_id,
	wf_status,
	validation_request_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	invoice_id,
	line_location_id,
	hold_lookup_code,
	last_update_date,
	last_updated_by,
	held_by,
	hold_date,
	hold_reason,
	release_lookup_code,
	release_reason,
	status_flag,
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
	responsibility_id,
	rcv_transaction_id,
	hold_details,
	line_number,
	hold_id,
	wf_status,
	validation_request_id,
	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.ap_holds_all
where kca_operation IN ('INSERT','UPDATE') 
	and (hold_id,KCA_SEQ_ID) in 
	(select hold_id,max(KCA_SEQ_ID) from bec_ods_stg.ap_holds_all 
     where kca_operation IN ('INSERT','UPDATE')
     group by hold_id)	
	);

commit;

-- Soft delete
update bec_ods.ap_holds_all set IS_DELETED_FLG = 'N';
update bec_ods.ap_holds_all set IS_DELETED_FLG = 'Y'
where (hold_id)  in
(
select hold_id from bec_raw_dl_ext.ap_holds_all
where (hold_id,KCA_SEQ_ID)
in 
(
select hold_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_holds_all
group by hold_id
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
	ods_table_name = 'ap_holds_all';

commit;