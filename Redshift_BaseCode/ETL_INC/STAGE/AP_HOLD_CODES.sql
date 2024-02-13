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
BEGIN;

TRUNCATE TABLE bec_ods_stg.ap_hold_codes;

insert into	bec_ods_stg.ap_hold_codes
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
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.ap_hold_codes
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (hold_lookup_code,KCA_SEQ_ID) in 
	(select hold_lookup_code,max(KCA_SEQ_ID) from bec_raw_dl_ext.ap_hold_codes 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by hold_lookup_code)
     and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='ap_hold_codes')
	 );
END;