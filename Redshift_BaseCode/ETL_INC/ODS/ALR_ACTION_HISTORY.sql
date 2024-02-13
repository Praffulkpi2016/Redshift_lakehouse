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

delete
from
	bec_ods.ALR_ACTION_HISTORY
where
	(
	nvl(APPLICATION_ID, 0) ,nvl(ACTION_HISTORY_ID, 0) 
	) in 
	(
	select
		NVL(stg.APPLICATION_ID, 0) as APPLICATION_ID ,
		NVL(stg.ACTION_HISTORY_ID, 0) as ACTION_HISTORY_ID 
	from
		bec_ods.alr_action_history ods,
		bec_ods_stg.alr_action_history stg
	where
		    NVL(ods.APPLICATION_ID, 0) = NVL(stg.APPLICATION_ID, 0) AND 
			NVL(ods.ACTION_HISTORY_ID, 0) = NVL(stg.ACTION_HISTORY_ID, 0)
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.alr_action_history (
	APPLICATION_ID, 
	ACTION_HISTORY_ID, 
	ALERT_ID, 
	ACTION_SET_ID, 
	CHECK_ID, 
	ACTION_ID, 
	ACTION_SET_MEMBER_ID, 
	LAST_UPDATE_DATE, 
	ACTION_LEVEL, 
	COLUMN_WRAP_FLAG, 
	MAXIMUM_SUMMARY_MESSAGE_WIDTH, 
	ACTION_EXECUTED_FLAG, 
	SUCCESS_FLAG, 
	VERSION_NUMBER, 
	MESSAGE_HANDLE, 
	NODE_HANDLE, 
	SECURITY_GROUP_ID,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
		APPLICATION_ID, 
		ACTION_HISTORY_ID, 
		ALERT_ID, 
		ACTION_SET_ID, 
		CHECK_ID, 
		ACTION_ID, 
		ACTION_SET_MEMBER_ID, 
		LAST_UPDATE_DATE, 
		ACTION_LEVEL, 
		COLUMN_WRAP_FLAG, 
		MAXIMUM_SUMMARY_MESSAGE_WIDTH, 
		ACTION_EXECUTED_FLAG, 
		SUCCESS_FLAG, 
		VERSION_NUMBER, 
		MESSAGE_HANDLE, 
		NODE_HANDLE, 
		SECURITY_GROUP_ID,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.alr_action_history
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		nvl(APPLICATION_ID, 0) , nvl(ACTION_HISTORY_ID, 0),
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(APPLICATION_ID, 0) as APPLICATION_ID ,
			nvl(ACTION_HISTORY_ID, 0) as ACTION_HISTORY_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.alr_action_history
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			APPLICATION_ID ,ACTION_HISTORY_ID
			)	
	);

commit;
-- Soft delete
update bec_ods.alr_action_history set IS_DELETED_FLG = 'N';
update bec_ods.alr_action_history set IS_DELETED_FLG = 'Y'
where (nvl(APPLICATION_ID, 0),nvl(ACTION_HISTORY_ID, 0) )  in
(
select APPLICATION_ID,ACTION_HISTORY_ID from bec_raw_dl_ext.alr_action_history
where (APPLICATION_ID,ACTION_HISTORY_ID,KCA_SEQ_ID)
in 
(
select nvl(APPLICATION_ID, 0) as APPLICATION_ID,nvl(ACTION_HISTORY_ID, 0) as ACTION_HISTORY_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.alr_action_history
group by nvl(APPLICATION_ID, 0),nvl(ACTION_HISTORY_ID, 0)
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
	ods_table_name = 'alr_action_history';

commit;