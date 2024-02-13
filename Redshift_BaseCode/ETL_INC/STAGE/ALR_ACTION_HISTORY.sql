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

truncate
	table bec_ods_stg.ALR_ACTION_HISTORY;

insert
	into
	bec_ods_stg.alr_action_history
    (
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
	KCA_OPERATION,
	KCA_SEQ_ID,
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
		KCA_OPERATION,
		KCA_SEQ_ID,
		kca_seq_date
	from
		bec_raw_dl_ext.alr_action_history
	where
		kca_operation != 'DELETE'
		and (nvl(APPLICATION_ID, 0)  ,
			nvl(ACTION_HISTORY_ID, 0)   , 
		KCA_SEQ_ID) in 
	(
		select
			nvl(APPLICATION_ID, 0) as APPLICATION_ID ,
			nvl(ACTION_HISTORY_ID, 0) as ACTION_HISTORY_ID ,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.alr_action_history
		where
			kca_operation != 'DELETE'
			and nvl(kca_seq_id,'')!= ''
		group by
			nvl(APPLICATION_ID, 0)  ,
			nvl(ACTION_HISTORY_ID, 0)   )
		and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'alr_action_history'));
end;