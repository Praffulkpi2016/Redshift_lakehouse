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
	table bec_ods_stg.ALR_OUTPUT_HISTORY;

COMMIT;

insert
	into
	bec_ods_stg.alr_output_history
(	
	APPLICATION_ID, 
	NAME, 
	CHECK_ID, 
	ROW_NUMBER, 
	DATA_TYPE, 
	VALUE, 
	SECURITY_GROUP_ID, 
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		APPLICATION_ID, 
		NAME, 
		CHECK_ID, 
		ROW_NUMBER, 
		DATA_TYPE, 
		VALUE, 
		SECURITY_GROUP_ID, 
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.alr_output_history
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(APPLICATION_ID, 0),
		nvl(CHECK_ID, 0),
		nvl(ROW_NUMBER, 0),
		nvl(NAME, 'NA'),
		kca_seq_id) in 
(
		select
			nvl(APPLICATION_ID,0) as APPLICATION_ID,
			nvl(CHECK_ID,0) as CHECK_ID,
			nvl(ROW_NUMBER, 0) as ROW_NUMBER,
			nvl(NAME, 'NA') as NAME,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.alr_output_history
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(APPLICATION_ID, 0),
			nvl(CHECK_ID, 0),
			nvl(ROW_NUMBER, 0),
			nvl(NAME, 'NA'))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'alr_output_history')
);
end;