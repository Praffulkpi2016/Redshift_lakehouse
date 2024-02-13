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
	table bec_ods_stg.gl_periods;

insert
	into
	bec_ods_stg.gl_periods
(PERIOD_SET_NAME,
PERIOD_NAME,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
START_DATE,
END_DATE,
PERIOD_TYPE,
PERIOD_YEAR,
PERIOD_NUM,
QUARTER_NUM,
ENTERED_PERIOD_NAME,
ADJUSTMENT_PERIOD_FLAG,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
DESCRIPTION,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
CONTEXT,
YEAR_START_DATE,
QUARTER_START_DATE
,ZD_EDITION_NAME
,ZD_SYNC
,kca_operation
,kca_seq_id,
	kca_seq_date)
(
	select
	PERIOD_SET_NAME,
PERIOD_NAME,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
START_DATE,
END_DATE,
PERIOD_TYPE,
PERIOD_YEAR,
PERIOD_NUM,
QUARTER_NUM,
ENTERED_PERIOD_NAME,
ADJUSTMENT_PERIOD_FLAG,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
DESCRIPTION,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
CONTEXT,
YEAR_START_DATE,
QUARTER_START_DATE
,ZD_EDITION_NAME
,ZD_SYNC
,kca_operation
,kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.gl_periods
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' and (PERIOD_SET_NAME,PERIOD_NAME,kca_seq_id)
in  (select PERIOD_SET_NAME,PERIOD_NAME,max(kca_seq_id) from bec_raw_dl_ext.GL_PERIODS 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by PERIOD_SET_NAME,PERIOD_NAME)	
AND
		( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_periods')
		 
            )
);
end;
