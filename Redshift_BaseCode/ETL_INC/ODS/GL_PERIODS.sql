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

delete from bec_ods.gl_periods
where (PERIOD_SET_NAME,PERIOD_NAME) in (
select stg.PERIOD_SET_NAME,stg.PERIOD_NAME from bec_ods.gl_periods ods, bec_ods_stg.gl_periods stg
where ods.PERIOD_SET_NAME = stg.PERIOD_SET_NAME  and ods.PERIOD_NAME = stg.PERIOD_NAME and stg.kca_operation IN ('INSERT','UPDATE') );

commit;

-- Insert records

insert
	into
	bec_ods.gl_periods
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
,IS_DELETED_FLG
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
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.gl_periods
where kca_operation IN ('INSERT','UPDATE') and (PERIOD_SET_NAME,PERIOD_NAME,kca_seq_id) in (select PERIOD_SET_NAME,PERIOD_NAME,max(kca_seq_id) from bec_ods_stg.gl_periods 
where kca_operation IN ('INSERT','UPDATE')
group by PERIOD_SET_NAME,PERIOD_NAME)
);

commit;

 
-- Soft Delete 
update bec_ods.gl_periods set IS_DELETED_FLG = 'N';
commit;
update bec_ods.gl_periods set IS_DELETED_FLG = 'Y'
where (PERIOD_SET_NAME,PERIOD_NAME)  in
(
select PERIOD_SET_NAME,PERIOD_NAME from bec_raw_dl_ext.gl_periods
where (PERIOD_SET_NAME,PERIOD_NAME,KCA_SEQ_ID)
in 
(
select PERIOD_SET_NAME,PERIOD_NAME,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.gl_periods
group by PERIOD_SET_NAME,PERIOD_NAME
) 
and kca_operation= 'DELETE'
);
commit;
end;


update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='gl_periods';
commit;

