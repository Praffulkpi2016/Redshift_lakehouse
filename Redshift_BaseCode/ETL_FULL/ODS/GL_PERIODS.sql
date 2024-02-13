/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_ods.GL_PERIODS;

CREATE TABLE IF NOT EXISTS bec_ods.GL_PERIODS
(
PERIOD_SET_NAME	VARCHAR(15)   ENCODE lzo
,PERIOD_NAME	VARCHAR(15)   ENCODE lzo
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,END_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,PERIOD_TYPE	VARCHAR(15)   ENCODE lzo
,PERIOD_YEAR	NUMERIC(15,0)   ENCODE az64
,PERIOD_NUM	NUMERIC(15,0)   ENCODE az64
,QUARTER_NUM	NUMERIC(15,0)   ENCODE az64
,ENTERED_PERIOD_NAME	VARCHAR(15)   ENCODE lzo
,ADJUSTMENT_PERIOD_FLAG	VARCHAR(1)   ENCODE lzo
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,DESCRIPTION	VARCHAR(240)   ENCODE lzo
,ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,CONTEXT	VARCHAR(150)   ENCODE lzo
,YEAR_START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,QUARTER_START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ZD_EDITION_NAME VARCHAR(30) ENCODE lzo 
,ZD_SYNC VARCHAR(30)  ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.GL_PERIODS
(
PERIOD_SET_NAME
,PERIOD_NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,START_DATE
,END_DATE
,PERIOD_TYPE
,PERIOD_YEAR
,PERIOD_NUM
,QUARTER_NUM
,ENTERED_PERIOD_NAME
,ADJUSTMENT_PERIOD_FLAG
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,CONTEXT
,YEAR_START_DATE
,QUARTER_START_DATE
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date)
(
select
PERIOD_SET_NAME
,PERIOD_NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,START_DATE
,END_DATE
,PERIOD_TYPE
,PERIOD_YEAR
,PERIOD_NUM
,QUARTER_NUM
,ENTERED_PERIOD_NAME
,ADJUSTMENT_PERIOD_FLAG
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,CONTEXT
,YEAR_START_DATE
,QUARTER_START_DATE
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.GL_PERIODS);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='gl_periods'; 

commit;