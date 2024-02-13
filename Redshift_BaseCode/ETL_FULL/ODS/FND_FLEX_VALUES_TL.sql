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

drop table if exists bec_ods.FND_FLEX_VALUES_TL;

CREATE TABLE IF NOT EXISTS bec_ods.FND_FLEX_VALUES_TL
(
FLEX_VALUE_ID	NUMERIC(15,0)   ENCODE az64
,LANGUAGE	VARCHAR(4)   ENCODE lzo
,LAST_UPDATE_DATE	TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,CREATION_DATE	TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,DESCRIPTION	VARCHAR(240)   ENCODE lzo
,SOURCE_LANG	VARCHAR(4)   ENCODE lzo
,FLEX_VALUE_MEANING	VARCHAR(150)   ENCODE lzo
--,SECURITY_GROUP_ID	NUMERIC(15,0)   ENCODE az64
,ZD_EDITION_NAME  VARCHAR(30)   ENCODE lzo
,ZD_SYNC VARCHAR(30)   ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.FND_FLEX_VALUES_TL
(
FLEX_VALUE_ID
,LANGUAGE
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,SOURCE_LANG
,FLEX_VALUE_MEANING 
,ZD_EDITION_NAME 
,ZD_SYNC 
,KCA_OPERATION
,IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date
)
(
select
FLEX_VALUE_ID
,LANGUAGE
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,SOURCE_LANG
,FLEX_VALUE_MEANING 
,ZD_EDITION_NAME 
,ZD_SYNC
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.FND_FLEX_VALUES_TL);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='fnd_flex_values_tl';

commit;
 