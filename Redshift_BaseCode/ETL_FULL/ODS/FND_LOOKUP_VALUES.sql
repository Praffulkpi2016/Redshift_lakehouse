/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_ods.FND_LOOKUP_VALUES;

CREATE TABLE IF NOT EXISTS bec_ods.FND_LOOKUP_VALUES
(
LOOKUP_TYPE	VARCHAR(30)   ENCODE lzo
,LANGUAGE	VARCHAR(30)   ENCODE lzo
,LOOKUP_CODE	VARCHAR(30)   ENCODE lzo
,MEANING	VARCHAR(80)   ENCODE lzo
,DESCRIPTION	VARCHAR(240)   ENCODE lzo
,ENABLED_FLAG	VARCHAR(1)   ENCODE lzo
,START_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,END_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,SOURCE_LANG	VARCHAR(4)   ENCODE lzo
,SECURITY_GROUP_ID	NUMERIC(15,0)   ENCODE az64
,VIEW_APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,TERRITORY_CODE	VARCHAR(2)   ENCODE lzo
,ATTRIBUTE_CATEGORY	VARCHAR(30)   ENCODE lzo
,ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE11	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE12	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE13	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE14	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE15	VARCHAR(150)   ENCODE lzo
,"TAG"	VARCHAR(150)   ENCODE lzo
,LEAF_NODE	VARCHAR(1)   ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.FND_LOOKUP_VALUES
(
LOOKUP_TYPE
,LANGUAGE
,LOOKUP_CODE
,MEANING
,DESCRIPTION
,ENABLED_FLAG
,START_DATE_ACTIVE
,END_DATE_ACTIVE
,CREATED_BY
,CREATION_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,LAST_UPDATE_DATE
,SOURCE_LANG
,SECURITY_GROUP_ID
,VIEW_APPLICATION_ID
,TERRITORY_CODE
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,"TAG"
,LEAF_NODE
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date
)
(
select
LOOKUP_TYPE
,LANGUAGE
,LOOKUP_CODE
,MEANING
,DESCRIPTION
,ENABLED_FLAG
,START_DATE_ACTIVE
,END_DATE_ACTIVE
,CREATED_BY
,CREATION_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,LAST_UPDATE_DATE
,SOURCE_LANG
,SECURITY_GROUP_ID
,VIEW_APPLICATION_ID
,TERRITORY_CODE
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,"TAG"
,LEAF_NODE
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
,kca_seq_date
from bec_ods_stg.FND_LOOKUP_VALUES);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='fnd_lookup_values';

commit;