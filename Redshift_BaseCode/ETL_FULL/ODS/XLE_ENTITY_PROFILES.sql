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
drop table if exists bec_ods.XLE_ENTITY_PROFILES;

CREATE TABLE IF NOT EXISTS bec_ods.XLE_ENTITY_PROFILES
(
LEGAL_ENTITY_ID		NUMERIC(15,0)   ENCODE az64
,PARTY_ID		NUMERIC(15,0)   ENCODE az64
,LEGAL_ENTITY_IDENTIFIER	VARCHAR(30)   ENCODE lzo
,NAME	VARCHAR(240)   ENCODE lzo
,GEOGRAPHY_ID		NUMERIC(15,0)   ENCODE az64
,TRANSACTING_ENTITY_FLAG	VARCHAR(1)   ENCODE lzo
,EFFECTIVE_FROM TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,EFFECTIVE_TO TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LE_INFORMATION_CONTEXT	VARCHAR(30)   ENCODE lzo
,LE_INFORMATION1	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION2	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION3	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION4	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION5	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION6	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION7	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION8	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION9	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION10	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION11	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION12	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION13	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION14	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION15	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION16	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION17	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION18	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION19	VARCHAR(150)   ENCODE lzo
,LE_INFORMATION20	VARCHAR(150)   ENCODE lzo
,LAST_UPDATED_BY		NUMERIC(15,0)   ENCODE az64
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,OBJECT_VERSION_NUMBER	NUMERIC(15,0)   ENCODE az64
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
,ATTRIBUTE16	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE17	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE18	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE19	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE20	VARCHAR(150)   ENCODE lzo
,ACTIVITY_CODE	VARCHAR(150)   ENCODE lzo
,SUB_ACTIVITY_CODE	VARCHAR(150)   ENCODE lzo
,TYPE_OF_COMPANY	VARCHAR(50)   ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.XLE_ENTITY_PROFILES
(LEGAL_ENTITY_ID
,PARTY_ID
,LEGAL_ENTITY_IDENTIFIER
,NAME
,GEOGRAPHY_ID
,TRANSACTING_ENTITY_FLAG
,EFFECTIVE_FROM
,EFFECTIVE_TO
,LE_INFORMATION_CONTEXT
,LE_INFORMATION1
,LE_INFORMATION2
,LE_INFORMATION3
,LE_INFORMATION4
,LE_INFORMATION5
,LE_INFORMATION6
,LE_INFORMATION7
,LE_INFORMATION8
,LE_INFORMATION9
,LE_INFORMATION10
,LE_INFORMATION11
,LE_INFORMATION12
,LE_INFORMATION13
,LE_INFORMATION14
,LE_INFORMATION15
,LE_INFORMATION16
,LE_INFORMATION17
,LE_INFORMATION18
,LE_INFORMATION19
,LE_INFORMATION20
,LAST_UPDATED_BY
,CREATION_DATE
,LAST_UPDATE_LOGIN
,LAST_UPDATE_DATE
,CREATED_BY
,OBJECT_VERSION_NUMBER
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
,ATTRIBUTE16
,ATTRIBUTE17
,ATTRIBUTE18
,ATTRIBUTE19
,ATTRIBUTE20
,ACTIVITY_CODE
,SUB_ACTIVITY_CODE
,TYPE_OF_COMPANY
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(
select
LEGAL_ENTITY_ID
,PARTY_ID
,LEGAL_ENTITY_IDENTIFIER
,NAME
,GEOGRAPHY_ID
,TRANSACTING_ENTITY_FLAG
,EFFECTIVE_FROM
,EFFECTIVE_TO
,LE_INFORMATION_CONTEXT
,LE_INFORMATION1
,LE_INFORMATION2
,LE_INFORMATION3
,LE_INFORMATION4
,LE_INFORMATION5
,LE_INFORMATION6
,LE_INFORMATION7
,LE_INFORMATION8
,LE_INFORMATION9
,LE_INFORMATION10
,LE_INFORMATION11
,LE_INFORMATION12
,LE_INFORMATION13
,LE_INFORMATION14
,LE_INFORMATION15
,LE_INFORMATION16
,LE_INFORMATION17
,LE_INFORMATION18
,LE_INFORMATION19
,LE_INFORMATION20
,LAST_UPDATED_BY
,CREATION_DATE
,LAST_UPDATE_LOGIN
,LAST_UPDATE_DATE
,CREATED_BY
,OBJECT_VERSION_NUMBER
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
,ATTRIBUTE16
,ATTRIBUTE17
,ATTRIBUTE18
,ATTRIBUTE19
,ATTRIBUTE20
,ACTIVITY_CODE
,SUB_ACTIVITY_CODE
,TYPE_OF_COMPANY
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID 
,kca_seq_date 
from bec_ods_stg.XLE_ENTITY_PROFILES
);
end;
update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='xle_entity_profiles';

COMMIT;