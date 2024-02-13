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
drop table if exists bec_ods.MTL_CATEGORIES_B;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_CATEGORIES_B
(
CATEGORY_ID	NUMERIC(15,0)   ENCODE az64
,STRUCTURE_ID	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)  ENCODE az64
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,DESCRIPTION	VARCHAR(240)   ENCODE lzo
,DISABLE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,SEGMENT1	 VARCHAR(40)   ENCODE lzo
,SEGMENT2	 VARCHAR(40)   ENCODE lzo
,SEGMENT3	 VARCHAR(40)   ENCODE lzo
,SEGMENT4	 VARCHAR(40)   ENCODE lzo
,SEGMENT5	 VARCHAR(40)   ENCODE lzo
,SEGMENT6	 VARCHAR(40)   ENCODE lzo
,SEGMENT7	 VARCHAR(40)   ENCODE lzo
,SEGMENT8	 VARCHAR(40)   ENCODE lzo
,SEGMENT9	 VARCHAR(40)   ENCODE lzo
,SEGMENT10	 VARCHAR(40)   ENCODE lzo
,SEGMENT11	 VARCHAR(40)   ENCODE lzo
,SEGMENT12	 VARCHAR(40)   ENCODE lzo
,SEGMENT13	 VARCHAR(40)   ENCODE lzo
,SEGMENT14	 VARCHAR(40)   ENCODE lzo
,SEGMENT15	 VARCHAR(40)   ENCODE lzo
,SEGMENT16	 VARCHAR(40)   ENCODE lzo
,SEGMENT17	 VARCHAR(40)   ENCODE lzo
,SEGMENT18	 VARCHAR(40)   ENCODE lzo
,SEGMENT19	 VARCHAR(40)   ENCODE lzo
,SEGMENT20	 VARCHAR(40)   ENCODE lzo
,SUMMARY_FLAG	 VARCHAR(1)   ENCODE lzo
,ENABLED_FLAG	 VARCHAR(1)   ENCODE lzo
,START_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,END_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ATTRIBUTE_CATEGORY	 VARCHAR(30)   ENCODE lzo
,ATTRIBUTE1	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE9	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE10	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE11	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE12	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE13	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE14	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE15	 VARCHAR(150)   ENCODE lzo
,REQUEST_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,WEB_STATUS	 VARCHAR(30)   ENCODE lzo
,SUPPLIER_ENABLED_FLAG	 VARCHAR(1)   ENCODE lzo
,ZD_EDITION_NAME VARCHAR(30) ENCODE lzo 
,ZD_SYNC VARCHAR(30)  ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.MTL_CATEGORIES_B
(
CATEGORY_ID
,STRUCTURE_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,DISABLE_DATE
,SEGMENT1
,SEGMENT2
,SEGMENT3
,SEGMENT4
,SEGMENT5
,SEGMENT6
,SEGMENT7
,SEGMENT8
,SEGMENT9
,SEGMENT10
,SEGMENT11
,SEGMENT12
,SEGMENT13
,SEGMENT14
,SEGMENT15
,SEGMENT16
,SEGMENT17
,SEGMENT18
,SEGMENT19
,SEGMENT20
,SUMMARY_FLAG
,ENABLED_FLAG
,START_DATE_ACTIVE
,END_DATE_ACTIVE
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
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
--,WH_UPDATE_DATE
--,TOTAL_PROD_ID
,WEB_STATUS
,SUPPLIER_ENABLED_FLAG
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date)
(
select
CATEGORY_ID
,STRUCTURE_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,DISABLE_DATE
,SEGMENT1
,SEGMENT2
,SEGMENT3
,SEGMENT4
,SEGMENT5
,SEGMENT6
,SEGMENT7
,SEGMENT8
,SEGMENT9
,SEGMENT10
,SEGMENT11
,SEGMENT12
,SEGMENT13
,SEGMENT14
,SEGMENT15
,SEGMENT16
,SEGMENT17
,SEGMENT18
,SEGMENT19
,SEGMENT20
,SUMMARY_FLAG
,ENABLED_FLAG
,START_DATE_ACTIVE
,END_DATE_ACTIVE
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
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
--,WH_UPDATE_DATE
--,TOTAL_PROD_ID
,WEB_STATUS
,SUPPLIER_ENABLED_FLAG
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date 
from bec_ods_stg.MTL_CATEGORIES_B
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='mtl_categories_b'; 

commit;

