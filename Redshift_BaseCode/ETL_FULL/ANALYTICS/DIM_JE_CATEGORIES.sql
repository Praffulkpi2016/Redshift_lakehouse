/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;
drop table if exists bec_dwh.DIM_JE_CATEGORIES;

CREATE TABLE  bec_dwh.DIM_JE_CATEGORIES 
	diststyle all sortkey(JE_CATEGORY_NAME)
AS
(
SELECT 
JE_CATEGORY_NAME
,"LANGUAGE"
,SOURCE_LANG
,USER_JE_CATEGORY_NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,CONTEXT
,CONSOLIDATION_FLAG
,JE_CATEGORY_KEY
,ZD_EDITION_NAME
,ZD_SYNC
,'N' as IS_DELETED_FLG
, (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id
,    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )||'-'||NVL(JE_CATEGORY_NAME,'NA')||'-'||NVL("LANGUAGE",'NA')
	as dw_load_id
,getdate()           AS dw_insert_date
,getdate()           AS dw_update_date
FROM BEC_ODS.GL_JE_CATEGORIES_TL
)
;


end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_je_categories'
	and batch_name = 'gl';

commit;