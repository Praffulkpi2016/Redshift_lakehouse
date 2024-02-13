/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;
-- Delete Records
delete
from
	bec_dwh.DIM_JE_CATEGORIES
where
	(NVL(JE_CATEGORY_NAME,'NA'),nvl("LANGUAGE",'NA') ) in
(
select ods.JE_CATEGORY_NAME,ods."LANGUAGE" from bec_dwh.DIM_JE_CATEGORIES dw,
(
SELECT NVL(JE_CATEGORY_NAME,'NA') JE_CATEGORY_NAME, NVL("LANGUAGE",'NA') "LANGUAGE"
FROM BEC_ODS.GL_JE_CATEGORIES_TL
WHERE 1=1
and (kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_je_categories' and batch_name = 'gl')
 )
)ods
where 1=1
and dw.dw_load_id = (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )||'-'||NVL(ods.JE_CATEGORY_NAME,'NA')||'-'||NVL(ods."LANGUAGE",'NA')
);
commit;
-- Insert records

insert into bec_dwh.DIM_JE_CATEGORIES
(
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
,is_deleted_flg
,SOURCE_APP_ID
,DW_LOAD_ID
,DW_INSERT_DATE
,DW_UPDATE_DATE
)
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
,'N' as is_deleted_flg
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
WHERE 1=1
and  (kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_je_categories' and batch_name = 'gl')
 )
);
commit;
-- Soft delete

update
	bec_dwh.dim_je_categories
set
	is_deleted_flg = 'Y'
where
	(JE_CATEGORY_NAME,"LANGUAGE") not in
(
select nvl(ods.JE_CATEGORY_NAME,'NA') as JE_CATEGORY_NAME,NVL(ods."LANGUAGE",'NA') as "LANGUAGE" from bec_dwh.DIM_JE_CATEGORIES dw,
bec_ods.GL_JE_CATEGORIES_TL ods
where 1=1
and dw.dw_load_id = (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )||'-'||NVL(ods.JE_CATEGORY_NAME,'NA')||'-'||NVL(ods."LANGUAGE",'NA')
AND ods.is_deleted_flg <> 'Y'
);
commit;

end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_je_categories'
	and batch_name = 'gl';

commit;