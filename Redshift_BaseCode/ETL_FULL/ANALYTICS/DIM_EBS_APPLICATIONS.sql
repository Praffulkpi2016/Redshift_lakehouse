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
drop table if exists bec_dwh.DIM_EBS_APPLICATIONS;

CREATE TABLE  bec_dwh.DIM_EBS_APPLICATIONS 
	diststyle all sortkey(APPLICATION_ID)
AS
(
SELECT 
APPLICATION_ID 
,"LANGUAGE"
, APPLICATION_NAME 
, CREATED_BY
, CREATION_DATE
, LAST_UPDATED_BY
, LAST_UPDATE_DATE 
, LAST_UPDATE_LOGIN 
, DESCRIPTION
, SOURCE_LANG
, ZD_EDITION_NAME
,  ZD_SYNC
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
    )||'-'||NVL(APPLICATION_ID,0)||'-'||NVL("LANGUAGE",'NA') as dw_load_id
	
,getdate()           AS dw_insert_date
,getdate()           AS dw_update_date
FROM BEC_ODS.FND_APPLICATION_TL
);


end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_ebs_applications'
	and batch_name = 'gl';

commit;