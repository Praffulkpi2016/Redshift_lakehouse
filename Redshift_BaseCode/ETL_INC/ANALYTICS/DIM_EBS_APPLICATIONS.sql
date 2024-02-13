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
BEGIN;

--  DELETE RECORDS
delete from bec_dwh.DIM_EBS_APPLICATIONS
where (nvl(APPLICATION_ID,0),nvl("LANGUAGE",'NA')) in (
select nvl(ods.APPLICATION_ID,0),nvl(ods.LANGUAGE,'NA') from bec_dwh.dim_ebs_applications dw, bec_ods.FND_APPLICATION_TL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.APPLICATION_ID,0)||'-'||nvl(ods.LANGUAGE,'NA')
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ebs_applications' and batch_name = 'gl')
 )
);

commit;

-- Insert records

insert into bec_dwh.dim_ebs_applications
(
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
    , ZD_SYNC
	, is_deleted_flg
	, source_app_id
	, dw_load_id
	, dw_insert_date
	, dw_update_date
)
(
select
	APPLICATION_ID, 
   "LANGUAGE",
    APPLICATION_NAME, 
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE, 
    LAST_UPDATE_LOGIN, 
    DESCRIPTION,
    SOURCE_LANG,
    ZD_EDITION_NAME, 
    ZD_SYNC,
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )||'-'|| NVL(APPLICATION_ID,0)||'-'||NVL("LANGUAGE",'NA') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.FND_APPLICATION_TL
Where 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ebs_applications' and batch_name = 'gl')
 )
);


-- Soft delete

update bec_dwh.dim_ebs_applications set is_deleted_flg = 'Y'
where (nvl(APPLICATION_ID,0),nvl("LANGUAGE",'NA')) not in (
select nvl(ods.APPLICATION_ID,0),nvl(ods.LANGUAGE,'NA') from bec_dwh.dim_ebs_applications dw, bec_ods.FND_APPLICATION_TL ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||NVL(ods.APPLICATION_ID,0)||'-'||NVL(ods.LANGUAGE,'NA') 
AND ods.is_deleted_flg <> 'Y');

commit;

END;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ebs_applications' and batch_name = 'gl';

COMMIT;

