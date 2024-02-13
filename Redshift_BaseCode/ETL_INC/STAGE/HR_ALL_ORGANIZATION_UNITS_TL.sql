/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN;
TRUNCATE TABLE bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL;

insert into bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL
(
ORGANIZATION_ID
,LANGUAGE
,SOURCE_LANG
,NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,CREATED_BY
,CREATION_DATE
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,kca_seq_id,
	kca_seq_date
) 
SELECT ORGANIZATION_ID
,LANGUAGE
,SOURCE_LANG
,NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,CREATED_BY
,CREATION_DATE
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,kca_seq_id,
	kca_seq_date
FROM bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS_TL
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' and (ORGANIZATION_ID,LANGUAGE,kca_seq_id) in (select ORGANIZATION_ID,LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.hr_all_organization_units_tl 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by ORGANIZATION_ID,LANGUAGE)
and ( kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='hr_all_organization_units_tl')
 
            )
;

commit;
end;