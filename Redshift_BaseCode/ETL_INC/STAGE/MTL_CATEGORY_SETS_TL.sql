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
begin;

truncate
	table bec_ods_stg.MTL_CATEGORY_SETS_TL ;

INSERT
	INTO
	bec_ods_stg.MTL_CATEGORY_SETS_TL
(CATEGORY_SET_ID,
	LANGUAGE,
	SOURCE_LANG,
	CATEGORY_SET_NAME,
	DESCRIPTION,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	ZD_EDITION_NAME,
    ZD_SYNC,
	KCA_OPERATION
	,kca_seq_id,
	kca_seq_date
	)
(
	SELECT
	CATEGORY_SET_ID,
	LANGUAGE,
	SOURCE_LANG,
	CATEGORY_SET_NAME,
	DESCRIPTION,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	ZD_EDITION_NAME,
    ZD_SYNC,
	KCA_OPERATION
	,kca_seq_id,
	kca_seq_date
FROM
	bec_raw_dl_ext.MTL_CATEGORY_SETS_TL
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
and (CATEGORY_SET_ID,LANGUAGE,kca_seq_id) in (select CATEGORY_SET_ID,LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.MTL_CATEGORY_SETS_TL 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by CATEGORY_SET_ID,LANGUAGE)
and 
 kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'mtl_category_sets_tl')
 
);
end;
