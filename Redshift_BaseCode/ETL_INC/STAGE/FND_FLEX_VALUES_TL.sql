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
	table bec_ods_stg.fnd_flex_values_tl;

insert into bec_ods_stg.FND_FLEX_VALUES_TL
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
---,SECURITY_GROUP_ID
,ZD_EDITION_NAME 
,ZD_SYNC
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
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
---,SECURITY_GROUP_ID
,ZD_EDITION_NAME 
,ZD_SYNC
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
from bec_raw_dl_ext.FND_FLEX_VALUES_TL
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' and (FLEX_VALUE_ID,language,kca_seq_id) in (select FLEX_VALUE_ID,language,max(kca_seq_id) from bec_raw_dl_ext.FND_FLEX_VALUES_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
group by FLEX_VALUE_ID,language) and
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fnd_flex_values_tl')
);
end;
