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
	table bec_ods_stg.fnd_lookup_values;

insert
	into
	bec_ods_stg.fnd_lookup_values
(LOOKUP_TYPE,
LANGUAGE,
LOOKUP_CODE,
MEANING,
DESCRIPTION,
ENABLED_FLAG,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
CREATED_BY,
CREATION_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
LAST_UPDATE_DATE,
SOURCE_LANG,
SECURITY_GROUP_ID,
VIEW_APPLICATION_ID,
TERRITORY_CODE,
ATTRIBUTE_CATEGORY,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
ATTRIBUTE9,
ATTRIBUTE10,
ATTRIBUTE11,
ATTRIBUTE12,
ATTRIBUTE13,
ATTRIBUTE14,
ATTRIBUTE15,
"TAG",
LEAF_NODE
,KCA_OPERATION
,kca_seq_id
,kca_seq_date)
(
	select
		LOOKUP_TYPE,
LANGUAGE,
LOOKUP_CODE,
MEANING,
DESCRIPTION,
ENABLED_FLAG,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
CREATED_BY,
CREATION_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
LAST_UPDATE_DATE,
SOURCE_LANG,
SECURITY_GROUP_ID,
VIEW_APPLICATION_ID,
TERRITORY_CODE,
ATTRIBUTE_CATEGORY,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
ATTRIBUTE6,
ATTRIBUTE7,
ATTRIBUTE8,
ATTRIBUTE9,
ATTRIBUTE10,
ATTRIBUTE11,
ATTRIBUTE12,
ATTRIBUTE13,
ATTRIBUTE14,
ATTRIBUTE15,
"TAG",
LEAF_NODE
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
	from
		bec_raw_dl_ext.fnd_lookup_values
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' and (LOOKUP_TYPE,LANGUAGE,LOOKUP_CODE,SECURITY_GROUP_ID,VIEW_APPLICATION_ID,kca_seq_id) in (select LOOKUP_TYPE,LANGUAGE,LOOKUP_CODE,SECURITY_GROUP_ID,VIEW_APPLICATION_ID,max(kca_seq_id)
from bec_raw_dl_ext.FND_LOOKUP_VALUES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
group by LOOKUP_TYPE,LANGUAGE,LOOKUP_CODE,SECURITY_GROUP_ID,VIEW_APPLICATION_ID)and
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fnd_lookup_values')
);
end;
