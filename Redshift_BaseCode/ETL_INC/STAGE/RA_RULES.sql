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

truncate table bec_ods_stg.RA_RULES;

insert into	bec_ods_stg.RA_RULES
   (
   RULE_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,NAME
,TYPE
,STATUS
,FREQUENCY
,OCCURRENCES
,DESCRIPTION
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
,DEFERRED_REVENUE_FLAG
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
	)
(
	select
RULE_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,NAME
,TYPE
,STATUS
,FREQUENCY
,OCCURRENCES
,DESCRIPTION
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
,DEFERRED_REVENUE_FLAG
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
	from bec_raw_dl_ext.RA_RULES
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (RULE_ID,kca_seq_id) in 
	(select RULE_ID,max(kca_seq_id) from bec_raw_dl_ext.RA_RULES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by RULE_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ra_rules')

            )
);
end;
