/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.RA_RULES
where RULE_ID in (
select stg.RULE_ID from bec_ods.RA_RULES ods,  bec_ods_stg.RA_RULES stg
where ods.RULE_ID = stg.RULE_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.RA_RULES
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
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date
)
(SELECT
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
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
  KCA_SEQ_DATE

	from bec_ods_stg.RA_RULES
	where kca_operation in ('INSERT','UPDATE') 
	and (RULE_ID,kca_seq_id) in 
	(select RULE_ID,max(kca_seq_id) from bec_ods_stg.RA_RULES 
     where kca_operation in ('INSERT','UPDATE')
     group by RULE_ID)
);

commit;



-- Soft delete
update bec_ods.RA_RULES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RA_RULES set IS_DELETED_FLG = 'Y'
where (RULE_ID)  in
(
select RULE_ID from bec_raw_dl_ext.RA_RULES
where (RULE_ID,KCA_SEQ_ID)
in 
(
select RULE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RA_RULES
group by RULE_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate(),load_type = 'I'
where ods_table_name = 'ra_rules';

commit;