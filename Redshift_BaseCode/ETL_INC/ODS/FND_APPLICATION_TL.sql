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

delete from bec_ods.FND_APPLICATION_TL
where (APPLICATION_ID,LANGUAGE) in (
select stg.APPLICATION_ID,stg.LANGUAGE from bec_ods.FND_APPLICATION_TL ods, bec_ods_stg.FND_APPLICATION_TL stg
where ods.APPLICATION_ID = stg.APPLICATION_ID and ods.LANGUAGE = stg.LANGUAGE and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.FND_APPLICATION_TL
(
 APPLICATION_ID,
 LANGUAGE,
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
 KCA_OPERATION,
 IS_DELETED_FLG,
 kca_seq_id,
 kca_seq_date
)

(SELECT
 APPLICATION_ID,
 LANGUAGE,
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
 KCA_OPERATION,
 'N' AS IS_DELETED_FLG,
cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.FND_APPLICATION_TL
where kca_operation IN ('INSERT','UPDATE') 
	and (APPLICATION_ID,LANGUAGE,kca_seq_id) in 
	(select APPLICATION_ID,LANGUAGE,max(kca_seq_id) from bec_ods_stg.FND_APPLICATION_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by APPLICATION_ID,LANGUAGE)
);
	
commit;

-- Soft delete
update bec_ods.FND_APPLICATION_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FND_APPLICATION_TL set IS_DELETED_FLG = 'Y'
where (APPLICATION_ID,LANGUAGE)  in
(
select APPLICATION_ID,LANGUAGE from bec_raw_dl_ext.FND_APPLICATION_TL
where (APPLICATION_ID,LANGUAGE,KCA_SEQ_ID)
in 
(
select APPLICATION_ID,LANGUAGE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FND_APPLICATION_TL
group by APPLICATION_ID,LANGUAGE
) 
and kca_operation= 'DELETE'
);
commit;

end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'fnd_application_tl';

commit;