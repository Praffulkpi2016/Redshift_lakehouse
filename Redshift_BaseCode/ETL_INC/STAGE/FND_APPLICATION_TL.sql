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

TRUNCATE TABLE bec_ods_stg.FND_APPLICATION_TL;

insert into	bec_ods_stg.FND_APPLICATION_TL
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
	kca_seq_id,
	kca_seq_date
	)
(select
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
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.FND_APPLICATION_TL 
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (APPLICATION_ID,LANGUAGE,KCA_SEQ_ID) in 
	(select APPLICATION_ID,LANGUAGE,max(KCA_SEQ_ID) from bec_raw_dl_ext.FND_APPLICATION_TL 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by APPLICATION_ID,LANGUAGE)
     and kca_seq_date > (select (executebegints-prune_days) 
	from bec_etl_ctrl.batch_ods_info where ods_table_name ='fnd_application_tl')
	);
END;

