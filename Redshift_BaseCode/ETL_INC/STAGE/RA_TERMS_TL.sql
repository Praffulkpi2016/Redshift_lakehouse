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

truncate table bec_ods_stg.RA_TERMS_TL;

insert into	bec_ods_stg.RA_TERMS_TL
   (
   TERM_ID
,DESCRIPTION
,NAME
,LANGUAGE
,SOURCE_LANG
,LAST_UPDATE_DATE
,CREATION_DATE
,CREATED_BY
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
)
(
	select
TERM_ID
,DESCRIPTION
,NAME
,LANGUAGE
,SOURCE_LANG
,LAST_UPDATE_DATE
,CREATION_DATE
,CREATED_BY
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
	from bec_raw_dl_ext.RA_TERMS_TL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (TERM_ID,LANGUAGE,kca_seq_id) in 
	(select TERM_ID,LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.RA_TERMS_TL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by TERM_ID,LANGUAGE)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ra_terms_tl')
            )
);
end;