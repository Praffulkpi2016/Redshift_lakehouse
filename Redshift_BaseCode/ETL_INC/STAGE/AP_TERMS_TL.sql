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
TRUNCATE TABLE bec_ods_stg.AP_TERMS_TL ;

insert into bec_ods_stg.AP_TERMS_TL
(
term_id
,last_update_date
,last_updated_by
,creation_date
,created_by
,last_update_login
,name
,enabled_flag
,due_cutoff_day
,description
,type
,start_date_active
,end_date_active
,rank
,attribute_category
,attribute1
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,language
,source_lang
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
) (SELECT 
term_id
,last_update_date
,last_updated_by
,creation_date
,created_by
,last_update_login
,name
,enabled_flag
,due_cutoff_day
,description
,type
,start_date_active
,end_date_active
,rank
,attribute_category
,attribute1
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,language
,source_lang
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
 from bec_raw_dl_ext.AP_TERMS_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
and (TERM_ID,language,kca_seq_id) in (select TERM_ID,language,max(kca_seq_id) from bec_raw_dl_ext.AP_TERMS_TL 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
group by TERM_ID,language)
AND kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='ap_terms_tl')
);
END;