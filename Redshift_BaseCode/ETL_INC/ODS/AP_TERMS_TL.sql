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

delete from bec_ods.AP_TERMS_TL
where (TERM_ID,language) in (
select stg.term_id,stg.language from bec_ods.AP_TERMS_TL ods, bec_ods_stg.AP_TERMS_TL stg
where ods.term_id = stg.term_id 
and ods.language = stg.language and stg.kca_operation IN ('INSERT','UPDATE') );

commit;
-- Insert records
 
insert into bec_ods.ap_terms_tl
(
   term_id             
, last_update_date
,   last_updated_by     
,  creation_date  
,   created_by          
,   last_update_login   
,   name                
,   enabled_flag        
,   due_cutoff_day      
,   description         
,   type                
,   start_date_active
,   end_date_active  
,   rank                
,   attribute_category  
,   attribute1          
,   attribute2          
,   attribute3          
,   attribute4          
,   attribute5          
,   attribute6          
,   attribute7          
,   attribute8          
,   attribute9          
,   attribute10         
,   attribute11         
,   attribute12         
,   attribute13         
,   attribute14         
,   attribute15         
,   language            
,   source_lang
,KCA_OPERATION,
IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date
)
(
select
   term_id             
,   TO_DATE(TO_CHAR(last_update_date,'YYYY-MM-DD HH24:MI:SS' ) ,'YYYY-MM-DD HH24:MI:SS' )   
,   last_updated_by     
,  TO_DATE(TO_CHAR(creation_date,'YYYY-MM-DD HH24:MI:SS' ) ,'YYYY-MM-DD HH24:MI:SS')        
,   created_by          
,   last_update_login   
,   name                
,   enabled_flag        
,   due_cutoff_day      
,   description         
,   type                
,  TO_DATE(TO_CHAR(start_date_active,'YYYY-MM-DD HH24:MI:SS' ) ,'YYYY-MM-DD HH24:MI:SS' )   
,  TO_DATE(TO_CHAR(end_date_active,'YYYY-MM-DD HH24:MI:SS' ),'YYYY-MM-DD HH24:MI:SS' )      
,   rank                
,   attribute_category  
,   attribute1          
,   attribute2          
,   attribute3          
,   attribute4          
,   attribute5          
,   attribute6          
,   attribute7          
,   attribute8          
,   attribute9          
,   attribute10         
,   attribute11         
,   attribute12         
,   attribute13         
,   attribute14         
,   attribute15         
,   language            
,   source_lang
,KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
FROM bec_ods_stg.AP_TERMS_TL
where kca_operation IN ('INSERT','UPDATE') and (TERM_ID,language,kca_seq_id) in (select TERM_ID,language,max(kca_seq_id) from bec_ods_stg.AP_TERMS_TL 
where kca_operation IN ('INSERT','UPDATE')
group by TERM_ID,language)
);
commit;
-- Soft delete
update bec_ods.AP_TERMS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AP_TERMS_TL set IS_DELETED_FLG = 'Y'
where (TERM_ID,language)  in
(
select TERM_ID,language from bec_raw_dl_ext.AP_TERMS_TL
where (TERM_ID,language,KCA_SEQ_ID)
in 
(
select TERM_ID,language,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AP_TERMS_TL
group by TERM_ID,language
) 
and kca_operation= 'DELETE'
);
commit;
end;


update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='ap_terms_tl';
commit;
