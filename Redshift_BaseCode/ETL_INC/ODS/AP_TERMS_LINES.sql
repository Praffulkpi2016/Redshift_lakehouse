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

delete from bec_ods.ap_terms_lines
where (term_id,sequence_num) in (
select stg.term_id,stg.sequence_num from bec_ods.ap_terms_lines ods, bec_ods_stg.ap_terms_lines stg
where ods.term_id = stg.term_id and ods.sequence_num = stg.sequence_num and stg.kca_operation IN ('INSERT','UPDATE'));

commit;


insert into bec_ods.ap_terms_lines
(
   term_id                    
,   sequence_num               
, last_update_date   
,   last_updated_by            
, creation_date           
,   created_by                 
,   last_update_login          
,   due_percent                
,   due_amount
,   due_days                   
,   due_day_of_month           
,   due_months_forward         
,   discount_percent           
,   discount_days              
,   discount_day_of_month      
,   discount_months_forward    
,  discount_percent_2    
,  discount_days_2      
,  discount_day_of_month_2
,  discount_months_forward_2
,  discount_percent_3
,  discount_days_3
,  discount_day_of_month_3    
,  discount_months_forward_3  
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
,fixed_date               
,   calendar                   
,  discount_amount             
,   discount_criteria          
,  discount_amount_2  
,   discount_criteria_2        
,   discount_amount_3           
,   discount_criteria_3  
,KCA_OPERATION,
IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date  
)
(
select
   term_id                    
,   sequence_num               
,   TO_DATE(TO_CHAR(last_update_date,'YYYY-MM-DD HH24:MI:SS' ),'YYYY-MM-DD HH24:MI:SS' )            
,   last_updated_by            
,  TO_DATE(TO_CHAR(creation_date,'YYYY-MM-DD HH24:MI:SS' )  ,'YYYY-MM-DD HH24:MI:SS' )              
,   created_by                 
,   last_update_login          
,   due_percent                
,   due_amount
,   due_days                   
,   due_day_of_month           
,   due_months_forward         
,   discount_percent           
,   discount_days              
,   discount_day_of_month      
,   discount_months_forward    
,  discount_percent_2    
,  discount_days_2      
,  discount_day_of_month_2
,  discount_months_forward_2
,  discount_percent_3
,  discount_days_3
,  discount_day_of_month_3    
,  discount_months_forward_3  
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
, TO_DATE(TO_CHAR(fixed_date,'YYYY-MM-DD HH24:MI:SS' ) ,'YYYY-MM-DD HH24:MI:SS' )    as fixed_date          
,   calendar                   
,  discount_amount             
,   discount_criteria          
,  discount_amount_2  
,   discount_criteria_2        
,   discount_amount_3           
,   discount_criteria_3  
,KCA_OPERATION,
'N' AS IS_DELETED_FLG 
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date  
from bec_ods_stg.ap_terms_lines
where kca_operation IN ('INSERT','UPDATE') and (TERM_ID,sequence_num,kca_seq_id) in (select TERM_ID,sequence_num,max(kca_seq_id) from bec_ods_stg.ap_terms_lines 
where kca_operation IN ('INSERT','UPDATE')
group by TERM_ID,sequence_num)
);

commit;

-- Soft delete
update bec_ods.ap_terms_lines set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ap_terms_lines set IS_DELETED_FLG = 'Y'
where (TERM_ID,sequence_num)  in
(
select TERM_ID,sequence_num from bec_raw_dl_ext.ap_terms_lines
where (TERM_ID,sequence_num,KCA_SEQ_ID)
in 
(
select TERM_ID,sequence_num,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_terms_lines
group by TERM_ID,sequence_num
) 
and kca_operation= 'DELETE'
);
commit;

end;


update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='ap_terms_lines';
commit;