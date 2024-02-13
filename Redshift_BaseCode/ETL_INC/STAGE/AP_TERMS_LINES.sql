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
TRUNCATE TABLE bec_ods_stg.AP_TERMS_LINES;

insert into bec_ods_stg.AP_TERMS_LINES 
(
term_id
,sequence_num
,last_update_date
,last_updated_by
,creation_date
,created_by
,last_update_login
,due_percent
,due_amount
,due_days
,due_day_of_month
,due_months_forward
,discount_percent
,discount_days
,discount_day_of_month
,discount_months_forward
,discount_percent_2
,discount_days_2
,discount_day_of_month_2
,discount_months_forward_2
,discount_percent_3
,discount_days_3
,discount_day_of_month_3
,discount_months_forward_3
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
,fixed_date
,calendar
,discount_amount
,discount_criteria
,discount_amount_2
,discount_criteria_2
,discount_amount_3
,discount_criteria_3
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
) (SELECT 
term_id
,sequence_num
,last_update_date
,last_updated_by
,creation_date
,created_by
,last_update_login
,due_percent
,due_amount
,due_days
,due_day_of_month
,due_months_forward
,discount_percent
,discount_days
,discount_day_of_month
,discount_months_forward
,discount_percent_2
,discount_days_2
,discount_day_of_month_2
,discount_months_forward_2
,discount_percent_3
,discount_days_3
,discount_day_of_month_3
,discount_months_forward_3
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
,fixed_date
,calendar
,discount_amount
,discount_criteria
,discount_amount_2
,discount_criteria_2
,discount_amount_3
,discount_criteria_3
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
from bec_raw_dl_ext.AP_TERMS_LINES	
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' and (TERM_ID,SEQUENCE_NUM,kca_seq_id) in (select TERM_ID,SEQUENCE_NUM,max(kca_seq_id) from bec_raw_dl_ext.AP_TERMS_LINES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
group by TERM_ID,SEQUENCE_NUM)
and  kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='ap_terms_lines')
);
END;