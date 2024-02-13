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

truncate table bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL;

insert into	bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL
   (	
	 plan_name
	,ran_date  
	,as_of_date 
	,part_number 
	,organization_id 
	,demand 
	,bqoh 
	,cvmi_qoh 
	,ncvmi_receipts
	,cvmi_receipts 
	,remaining 
	,demand_pulls 
	,aging_pulls 
	,future_consumptions 
	,ending_bqoh
	,ending_cvmi_bqoh 
    ,KCA_OPERATION,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
     plan_name
	,ran_date  
	,as_of_date 
	,part_number 
	,organization_id 
	,demand 
	,bqoh 
	,cvmi_qoh 
	,ncvmi_receipts
	,cvmi_receipts 
	,remaining 
	,demand_pulls 
	,aging_pulls 
	,future_consumptions 
	,ending_bqoh
	,ending_cvmi_bqoh  
        ,KCA_OPERATION
		,kca_seq_id
		,KCA_SEQ_DATE
	from bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and  (   NVL(PART_NUMBER,'NA'),
	         NVL(ORGANIZATION_ID,0),
			 NVL(PLAN_NAME,'NA'),NVL(BQOH,0),
	         NVL(CVMI_QOH,0) ,
			 NVL(AS_OF_DATE,'1900-01-01'),
			 kca_seq_id) in 
	       (
	        select
         	    NVL(PART_NUMBER,'NA') AS PART_NUMBER ,
		        NVL(ORGANIZATION_ID,0) AS ORGANIZATION_ID,
                NVL(PLAN_NAME,'NA') AS PLAN_NAME ,
		        NVL(BQOH,0) AS BQOH ,
		        NVL(CVMI_QOH,0) AS CVMI_QOH ,
                NVL(AS_OF_DATE,'1900-01-01') AS AS_OF_DATE,
			    max(kca_seq_id) from bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL 
           where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
           group by 
	               NVL(PART_NUMBER,'NA'),
	               NVL(ORGANIZATION_ID,0),
			       NVL(PLAN_NAME,'NA'),NVL(BQOH,0),
	               NVL(CVMI_QOH,0) ,
			       NVL(AS_OF_DATE,'1900-01-01')
		    )
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'xxbec_cvmi_cons_cal_final')
)
);
end;