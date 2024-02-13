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

delete from bec_ods.XXBEC_CVMI_CONS_CAL_FINAL
where (     NVL(PART_NUMBER,'NA'),
	        NVL(ORGANIZATION_ID,0),
			NVL(PLAN_NAME,'NA'),NVL(BQOH,0),
	        NVL(CVMI_QOH,0) ,
			NVL(AS_OF_DATE,'1900-01-01')
	  ) 
		in 
		  (
            select    
	            NVL(stg.PART_NUMBER,'NA') AS PART_NUMBER ,
		        NVL(stg.ORGANIZATION_ID,0) AS ORGANIZATION_ID,
                NVL(stg.PLAN_NAME,'NA') AS PLAN_NAME ,
		        NVL(stg.BQOH,0) AS BQOH ,
		        NVL(stg.CVMI_QOH,0) AS CVMI_QOH ,
                NVL(stg.AS_OF_DATE,'1900-01-01') AS AS_OF_DATE 
                from bec_ods.XXBEC_CVMI_CONS_CAL_FINAL ods , bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL stg 
                
                   where 
                        NVL(ods.PART_NUMBER,'NA') = NVL(stg.PART_NUMBER,'NA') and 
		                NVL(ods.ORGANIZATION_ID,0) = NVL(stg.ORGANIZATION_ID,0) and 
                        NVL(ods.PLAN_NAME,'NA') = NVL(stg.PLAN_NAME,'NA') and 
		                NVL(ods.BQOH,0) = NVL(stg.BQOH,0) and
		                NVL(ods.CVMI_QOH,0) = NVL(stg.CVMI_QOH,0) and
                        NVL(ods.AS_OF_DATE,'1900-01-01') = NVL(stg.AS_OF_DATE,'1900-01-01') 
		                and stg.kca_operation IN ('INSERT','UPDATE')
              );

commit;

-- Insert records

insert into	bec_ods.XXBEC_CVMI_CONS_CAL_FINAL
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
        IS_DELETED_FLG,
		kca_seq_id
		,kca_seq_date)	
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
        ,KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL
	where kca_operation in ('INSERT','UPDATE') 
	  and ( NVL(PART_NUMBER,'NA'),
	         NVL(ORGANIZATION_ID,0),
			 NVL(PLAN_NAME,'NA'),NVL(BQOH,0),
	         NVL(CVMI_QOH,0) ,
			 NVL(AS_OF_DATE,'1900-01-01'),
			 kca_seq_id) 
		  in
		  
	        ( select  NVL(PART_NUMBER,'NA') AS PART_NUMBER ,
		        NVL(ORGANIZATION_ID,0) AS ORGANIZATION_ID,
                NVL(PLAN_NAME,'NA') AS PLAN_NAME ,
		        NVL(BQOH,0) AS BQOH ,
		        NVL(CVMI_QOH,0) AS CVMI_QOH ,
                NVL(AS_OF_DATE,'1900-01-01') AS AS_OF_DATE,
				max(kca_seq_id) 
			from bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL 
                 where kca_operation in ('INSERT','UPDATE')
                     group by NVL(PART_NUMBER,'NA'),
	                          NVL(ORGANIZATION_ID,0),
	                		  NVL(PLAN_NAME,'NA'),NVL(BQOH,0),
	                          NVL(CVMI_QOH,0) ,
	                		  NVL(AS_OF_DATE,'1900-01-01')
		    ) 
);

commit;


-- Soft delete
update bec_ods.XXBEC_CVMI_CONS_CAL_FINAL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.XXBEC_CVMI_CONS_CAL_FINAL set IS_DELETED_FLG = 'Y'
where (NVL(PART_NUMBER,'NA'),NVL(ORGANIZATION_ID,0),NVL(PLAN_NAME,'NA'),NVL(BQOH,0),NVL(CVMI_QOH,0) ,NVL(AS_OF_DATE,'1900-01-01'))  in
(
select NVL(PART_NUMBER,'NA'),NVL(ORGANIZATION_ID,0),NVL(PLAN_NAME,'NA'),NVL(BQOH,0),NVL(CVMI_QOH,0) ,NVL(AS_OF_DATE,'1900-01-01') from bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL
where (NVL(PART_NUMBER,'NA'),NVL(ORGANIZATION_ID,0),NVL(PLAN_NAME,'NA'),NVL(BQOH,0),NVL(CVMI_QOH,0) ,NVL(AS_OF_DATE,'1900-01-01'),KCA_SEQ_ID)
in 
(
select NVL(PART_NUMBER,'NA'),NVL(ORGANIZATION_ID,0),NVL(PLAN_NAME,'NA'),NVL(BQOH,0),NVL(CVMI_QOH,0) ,NVL(AS_OF_DATE,'1900-01-01'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL
group by NVL(PART_NUMBER,'NA'),NVL(ORGANIZATION_ID,0),NVL(PLAN_NAME,'NA'),NVL(BQOH,0),NVL(CVMI_QOH,0) ,NVL(AS_OF_DATE,'1900-01-01')
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'xxbec_cvmi_cons_cal_final';

commit;