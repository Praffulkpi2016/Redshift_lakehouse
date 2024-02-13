/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for stage.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL;

create table bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL 
	DISTKEY(PART_NUMBER)
SORTKEY(PART_NUMBER, ORGANIZATION_ID, PLAN_NAME, BQOH, CVMI_QOH, AS_OF_DATE)
	as (
	select
	*
from
	bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL
where
	kca_operation != 'DELETE'
	and 
        (
         NVL(PART_NUMBER,'NA'),NVL(ORGANIZATION_ID,0),NVL(PLAN_NAME,'NA'),NVL(BQOH,0),NVL(CVMI_QOH,0) ,NVL(AS_OF_DATE,'1900-01-01')
        )
in (
	select
		NVL(PART_NUMBER,'NA') AS PART_NUMBER ,
		NVL(ORGANIZATION_ID,0) AS ORGANIZATION_ID,
        NVL(PLAN_NAME,'NA') AS PLAN_NAME ,
		NVL(BQOH,0) AS BQOH ,
		NVL(CVMI_QOH,0) AS CVMI_QOH ,
        NVL(AS_OF_DATE,'1900-01-01') AS AS_OF_DATE
		
	from
		bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		(NVL(PART_NUMBER,'NA'),NVL(ORGANIZATION_ID,0),NVL(PLAN_NAME,'NA'),NVL(BQOH,0),NVL(CVMI_QOH,0) ,NVL(AS_OF_DATE,'1900-01-01'))

		
)
);
end;