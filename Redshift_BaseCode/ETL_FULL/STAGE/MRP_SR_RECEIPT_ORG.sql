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

drop table if exists bec_ods_stg.MRP_SR_RECEIPT_ORG;

create table bec_ods_stg.MRP_SR_RECEIPT_ORG 
	DISTKEY (SR_RECEIPT_ID)
	SORTKEY (SR_RECEIPT_ID)
	as (
select
*
from
	bec_raw_dl_ext.MRP_SR_RECEIPT_ORG
where
	kca_operation != 'DELETE'
	and 
        (
         NVL(SR_RECEIPT_ID,0)
        )
in (
	select
		NVL(SR_RECEIPT_ID,0) AS SR_RECEIPT_ID 
	from
		bec_raw_dl_ext.MRP_SR_RECEIPT_ORG
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		NVL(SR_RECEIPT_ID,0)

		
)
);
end;