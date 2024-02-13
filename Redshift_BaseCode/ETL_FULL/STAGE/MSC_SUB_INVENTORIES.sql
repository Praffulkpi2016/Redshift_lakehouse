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

drop table if exists bec_ods_stg.MSC_SUB_INVENTORIES;

create table bec_ods_stg.MSC_SUB_INVENTORIES 
DISTKEY (PLAN_ID)
SORTKEY (  PLAN_ID, SR_INSTANCE_ID, ORGANIZATION_ID, SUB_INVENTORY_CODE, last_update_date)
as 
select
	*
from
	bec_raw_dl_ext.MSC_SUB_INVENTORIES
where
	kca_operation != 'DELETE'
	and (nvl(PLAN_ID,0),	nvl(SR_INSTANCE_ID,0),nvl(ORGANIZATION_ID,0),	nvl(SUB_INVENTORY_CODE,'NA'),
	last_update_date )  in (
	select
		nvl(PLAN_ID,0) as PLAN_ID, 
		nvl(SR_INSTANCE_ID,0) as SR_INSTANCE_ID,
		nvl(ORGANIZATION_ID,0) as ORGANIZATION_ID, 
		nvl(SUB_INVENTORY_CODE,'NA') as SUB_INVENTORY_CODE,
		max(last_update_date)
	from
		bec_raw_dl_ext.MSC_SUB_INVENTORIES
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
	nvl(PLAN_ID,0),	nvl(SR_INSTANCE_ID,0),nvl(ORGANIZATION_ID,0),	nvl(SUB_INVENTORY_CODE,'NA') );
end;
 