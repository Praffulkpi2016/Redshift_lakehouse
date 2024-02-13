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

drop table if exists bec_ods_stg.MTL_SECONDARY_INVENTORIES;

create table bec_ods_stg.MTL_SECONDARY_INVENTORIES
DISTKEY (ORGANIZATION_ID)
SORTKEY (SECONDARY_INVENTORY_NAME, last_update_date)
as 
select
	*
from
	bec_raw_dl_ext.MTL_SECONDARY_INVENTORIES
where
kca_operation != 'DELETE'
	and (SECONDARY_INVENTORY_NAME,ORGANIZATION_ID,last_update_date) in 
	(
	select
		SECONDARY_INVENTORY_NAME,ORGANIZATION_ID,max(last_update_date)
	from
		bec_raw_dl_ext.MTL_SECONDARY_INVENTORIES
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		SECONDARY_INVENTORY_NAME,ORGANIZATION_ID
	);
end;