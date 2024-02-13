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

drop table if exists bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS;

create table bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS 
DISTSTYLE AUTO
SORTKEY ( LAYER_ID, TRANSACTION_ID, ORGANIZATION_ID, COST_ELEMENT_ID, LEVEL_TYPE, TRANSACTION_ACTION_ID, LAST_UPDATE_DATE)

as
select
*
from
	bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
where
	kca_operation != 'DELETE'
	and (LAYER_ID,
	TRANSACTION_ID,
	ORGANIZATION_ID,
	COST_ELEMENT_ID,
	LEVEL_TYPE,
	TRANSACTION_ACTION_ID,
	LAST_UPDATE_DATE) in 
(
	select
		LAYER_ID,
		TRANSACTION_ID,
		ORGANIZATION_ID,
		COST_ELEMENT_ID,
		LEVEL_TYPE,
		TRANSACTION_ACTION_ID,
		max(LAST_UPDATE_DATE)
	from
		bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		LAYER_ID,
		TRANSACTION_ID,
		ORGANIZATION_ID,
		COST_ELEMENT_ID,
		LEVEL_TYPE,
		TRANSACTION_ACTION_ID);
end;