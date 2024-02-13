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

drop table if exists bec_ods_stg.XXBEC_MSC_ORDERS;

create table bec_ods_stg.XXBEC_MSC_ORDERS 
	DISTKEY(TRANSACTION_ID)
SORTKEY(TRANSACTION_ID, INVENTORY_ITEM_ID )
	as (
select
	*
from
	bec_raw_dl_ext.XXBEC_MSC_ORDERS
where
	kca_operation != 'DELETE'
	and 
( nvl(TRANSACTION_ID, 0),
	nvl(INVENTORY_ITEM_ID,0) )
in (
	select
		nvl(TRANSACTION_ID,0) as TRANSACTION_ID,
		nvl(INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID 
 	from
		bec_raw_dl_ext.XXBEC_MSC_ORDERS
	where
		kca_operation != 'DELETE'
		and nvl(kca_seq_id, '') = ''
	group by
		TRANSACTION_ID,
		INVENTORY_ITEM_ID  
)
);
end;