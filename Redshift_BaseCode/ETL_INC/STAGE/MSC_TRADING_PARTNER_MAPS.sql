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

truncate table bec_ods_stg.MSC_TRADING_PARTNER_MAPS;

insert into	bec_ods_stg.MSC_TRADING_PARTNER_MAPS
   (
	map_id,
	map_type,
	tp_key,
	company_key,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	kca_operation,
	kca_seq_id,
	kca_seq_date)
(
	select		
	map_id,
	map_type,
	tp_key,
	company_key,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	kca_operation,
	kca_seq_id ,
	kca_seq_date from bec_raw_dl_ext.MSC_TRADING_PARTNER_MAPS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (MAP_ID,kca_seq_id) in 
	(select MAP_ID,max(kca_seq_id) from bec_raw_dl_ext.MSC_TRADING_PARTNER_MAPS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by MAP_ID)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'msc_trading_partner_maps')
			 
            )
);
end;