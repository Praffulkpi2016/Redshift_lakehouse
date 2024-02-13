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

truncate table bec_ods_stg.RCV_LOTS_SUPPLY;

insert into	bec_ods_stg.RCV_LOTS_SUPPLY
   (
	supply_type_code,
	lot_num,
	quantity,
	primary_quantity,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	shipment_line_id,
	transaction_id,
	expiration_date,
	secondary_quantity,
	sublot_num,
	reason_code,
	reason_id,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE	)
(
	select
	supply_type_code,
	lot_num,
	quantity,
	primary_quantity,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	shipment_line_id,
	transaction_id,
	expiration_date,
	secondary_quantity,
	sublot_num,
	reason_code,
	reason_id,
	kca_operation,
	kca_seq_id 
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.RCV_LOTS_SUPPLY
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity,kca_seq_id) in 
	(select NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity,max(kca_seq_id) 
	from bec_raw_dl_ext.RCV_LOTS_SUPPLY 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'rcv_lots_supply')
            )
);
end;