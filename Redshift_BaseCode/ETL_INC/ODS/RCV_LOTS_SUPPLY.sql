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

delete from bec_ods.RCV_LOTS_SUPPLY
where (NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity) in (
select NVL(stg.SUPPLY_TYPE_CODE,''),NVL(stg.SHIPMENT_LINE_ID,0),NVL(stg.LOT_NUM,''),stg.quantity
from bec_ods.RCV_LOTS_SUPPLY ods, bec_ods_stg.RCV_LOTS_SUPPLY stg
where 
ods.SUPPLY_TYPE_CODE = stg.SUPPLY_TYPE_CODE and 
ods.SHIPMENT_LINE_ID = stg.SHIPMENT_LINE_ID and 
ods.LOT_NUM = stg.LOT_NUM and 
ods.quantity = stg.quantity
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.RCV_LOTS_SUPPLY
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
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.RCV_LOTS_SUPPLY
	where kca_operation IN ('INSERT')
	and (NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity,kca_seq_id) in 
	(select NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity,max(kca_seq_id) from bec_ods_stg.RCV_LOTS_SUPPLY 
     where kca_operation IN ('INSERT')
     group by NVL(SUPPLY_TYPE_CODE,''),NVL(SHIPMENT_LINE_ID,0),NVL(LOT_NUM,''),quantity)
);

commit;



-- Soft delete
update bec_ods.RCV_LOTS_SUPPLY set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RCV_LOTS_SUPPLY set IS_DELETED_FLG = 'Y'
where (SUPPLY_TYPE_CODE,SHIPMENT_LINE_ID,LOT_NUM,quantity)  in
(
select SUPPLY_TYPE_CODE,SHIPMENT_LINE_ID,LOT_NUM,quantity from bec_raw_dl_ext.RCV_LOTS_SUPPLY
where (SUPPLY_TYPE_CODE,SHIPMENT_LINE_ID,LOT_NUM,quantity,KCA_SEQ_ID)
in 
(
select SUPPLY_TYPE_CODE,SHIPMENT_LINE_ID,LOT_NUM,quantity,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RCV_LOTS_SUPPLY
group by SUPPLY_TYPE_CODE,SHIPMENT_LINE_ID,LOT_NUM,quantity
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'rcv_lots_supply';

commit;