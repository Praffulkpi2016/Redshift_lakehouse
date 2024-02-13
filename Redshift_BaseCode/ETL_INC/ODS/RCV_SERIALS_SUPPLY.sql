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

delete from bec_ods.RCV_SERIALS_SUPPLY
where (SHIPMENT_LINE_ID,SERIAL_NUM) in (
select stg.SHIPMENT_LINE_ID,stg.SERIAL_NUM
from bec_ods.RCV_SERIALS_SUPPLY ods, bec_ods_stg.RCV_SERIALS_SUPPLY stg
where ods.SHIPMENT_LINE_ID = stg.SHIPMENT_LINE_ID
and  ods.SERIAL_NUM = stg.SERIAL_NUM
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.RCV_SERIALS_SUPPLY
       (supply_type_code,
	serial_num,
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
	lot_num,
	vendor_serial_num,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
	)	
(
	select
		supply_type_code,
	serial_num,
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
	lot_num,
	vendor_serial_num,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.RCV_SERIALS_SUPPLY
	where kca_operation in ('INSERT','UPDATE') 
	and (SHIPMENT_LINE_ID,SERIAL_NUM,kca_seq_id) in 
	(select SHIPMENT_LINE_ID,SERIAL_NUM,max(kca_seq_id) from bec_ods_stg.RCV_SERIALS_SUPPLY 
     where kca_operation in ('INSERT','UPDATE')
     group by SHIPMENT_LINE_ID,SERIAL_NUM)
);

commit;


-- Soft delete
update bec_ods.RCV_SERIALS_SUPPLY set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RCV_SERIALS_SUPPLY set IS_DELETED_FLG = 'Y'
where (SHIPMENT_LINE_ID,SERIAL_NUM)  in
(
select SHIPMENT_LINE_ID,SERIAL_NUM from bec_raw_dl_ext.RCV_SERIALS_SUPPLY
where (SHIPMENT_LINE_ID,SERIAL_NUM,KCA_SEQ_ID)
in 
(
select SHIPMENT_LINE_ID,SERIAL_NUM,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RCV_SERIALS_SUPPLY
group by SHIPMENT_LINE_ID,SERIAL_NUM
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'rcv_serials_supply';

commit;