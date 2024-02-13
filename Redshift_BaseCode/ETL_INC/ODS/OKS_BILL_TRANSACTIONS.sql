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

delete from bec_ods.OKS_BILL_TRANSACTIONS
where NVL(ID,'NA') in (
select NVL(stg.ID,'NA') from bec_ods.oks_bill_transactions ods, bec_ods_stg.oks_bill_transactions stg
where ods.ID = stg.ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.oks_bill_transactions
       (	
	id,
	currency_code,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	trx_date,
	trx_number,
	trx_amount,
	trx_class,
	last_update_login,
	security_group_id,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
	id,
	currency_code,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	trx_date,
	trx_number,
	trx_amount,
	trx_class,
	last_update_login,
	security_group_id,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.oks_bill_transactions
	where kca_operation IN ('INSERT','UPDATE') 
	and (ID,kca_seq_id) in 
	(select ID,max(kca_seq_id) from bec_ods_stg.oks_bill_transactions 
     where kca_operation IN ('INSERT','UPDATE')
     group by ID)
);

commit;

-- Soft delete
update bec_ods.oks_bill_transactions set IS_DELETED_FLG = 'N';
commit;
update bec_ods.oks_bill_transactions set IS_DELETED_FLG = 'Y'
where (NVL(ID,'NA'))  in
(
select NVL(ID,'NA') from bec_raw_dl_ext.oks_bill_transactions
where (NVL(ID,'NA'),KCA_SEQ_ID)
in 
(
select NVL(ID,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.oks_bill_transactions
group by NVL(ID,'NA')
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oks_bill_transactions';

commit;