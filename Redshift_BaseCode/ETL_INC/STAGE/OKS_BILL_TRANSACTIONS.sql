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

truncate table bec_ods_stg.OKS_BILL_TRANSACTIONS;

insert into	bec_ods_stg.oks_bill_transactions
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
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.oks_bill_transactions
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (ID,kca_seq_id) in 
	(select ID,max(kca_seq_id) from bec_raw_dl_ext.oks_bill_transactions 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'oks_bill_transactions')
);
end;