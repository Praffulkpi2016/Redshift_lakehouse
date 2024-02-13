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

truncate
	table bec_ods_stg.AP_PAY_GROUP;

insert
	into
	bec_ods_stg.AP_PAY_GROUP
(pay_group_id,
	vendor_pay_group,
	template_id,
	checkrun_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		pay_group_id,
		vendor_pay_group,
		template_id,
		checkrun_id,
		creation_date,
		created_by,
		last_update_date,
		last_updated_by,
		last_update_login,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.AP_PAY_GROUP
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (pay_group_id,KCA_SEQ_ID) in 
	(select pay_group_id,max(KCA_SEQ_ID) from bec_raw_dl_ext.AP_PAY_GROUP 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by pay_group_id)
     and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ap_pay_group')
			);
end;