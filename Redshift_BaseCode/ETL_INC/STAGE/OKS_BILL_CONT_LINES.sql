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

truncate table bec_ods_stg.OKS_BILL_CONT_LINES;

insert into	bec_ods_stg.oks_bill_cont_lines
   (	
	id,
	cle_id,
	btn_id,
	date_billed_from,
	date_billed_to,
	sent_yn,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	amount,
	bill_action,
	date_next_invoice,
	last_update_login,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	security_group_id,
	currency_code,
	averaged_yn,
	settled_yn,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
	id,
	cle_id,
	btn_id,
	date_billed_from,
	date_billed_to,
	sent_yn,
	object_version_number,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	amount,
	bill_action,
	date_next_invoice,
	last_update_login,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	security_group_id,
	currency_code,
	averaged_yn,
	settled_yn,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.oks_bill_cont_lines
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (ID,kca_seq_id) in 
	(select ID,max(kca_seq_id) from bec_raw_dl_ext.oks_bill_cont_lines 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'oks_bill_cont_lines')
);
end;