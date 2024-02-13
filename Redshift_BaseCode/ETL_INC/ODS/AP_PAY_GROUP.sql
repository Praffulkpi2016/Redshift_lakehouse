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

delete from bec_ods.ap_pay_group
where pay_group_id in (
select stg.pay_group_id 
from bec_ods.ap_pay_group ods, bec_ods_stg.ap_pay_group stg
where ods.pay_group_id = stg.pay_group_id
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;


-- Insert records

insert
	into
	bec_ods.ap_pay_group
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
	IS_DELETED_FLG,
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
		'N' AS IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.ap_pay_group
	where kca_operation IN ('INSERT','UPDATE') 
	and (pay_group_id,kca_seq_id) in 
	(select pay_group_id,max(kca_seq_id) from bec_ods_stg.ap_pay_group 
     where kca_operation IN ('INSERT','UPDATE')
     group by pay_group_id)	
);

commit;

-- Soft delete
update bec_ods.ap_pay_group set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ap_pay_group set IS_DELETED_FLG = 'Y'
where (pay_group_id)  in
(
select pay_group_id from bec_raw_dl_ext.ap_pay_group
where (pay_group_id,KCA_SEQ_ID)
in 
(
select pay_group_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_pay_group
group by pay_group_id
) 
and kca_operation= 'DELETE'
);
commit;

end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'ap_pay_group';

commit;