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

delete from bec_ods.AP_INV_APRVL_HIST_ALL
where (nvl(APPROVAL_HISTORY_ID, '0'))  in (
select nvl(stg.APPROVAL_HISTORY_ID, '0') as APPROVAL_HISTORY_ID
             from bec_ods.AP_INV_APRVL_HIST_ALL ods, bec_ods_stg.AP_INV_APRVL_HIST_ALL stg
where nvl(ods.APPROVAL_HISTORY_ID, '0') = nvl(stg.APPROVAL_HISTORY_ID, '0')

and stg.kca_operation IN ('INSERT','UPDATE')
);



commit;

-- Insert records

insert into	bec_ods.AP_INV_APRVL_HIST_ALL
       (
       APPROVAL_HISTORY_ID,
INVOICE_ID,
ITERATION,
RESPONSE,
APPROVER_ID,
APPROVER_NAME,
AMOUNT_APPROVED,
APPROVER_COMMENTS,
CREATED_BY,
CREATION_DATE,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
ORG_ID,
NOTIFICATION_ORDER,
ORIG_SYSTEM,
ITEM_CLASS,
ITEM_ID,
LINE_NUMBER,
HOLD_ID,
HISTORY_TYPE,
"APPROVER_COMMENTS#1",
"LINE_NUMBER#1",
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)
(
	select
		APPROVAL_HISTORY_ID,
INVOICE_ID,
ITERATION,
RESPONSE,
APPROVER_ID,
APPROVER_NAME,
AMOUNT_APPROVED,
APPROVER_COMMENTS,
CREATED_BY,
CREATION_DATE,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
ORG_ID,
NOTIFICATION_ORDER,
ORIG_SYSTEM,
ITEM_CLASS,
ITEM_ID,
LINE_NUMBER,
HOLD_ID,
HISTORY_TYPE,
"APPROVER_COMMENTS#1",
"LINE_NUMBER#1",
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.AP_INV_APRVL_HIST_ALL
	where kca_operation IN ('INSERT','UPDATE')
	and (nvl(APPROVAL_HISTORY_ID, '0'),kca_seq_id) in
	(select nvl(APPROVAL_HISTORY_ID, '0') as APPROVAL_HISTORY_ID,
	max(kca_seq_id) from bec_ods_stg.AP_INV_APRVL_HIST_ALL
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(APPROVAL_HISTORY_ID, '0'))
);

commit;

-- Soft delete
update bec_ods.AP_INV_APRVL_HIST_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AP_INV_APRVL_HIST_ALL set IS_DELETED_FLG = 'Y'
where (APPROVAL_HISTORY_ID)  in
(
select APPROVAL_HISTORY_ID from bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
where (APPROVAL_HISTORY_ID,KCA_SEQ_ID)
in 
(
select APPROVAL_HISTORY_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
group by APPROVAL_HISTORY_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ap_inv_aprvl_hist_all';

commit;