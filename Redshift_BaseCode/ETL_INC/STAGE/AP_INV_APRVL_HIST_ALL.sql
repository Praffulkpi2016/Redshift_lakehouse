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

truncate table bec_ods_stg.AP_INV_APRVL_HIST_ALL;

insert into	bec_ods_stg.AP_INV_APRVL_HIST_ALL
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
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (
	    nvl(APPROVAL_HISTORY_ID, '0'),

	kca_seq_id) in
	(select 
	nvl(APPROVAL_HISTORY_ID, '0') as APPROVAL_HISTORY_ID,

	max(kca_seq_id) from bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by 
        nvl(APPROVAL_HISTORY_ID, '0')
        )
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ap_inv_aprvl_hist_all')
);
end;
