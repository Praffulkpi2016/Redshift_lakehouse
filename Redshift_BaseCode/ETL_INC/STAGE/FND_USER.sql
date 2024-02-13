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
	table bec_ods_stg.fnd_user;

insert
	into
	bec_ods_stg.fnd_user
(USER_ID,
USER_NAME,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
--LAST_UPDATE_LOGIN,
ENCRYPTED_FOUNDATION_PASSWORD,
ENCRYPTED_USER_PASSWORD,
--SESSION_NUMBER,
START_DATE,
END_DATE,
DESCRIPTION,
--LAST_LOGON_DATE,
PASSWORD_DATE,
PASSWORD_ACCESSES_LEFT,
PASSWORD_LIFESPAN_ACCESSES,
PASSWORD_LIFESPAN_DAYS,
EMPLOYEE_ID,
EMAIL_ADDRESS,
FAX,
CUSTOMER_ID,
SUPPLIER_ID,
WEB_PASSWORD,
GCN_CODE_COMBINATION_ID,
PERSON_PARTY_ID
,kca_operation
,kca_seq_id
,kca_seq_date)
(
	select
		USER_ID,
USER_NAME,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
--LAST_UPDATE_LOGIN,
ENCRYPTED_FOUNDATION_PASSWORD,
ENCRYPTED_USER_PASSWORD,
--SESSION_NUMBER,
START_DATE,
END_DATE,
DESCRIPTION,
--LAST_LOGON_DATE,
PASSWORD_DATE,
PASSWORD_ACCESSES_LEFT,
PASSWORD_LIFESPAN_ACCESSES,
PASSWORD_LIFESPAN_DAYS,
EMPLOYEE_ID,
EMAIL_ADDRESS,
FAX,
CUSTOMER_ID,
SUPPLIER_ID,
WEB_PASSWORD,
GCN_CODE_COMBINATION_ID,
PERSON_PARTY_ID
,kca_operation
,kca_seq_id
,kca_seq_date
	from
		bec_raw_dl_ext.fnd_user
where  kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' and (user_id,kca_seq_id) in (select user_id,max(kca_seq_id) from bec_raw_dl_ext.FND_USER 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
group by user_id) and
		(kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fnd_user') 
            )
			
			
			 
);
end;
