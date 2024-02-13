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

delete from bec_ods.fnd_user
where USER_ID in (
select stg.USER_ID from bec_ods.fnd_user ods, bec_ods_stg.fnd_user stg
where ods.USER_ID = stg.USER_ID  and stg.kca_operation IN ('INSERT','UPDATE') );

commit;

--DELETE FROM bec_ods.fnd_user WHERE USER_ID IN (SELECT USER_ID FROM bec_ods_stg.fnd_user);

insert into bec_ods.fnd_user
(
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
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID,
	kca_seq_date)
(
select 
distinct
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
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.fnd_user
where kca_operation IN ('INSERT','UPDATE') and (USER_ID,kca_seq_id) in (select USER_ID,max(kca_seq_id) from bec_ods_stg.fnd_user 
where kca_operation IN ('INSERT','UPDATE')
group by USER_ID)
);

commit;

--soft delete
update bec_ods.fnd_user set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fnd_user set IS_DELETED_FLG = 'Y'
where (USER_ID)  in
(
select USER_ID from bec_raw_dl_ext.fnd_user
where (USER_ID,KCA_SEQ_ID)
in 
(
select USER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fnd_user
group by USER_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;



update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='fnd_user';
commit;

