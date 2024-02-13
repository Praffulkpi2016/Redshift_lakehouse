/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/


begin;

-- Delete Records

delete from bec_dwh.DIM_AR_CONTACTS
where nvl(contact_id,0) in (
select nvl(ods.contact_id,0)  from bec_dwh.DIM_AR_CONTACTS dw, bec_ods.RA_CONTACTS ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.contact_id,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_contacts' and batch_name = 'ar')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_AR_CONTACTS
(
contact_id,
	sold_to_contact_name,
	contact_number,
	email_address,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	job_title,
	creation_date,
	last_update_date,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select 
	con.contact_id,
          con.first_name || ' ' || con.last_name "SOLD_TO_CONTACT_NAME",
          con.contact_number, 
          con.email_address, 
          con.attribute1,
          con.attribute2, 
          con.attribute3, 
          con.attribute4, 
          con.attribute5,
          con.job_title, 
		  TRUNC (con.creation_date) "CREATION_DATE",
          TRUNC (con.last_update_date) "LAST_UPDATE_DATE",
	'N' as is_deleted_flg,
 (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(con.CONTACT_ID,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
 bec_ods.RA_CONTACTS con
 where 1=1
 and (con.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_contacts' and batch_name = 'ar')
 )
 );

-- Soft delete

update bec_dwh.DIM_AR_CONTACTS set is_deleted_flg = 'Y'
where nvl(contact_id,0) not in (
select nvl(ods.contact_id,0) from bec_dwh.DIM_AR_CONTACTS dw, bec_raw_dl_ext.RA_CONTACTS ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.contact_id,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ar_contacts' and batch_name = 'ar';

COMMIT;