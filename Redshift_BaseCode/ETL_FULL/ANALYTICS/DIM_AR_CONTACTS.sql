/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/



begin;
drop table if exists bec_dwh.DIM_AR_CONTACTS;

CREATE TABLE  bec_dwh.DIM_AR_CONTACTS 
	diststyle all sortkey(contact_id)
as
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
from BEC_ODS.RA_CONTACTS  con
);

end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_ar_contacts'
	and batch_name = 'ar';

commit;