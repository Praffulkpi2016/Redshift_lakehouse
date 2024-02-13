/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents common load approach.
# File Version: KPI v1.0
*/
begin;
drop table if exists BEC_SECURITY.EDP_USER_SECURITY;

CREATE TABLE  BEC_SECURITY.EDP_USER_SECURITY 
	diststyle all sortkey(USER_NAME,ENTITY_TYPE)
as
(select
	eus.user_name ,
	tu.email,
	eus.entity_type ,
	eus.entity_id::integer,
	sysdate load_date
from
	bec_security.ebs_user_security eus,
	bec_security.tableau_users tu
where 1=1
and	upper(eus.email_address) = upper(tu.email)
);
commit;

INSERT INTO BEC_SECURITY.EDP_USER_SECURITY (USER_NAME,EMAIL,ENTITY_TYPE,ENTITY_ID,LOAD_DATE) 
 select B.USER_NAME,B.EMAIL,A.ENTITY_TYPE, A.ENTITY_ID::integer, GETDATE() as LOAD_DATE 
 from BEC_SECURITY.EDP_USER_ROLES A,BEC_SECURITY.EDP_SECURITY_STAGE B
 where A.USER_ROLE = B.USER_ROLE;
 
commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate(),load_type='I'
WHERE
    dw_table_name = 'edp_user_security'
and batch_name = 'security';

COMMIT;