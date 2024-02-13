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
drop table if exists BEC_SECURITY.EBS_USER_SECURITY;

CREATE TABLE  BEC_SECURITY.EBS_USER_SECURITY 
	diststyle all sortkey(USER_NAME,ENTITY_TYPE)
as
select USER_NAME,
	   EMAIL_ADDRESS,
	   decode(ENTITY_TYPE,'Inv Org','INV_ORG',upper(ENTITY_TYPE)) ENTITY_TYPE,
	   ENTITY_ID,
	   GETDATE() as DW_LOAD_DATE
from
(
select	DISTINCT
	FND_USER.USER_NAME	   USER_NAME,
	FND_USER.EMAIL_ADDRESS ,
	DECODE(SQ_FND_RESPONSIBILITY.PROFILE_OPTION_ID, '1991','OU','1201', 'Ledger',null  )::varchar(240) ENTITY_TYPE,
	SQ_FND_RESPONSIBILITY.PROFILE_OPTION_VALUE	   ENTITY_ID
from	bec_ods.FND_USER_RESP_GROUPS_DIRECT   FND_USER_RESP_GROUPS_DIRECT
       ,bec_ods.FND_USER   FND_USER
       ,bec_ods.HR_ALL_ORGANIZATION_UNITS   HR_ORGANIZATION_UNITS 
       ,bec_ods.GL_LEDGERS LED 
       ,(SELECT 	
			FND_RESPONSIBILITY.RESPONSIBILITY_ID RESPONSIBILITY_ID,
			FND_PROFILE_OPTION_VALUES.PROFILE_OPTION_VALUE PROFILE_OPTION_VALUE,
			FND_PROFILE_OPTION_VALUES.PROFILE_OPTION_ID PROFILE_OPTION_ID,
			FND_PROFILE_OPTION_VALUES.LEVEL_VALUE LEVEL_VALUE
		FROM
			bec_ods.FND_RESPONSIBILITY FND_RESPONSIBILITY,
			bec_ods.FND_PROFILE_OPTION_VALUES FND_PROFILE_OPTION_VALUES
		WHERE
			(1 = 1)
			AND (FND_RESPONSIBILITY.RESPONSIBILITY_ID = FND_PROFILE_OPTION_VALUES.LEVEL_VALUE (+))
			AND FND_RESPONSIBILITY.APPLICATION_ID = 101
			AND SYSDATE between start_date and nvl(end_date,sysdate+1) 
		) SQ_FND_RESPONSIBILITY
WHERE
	(1 = 1)
	AND ((
	FND_USER_RESP_GROUPS_DIRECT.END_DATE IS NULL
	and	 SQ_FND_RESPONSIBILITY.PROFILE_OPTION_ID = '1201'
		OR FND_USER_RESP_GROUPS_DIRECT.END_DATE >= TRUNC(GETDATE())
		))
	AND (FND_USER_RESP_GROUPS_DIRECT.RESPONSIBILITY_ID = SQ_FND_RESPONSIBILITY.RESPONSIBILITY_ID)
	AND (SQ_FND_RESPONSIBILITY.PROFILE_OPTION_VALUE = (HR_ORGANIZATION_UNITS.ORGANIZATION_ID)::varchar
         OR SQ_FND_RESPONSIBILITY.PROFILE_OPTION_VALUE = (LED.LEDGER_ID)::varchar
)
	AND (FND_USER_RESP_GROUPS_DIRECT.USER_ID = FND_USER.USER_ID)
UNION
select	DISTINCT
	FND_USER.USER_NAME	   USER_NAME,
	FND_USER.EMAIL_ADDRESS ,
	DECODE(SQ_FND_RESPONSIBILITY.PROFILE_OPTION_ID, '9930', 'Ledger',null  )::varchar(240) ENTITY_TYPE,
	LED.ledger_id::varchar(240)	   ENTITY_ID
from	bec_ods.FND_USER_RESP_GROUPS_DIRECT   FND_USER_RESP_GROUPS_DIRECT
       ,bec_ods.FND_USER   FND_USER
       ,bec_ods.HR_ALL_ORGANIZATION_UNITS   HR_ORGANIZATION_UNITS 
       ,bec_ods.GL_LEDGERS LED 
	   ,bec_ods.gl_access_set_ledgers access_led
       ,(SELECT 	
			FND_RESPONSIBILITY.RESPONSIBILITY_ID RESPONSIBILITY_ID,
			FND_PROFILE_OPTION_VALUES.PROFILE_OPTION_VALUE PROFILE_OPTION_VALUE,
			FND_PROFILE_OPTION_VALUES.PROFILE_OPTION_ID PROFILE_OPTION_ID,
			FND_PROFILE_OPTION_VALUES.LEVEL_VALUE LEVEL_VALUE
		FROM
			bec_ods.FND_RESPONSIBILITY FND_RESPONSIBILITY,
			bec_ods.FND_PROFILE_OPTION_VALUES FND_PROFILE_OPTION_VALUES
		WHERE
			(1 = 1)
			AND (FND_RESPONSIBILITY.RESPONSIBILITY_ID = FND_PROFILE_OPTION_VALUES.LEVEL_VALUE )
			AND FND_RESPONSIBILITY.APPLICATION_ID = 101
			AND FND_PROFILE_OPTION_VALUES.PROFILE_OPTION_ID = '9930'
			AND SYSDATE between start_date and nvl(end_date,sysdate+1) 
		) SQ_FND_RESPONSIBILITY
WHERE
	(1 = 1)
	AND ((
	FND_USER_RESP_GROUPS_DIRECT.END_DATE IS NULL
		OR FND_USER_RESP_GROUPS_DIRECT.END_DATE >= TRUNC(GETDATE())
		))
	AND (FND_USER_RESP_GROUPS_DIRECT.RESPONSIBILITY_ID = SQ_FND_RESPONSIBILITY.RESPONSIBILITY_ID)
	AND SQ_FND_RESPONSIBILITY.PROFILE_OPTION_VALUE = (access_led.access_set_id)::varchar
    AND access_led.ledger_id = LED.ledger_id
	AND (FND_USER_RESP_GROUPS_DIRECT.USER_ID = FND_USER.USER_ID)
UNION
select	DISTINCT
	FND_USER.USER_NAME	   USER_NAME,
	FND_USER.EMAIL_ADDRESS ,
	DECODE(SQ_FND_RESPONSIBILITY.PROFILE_OPTION_ID, '1991','OU','1201', 'Ledger',null  )::varchar(240) ENTITY_TYPE,
	SQ_FND_RESPONSIBILITY.PROFILE_OPTION_VALUE	   ENTITY_ID
from	bec_ods.FND_USER_RESP_GROUPS_DIRECT   FND_USER_RESP_GROUPS_DIRECT
       ,bec_ods.FND_USER   FND_USER
       ,bec_ods.HR_ALL_ORGANIZATION_UNITS   HR_ORGANIZATION_UNITS 
       ,bec_ods.GL_LEDGERS LED 
       ,(SELECT 	
			FND_RESPONSIBILITY.RESPONSIBILITY_ID RESPONSIBILITY_ID,
			FND_PROFILE_OPTION_VALUES.PROFILE_OPTION_VALUE PROFILE_OPTION_VALUE,
			FND_PROFILE_OPTION_VALUES.PROFILE_OPTION_ID PROFILE_OPTION_ID,
			FND_PROFILE_OPTION_VALUES.LEVEL_VALUE LEVEL_VALUE
		FROM
			bec_ods.FND_RESPONSIBILITY FND_RESPONSIBILITY,
			bec_ods.FND_PROFILE_OPTION_VALUES FND_PROFILE_OPTION_VALUES
		WHERE
			(1 = 1)
			AND (FND_RESPONSIBILITY.RESPONSIBILITY_ID = FND_PROFILE_OPTION_VALUES.LEVEL_VALUE (+))
			AND FND_RESPONSIBILITY.APPLICATION_ID != 101
			AND SYSDATE between start_date and nvl(end_date,sysdate+1) 
		) SQ_FND_RESPONSIBILITY
WHERE
	(1 = 1)
	AND ((
	FND_USER_RESP_GROUPS_DIRECT.END_DATE IS NULL
	and	 SQ_FND_RESPONSIBILITY.PROFILE_OPTION_ID ='1991'
		OR FND_USER_RESP_GROUPS_DIRECT.END_DATE >= TRUNC(GETDATE())
		))
	AND (FND_USER_RESP_GROUPS_DIRECT.RESPONSIBILITY_ID = SQ_FND_RESPONSIBILITY.RESPONSIBILITY_ID)
	AND (SQ_FND_RESPONSIBILITY.PROFILE_OPTION_VALUE = (HR_ORGANIZATION_UNITS.ORGANIZATION_ID)::varchar
         OR SQ_FND_RESPONSIBILITY.PROFILE_OPTION_VALUE = (LED.LEDGER_ID)::varchar
)
	AND (FND_USER_RESP_GROUPS_DIRECT.USER_ID = FND_USER.USER_ID)
UNION
 select	DISTINCT
	FND_USER.USER_NAME	   USER_NAME,
	FND_USER.EMAIL_ADDRESS ,
	'Inv Org' ENTITY_TYPE,
	OOD.ORGANIZATION_ID::varchar(240)	   ENTITY_ID
from	bec_ods.FND_USER_RESP_GROUPS_DIRECT   FND_USER_RESP_GROUPS_DIRECT
       ,bec_ods.FND_USER   FND_USER
       ,bec_ods.HR_ALL_ORGANIZATION_UNITS   HR_ORGANIZATION_UNITS 
       ,bec_ods.ORG_ORGANIZATION_DEFINITIONS OOD 
       ,(SELECT 	
			FND_RESPONSIBILITY.RESPONSIBILITY_ID RESPONSIBILITY_ID,
			FND_PROFILE_OPTION_VALUES.PROFILE_OPTION_VALUE PROFILE_OPTION_VALUE,
			FND_PROFILE_OPTION_VALUES.PROFILE_OPTION_ID PROFILE_OPTION_ID,
			FND_PROFILE_OPTION_VALUES.LEVEL_VALUE LEVEL_VALUE
		FROM
			bec_ods.FND_RESPONSIBILITY FND_RESPONSIBILITY,
			bec_ods.FND_PROFILE_OPTION_VALUES FND_PROFILE_OPTION_VALUES
		WHERE
			(1 = 1)
			AND (FND_RESPONSIBILITY.RESPONSIBILITY_ID = FND_PROFILE_OPTION_VALUES.LEVEL_VALUE (+))
			AND SYSDATE between start_date and nvl(end_date,sysdate+1) 
		) SQ_FND_RESPONSIBILITY
WHERE
	(1 = 1)
	AND ((FND_USER_RESP_GROUPS_DIRECT.END_DATE IS NULL
		AND SQ_FND_RESPONSIBILITY.PROFILE_OPTION_ID ='1991'
		OR FND_USER_RESP_GROUPS_DIRECT.END_DATE >= TRUNC(GETDATE())))
	AND (FND_USER_RESP_GROUPS_DIRECT.RESPONSIBILITY_ID = SQ_FND_RESPONSIBILITY.RESPONSIBILITY_ID)
	AND (SQ_FND_RESPONSIBILITY.PROFILE_OPTION_VALUE = (HR_ORGANIZATION_UNITS.ORGANIZATION_ID::varchar))
    AND (HR_ORGANIZATION_UNITS.ORGANIZATION_ID)::varchar = (OOD.OPERATING_UNIT)::varchar
	AND FND_USER_RESP_GROUPS_DIRECT.USER_ID = FND_USER.USER_ID	
)
where 1=1
and ENTITY_TYPE is not NULL
;
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate(),load_type='I'
WHERE
    dw_table_name = 'ebs_user_security'
and batch_name = 'security';

COMMIT;