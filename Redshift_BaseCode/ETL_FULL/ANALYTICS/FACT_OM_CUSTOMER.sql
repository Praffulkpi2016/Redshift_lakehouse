/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.FACT_OM_CUSTOMER;

create table bec_dwh.FACT_OM_CUSTOMER distkey(dw_load_id) 
sortkey (CUST_ACCOUNT_ID,SITE_USE_ID) 
as 
(
SELECT  
 BCDV.PARTY_SITE_ID
,BCDV.CUST_ACCOUNT_ID
,HCSU.ORG_ID
,BCDV.PARTY_NAME		CUSTOMER_NAME
,BCDV.ACCOUNT_NUMBER	CUSTOMER_NUMBER
,BCDV.ADDRESS2			SITE_ADDRESS2
,DECODE(HCAL.STATUS,'A','Active','I','Inactive')			CUSTOMER_STATUS
,HCPC.NAME				CUSTOMER_PROFILE_CLASS
,HPS.PARTY_SITE_NUMBER	SITE_NUMBER
,HCSU.SITE_USE_CODE		BUSINESS_PURPOSE
,HL.ADDRESS1			SITE_ID
,HCSU.PRIMARY_FLAG		PRIMARY_SITE
, NVL(HL.ADDRESS1,'NA')
|| '-'
|| NVL(HL.ADDRESS2,'NA')
|| '-'
|| NVL(HL.CITY,'NA')
|| '-'
|| NVL(HL.STATE,'NA')
|| '-'
|| NVL(HL.POSTAL_CODE,'NA')
|| '-'
|| NVL(HL.COUNTRY,'NA')
 SITE_ADDRESS
,HL.STATE	
,HL.COUNTRY
,HL.COUNTY
,HL.POSTAL_CODE	
--,HCSU.STATUS			SITE_STATUS
,DECODE(HCSU.STATUS,'A','Active','I','Inactive') 			SITE_STATUS
,HP.CREATION_DATE		CREATION_DATE
,HP.LAST_UPDATE_DATE
,HP.LAST_UPDATED_BY
,HP.CREATED_BY CUSTOMER_CREATED_BY
,HP.PARTY_TYPE  CUSTOMER_TYPE
--added columns for quicksite
,BCDV.SITE_USE_ID
,BCDV.party_number
,BCDV.party_site_number
,BCDV.address3
,BCDV.address4
,BCDV.address5
,BCDV.city
,BCDV.location
,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || BCDV.PARTY_SITE_ID PARTY_SITE_ID_KEY
		,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || BCDV.CUST_ACCOUNT_ID CUST_ACCOUNT_ID_KEY
		,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || HCSU.ORG_ID  ORG_ID_KEY		,
			-- audit columns
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	    (
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
	 || '-' || nvl(BCDV.DW_LOAD_ID,'NA')
	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM   
(select * from  bec_dwh.DIM_CUSTOMER_DETAILS where is_deleted_flg <> 'Y')	BCDV
,(select * from bec_ods.HZ_PARTY_SITES 			where is_deleted_flg <> 'Y')    HPS
,(select * from bec_ods.HZ_LOCATIONS 			where is_deleted_flg <> 'Y')	HL
,(select * from bec_ods.HZ_CUST_ACCOUNTS 		where is_deleted_flg <> 'Y')    HCAL
,(select * from bec_ods.HZ_CUST_ACCOUNTS 		where is_deleted_flg <> 'Y')	HCA
,(select * from bec_ods.HZ_PARTIES              where is_deleted_flg <> 'Y')    HP
,(select * from bec_ods.HZ_CUST_SITE_USES_ALL 	where is_deleted_flg <> 'Y')	HCSU
,(select * from bec_ods.HZ_CUSTOMER_PROFILES 	where is_deleted_flg <> 'Y')	HCP
,(select * from bec_ods.HZ_CUST_PROFILE_CLASSES where is_deleted_flg <> 'Y')	HCPC    
WHERE  1=1
AND BCDV.PARTY_SITE_ID = HPS.PARTY_SITE_ID(+)
AND BCDV.PARTY_SITE_NUMBER = HPS.PARTY_SITE_NUMBER(+)
AND HPS.LOCATION_ID = HL.LOCATION_ID
AND BCDV.CUST_ACCOUNT_ID = HCAL.CUST_ACCOUNT_ID(+)
AND BCDV.CUST_ACCOUNT_ID = HCA.CUST_ACCOUNT_ID(+)
AND HCA.PARTY_ID         = HP.PARTY_ID(+)
AND BCDV.SITE_USE_ID = HCSU.SITE_USE_ID(+)
--AND BCDV.SITE_USE_ID  = HCP.SITE_USE_ID(+)
AND HCA.PARTY_ID  = HCP.PARTY_ID 
AND BCDV.CUST_ACCOUNT_ID = HCP.CUST_ACCOUNT_ID(+)
AND HCP.SITE_USE_ID(+) IS NULL
AND HCP.PROFILE_CLASS_ID = HCPC.PROFILE_CLASS_ID(+)
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_om_customer'
	and batch_name = 'om';

commit;