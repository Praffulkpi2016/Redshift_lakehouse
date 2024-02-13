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

drop table if exists bec_dwh.DIM_CUSTOMER_DETAILS;

create table bec_dwh.DIM_CUSTOMER_DETAILS 
distkey(CUST_ACCOUNT_ID)
sortkey (CUST_ACCT_SITE_ID,SITE_USE_ID) 
as 
(
SELECT
HP.PARTY_NAME,
HP.PARTY_NUMBER,
HPS.PARTY_SITE_NUMBER,
HPS.PARTY_SITE_ID,
HL.ADDRESS1,
HL.ADDRESS2,
HL.ADDRESS3,
HL.ADDRESS4,
DECODE(HL.CITY, NULL, NULL, HL.CITY || ', ')
|| DECODE(HL.STATE, NULL, HL.PROVINCE || ', ', HL.STATE || ', ')
|| DECODE(HL.POSTAL_CODE, NULL, NULL, HL.POSTAL_CODE || ', ')
|| DECODE(HL.COUNTRY, NULL, NULL, HL.COUNTRY) ADDRESS5,
HL.COUNTY,
HL.CITY,
HL.COUNTRY,
HL.STATE,
HL.POSTAL_CODE,
HCA.CUST_ACCOUNT_ID,
HCSU.SITE_USE_ID,
HCA.ACCOUNT_NUMBER,
HCSU.LOCATION,
HCSU.SITE_USE_CODE,
HCSU.ORIG_SYSTEM_REFERENCE,
HCS.CUST_ACCT_SITE_ID,
'N' AS IS_DELETED_FLG,
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
   || '-' || nvl(HCS.CUST_ACCT_SITE_ID, 0)
   || '-' || nvl(HCSU.SITE_USE_ID, 0) 
   as dw_load_id, 
getdate() as dw_insert_date,
getdate() as dw_update_date
FROM
BEC_ODS.HZ_PARTIES             HP,
BEC_ODS.HZ_PARTY_SITES         HPS,
BEC_ODS.HZ_LOCATIONS           HL,
BEC_ODS.HZ_CUST_ACCOUNTS  HCA,
BEC_ODS.HZ_CUST_ACCT_SITES_ALL HCS,
BEC_ODS.HZ_CUST_SITE_USES_ALL  HCSU
WHERE HP.PARTY_ID = HPS.PARTY_ID
AND HPS.LOCATION_ID = HL.LOCATION_ID
AND HP.PARTY_ID = HCA.PARTY_ID
AND HCA.CUST_ACCOUNT_ID = HCS.CUST_ACCOUNT_ID
AND HCS.CUST_ACCT_SITE_ID = HCSU.CUST_ACCT_SITE_ID
AND HCS.PARTY_SITE_ID = HPS.PARTY_SITE_ID
AND HPS.STATUS = 'A'
AND HCSU.STATUS = 'A'
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_customer_details'
	and batch_name = 'om';

commit;