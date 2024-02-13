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

drop table if exists bec_dwh.DIM_SC_CUSTOMERS;

commit;

create table bec_dwh.DIM_SC_CUSTOMERS 
	diststyle all
	sortkey (site_use_id)
as
(
select
	HP.PARTY_NAME Customer_name,
    hl.address1 Site_id,
    nvl(hl.address1,'NA')||' '||nvl(hl.address2,'NA')||' '||nvl(hl.city,'NA')||' '||nvl(hl.county,'NA')||' '||nvl(hl.state,'NA')
	||' '||nvl(hl.country,'NA')
	||' '||nvl(hl.postal_code,'NA') Site_Address,
    hl.address_lines_phonetic geo_code,
    hcsu.site_use_id,
    hcas.org_id,
	-- audit columns
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || nvl(site_use_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM bec_ods.hz_parties hp,
     bec_ods.hz_party_sites hps,
     bec_ods.hz_cust_accounts hca,
     bec_ods.hz_cust_acct_sites_all hcas,
     bec_ods.hz_cust_site_uses_all hcsu,
     bec_ods.hz_locations hl
WHERE 1 = 1
  AND hp.party_id            = hca.party_id
  AND hca.cust_account_id    = hcas.cust_account_id(+)
  AND hps.party_site_id(+)   = hcas.party_site_id
  AND hcas.cust_acct_site_id = hcsu.cust_acct_site_id
  AND hps.location_id = hl.location_id(+)
  AND hp.party_type = 'ORGANIZATION' 
  AND hp.status     = 'A' 
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_sc_customers'
	and batch_name = 'sc';

commit;