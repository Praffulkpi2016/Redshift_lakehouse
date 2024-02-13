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

delete from 
  bec_dwh.DIM_SC_CUSTOMERS 
where 
  (
    nvl(site_use_id, 0)
  ) in (
    select 
      nvl(ods.site_use_id, 0) as site_use_id 
    from 
      bec_dwh.DIM_SC_CUSTOMERS dw, 
      (
        select 
          hcsu.site_use_id 
        FROM 
          bec_ods.hz_parties hp, 
          bec_ods.hz_party_sites hps, 
          bec_ods.hz_cust_accounts hca, 
          bec_ods.hz_cust_acct_sites_all hcas, 
          bec_ods.hz_cust_site_uses_all hcsu, 
          bec_ods.hz_locations hl 
        WHERE 
          1 = 1 
          AND hp.party_id = hca.party_id 
          AND hca.cust_account_id = hcas.cust_account_id(+) 
          AND hps.party_site_id(+) = hcas.party_site_id 
          AND hcas.cust_acct_site_id = hcsu.cust_acct_site_id 
          AND hps.location_id = hl.location_id(+) 
          AND hp.party_type = 'ORGANIZATION' 
          AND hp.status = 'A' 
          and (
			hp.kca_seq_date > (select (executebegints - prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'dim_sc_customers' and batch_name = 'sc')
			or hcsu.kca_seq_date > (select (executebegints - prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'dim_sc_customers' and batch_name = 'sc')
			or hca.kca_seq_date > (select (executebegints - prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'dim_sc_customers' and batch_name = 'sc')
			)
      ) ods 
    where 
      dw.dw_load_id = (
        select 
          system_id 
        from 
          bec_etl_ctrl.etlsourceappid 
        where 
          source_system = 'EBS'
      ) || '-' || nvl(ods.site_use_id, 0)
  );
  
commit;

-- Insert records

insert into bec_dwh.DIM_SC_CUSTOMERS (
  Customer_name, 
  Site_id, 
  Site_Address, 
  geo_code, 
  site_use_id, 
  org_id, 
  is_deleted_flg, 
  source_app_id, 
  dw_load_id, 
  dw_insert_date, 
  dw_update_date
) (
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
        source_system = 'EBS'
    ) as source_app_id, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || nvl(site_use_id, 0) as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  FROM 
    bec_ods.hz_parties hp, 
    bec_ods.hz_party_sites hps, 
    bec_ods.hz_cust_accounts hca, 
    bec_ods.hz_cust_acct_sites_all hcas, 
    bec_ods.hz_cust_site_uses_all hcsu, 
    bec_ods.hz_locations hl 
  WHERE 
    1 = 1 
    AND hp.party_id = hca.party_id 
    AND hca.cust_account_id = hcas.cust_account_id(+) 
    AND hps.party_site_id(+) = hcas.party_site_id 
    AND hcas.cust_acct_site_id = hcsu.cust_acct_site_id 
    AND hps.location_id = hl.location_id(+) 
    AND hp.party_type = 'ORGANIZATION' 
    AND hp.status = 'A' 
    and (
    hp.kca_seq_date > (select (executebegints - prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'dim_sc_customers' and batch_name = 'sc')
	or hcsu.kca_seq_date > (select (executebegints - prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'dim_sc_customers' and batch_name = 'sc')
	or hca.kca_seq_date > (select (executebegints - prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'dim_sc_customers' and batch_name = 'sc')
    )
);

commit;

-- Soft delete
update 
  bec_dwh.DIM_SC_CUSTOMERS 
set 
  is_deleted_flg = 'Y' 
where 
  (
    nvl(site_use_id, 0)
  ) not in (
    select 
      nvl(ods.site_use_id, 0) as site_use_id 
    from 
      bec_dwh.DIM_SC_CUSTOMERS dw, 
      (
        select 
          hcsu.site_use_id 
        FROM (select * from bec_ods.hz_parties where is_deleted_flg <> 'Y') hp,
			 (select * from bec_ods.hz_party_sites where is_deleted_flg <> 'Y') hps,
			 (select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y') hca,
			 (select * from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y') hcas,
			 (select * from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y') hcsu,
			 (select * from bec_ods.hz_locations where is_deleted_flg <> 'Y') hl
		WHERE 1 = 1
		AND hp.party_id            = hca.party_id
		AND hca.cust_account_id    = hcas.cust_account_id(+)
		AND hps.party_site_id(+)   = hcas.party_site_id
		AND hcas.cust_acct_site_id = hcsu.cust_acct_site_id
		AND hps.location_id = hl.location_id(+)
		AND hp.party_type = 'ORGANIZATION' 
		AND hp.status     = 'A'
      ) ods 
    where 
      dw.dw_load_id = (
        select 
          system_id 
        from 
          bec_etl_ctrl.etlsourceappid 
        where 
          source_system = 'EBS'
      ) || '-' || nvl(ods.site_use_id, 0)
  );
  
commit;

end;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'dim_sc_customers' 
  and batch_name = 'sc';
commit;
