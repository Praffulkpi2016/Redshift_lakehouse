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
	
	drop table if exists bec_dwh.DIM_AP_VENDOR_COUNTRY;
	
	create table bec_dwh.DIM_AP_VENDOR_COUNTRY  diststyle all  sortkey(vendor_id)
	as
	(
		SELECT
		vendor_id,
		VENDOR_NAME,
		territory_short_name,
		TERRITORY_CODE,
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
		|| '-'
		|| nvl(vendor_id, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
		FROM
		
		(
			SELECT
			aps.vendor_id,
			aps.VENDOR_NAME,
			ft.territory_short_name,
			ft.TERRITORY_CODE
			FROM
			bec_ods.hz_parties hp,
			bec_ods.ap_suppliers aps,
			bec_ods.fnd_territories_tl ft
			WHERE
			aps.party_id = hp.party_id
			AND hp.country IS NOT NULL
			--AND aps.vendor_id = pasl.vendor_id
			AND hp.country = ft.territory_code
		AND ft.language = 'US') 
		
	);
end;

update
bec_etl_ctrl.batch_dw_info
set
load_type = 'I',
last_refresh_date = getdate()
where
dw_table_name = 'dim_ap_vendor_country'
and batch_name = 'ascp';

commit;
