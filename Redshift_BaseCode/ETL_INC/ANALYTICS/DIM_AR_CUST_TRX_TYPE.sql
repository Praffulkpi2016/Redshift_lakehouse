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

delete
from
	bec_dwh.DIM_AR_CUST_TRX_TYPE
where
	(nvl(CUST_TRX_TYPE_ID,0),
	nvl(ORG_ID, 0)) in (
	select
		nvl(ods.CUST_TRX_TYPE_ID,0),
		nvl(ods.ORG_ID, 0) as ORG_ID
	from
		bec_dwh.DIM_AR_CUST_TRX_TYPE dw,
		bec_ods.RA_CUST_TRX_TYPES_ALL ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.CUST_TRX_TYPE_ID, 0)
|| '-' || nvl(ods.ORG_ID, 0)
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_cust_trx_type'
				and batch_name = 'ar')
			 )
);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_AR_CUST_TRX_TYPE
(
cust_trx_type_id,
	last_update_date,
	status,
	cust_trx_type_name,
	cust_trx_type_description,
	cust_trx_type,
	set_of_books_id,
	end_date,
	start_date,
	org_id,
	legal_entity_id,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
		CUST_TRX_TYPE_ID,
		LAST_UPDATE_DATE,
		STATUS,
		NAME CUST_TRX_TYPE_NAME,
		DESCRIPTION CUST_TRX_TYPE_DESCRIPTION,
		"TYPE" CUST_TRX_TYPE,
		SET_OF_BOOKS_ID,
		END_DATE,
		START_DATE,
		ORG_ID,
		LEGAL_ENTITY_ID ,
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
    || '-' || nvl(CUST_TRX_TYPE_ID, 0)
	|| '-' || nvl(ORG_ID, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		bec_ods.RA_CUST_TRX_TYPES_ALL
	where
1=1
			and (kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_cust_trx_type'
				and batch_name = 'ar')
			 )
 );
-- Soft delete

update
	bec_dwh.DIM_AR_CUST_TRX_TYPE
set
	is_deleted_flg = 'Y'
where
	(nvl(CUST_TRX_TYPE_ID,0),
	nvl(ORG_ID, 0)) not in (
	select
		nvl(ods.CUST_TRX_TYPE_ID,0),
		nvl(ods.ORG_ID, 0) as ORG_ID
	from
		bec_dwh.DIM_AR_CUST_TRX_TYPE dw,
		bec_ods.RA_CUST_TRX_TYPES_ALL ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.CUST_TRX_TYPE_ID, 0)
|| '-' || nvl(ods.ORG_ID, 0)
			and ods.is_deleted_flg <> 'Y');

commit;
end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ar_cust_trx_type' and batch_name = 'ar';

COMMIT;