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
	bec_dwh.DIM_AR_PAYMENT_METHODS
where
	(nvl(RECEIPT_METHOD_ID, 0),
	nvl(RECEIPT_CLASS_ID, 0)) in
(
	select
		nvl(ods.RECEIPT_METHOD_ID, 0) as RECEIPT_METHOD_ID,
		nvl(ods.RECEIPT_CLASS_ID, 0) as RECEIPT_CLASS_ID
	from
		bec_dwh.DIM_AR_PAYMENT_METHODS dw,
		(
		select
			ARM.RECEIPT_METHOD_ID,
			ARC.RECEIPT_CLASS_ID
		from
			bec_ods.AR_RECEIPT_CLASSES ARC,
			bec_ods.AR_RECEIPT_METHODS ARM
		where
			ARC.RECEIPT_CLASS_ID = ARM.RECEIPT_CLASS_ID
			and ARM.END_DATE is null
			and (ARM.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_payment_methods'
				and batch_name = 'ar')
				or ARC.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_payment_methods'
				and batch_name = 'ar')
				 
				)
				) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.RECEIPT_METHOD_ID, 0)
|| '-' || nvl(ods.RECEIPT_CLASS_ID, 0)
);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_AR_PAYMENT_METHODS
(
receipt_method_id,
	payment_method_name,
	receipt_class_id,
	receipt_class_name,
	receipt_creation_method,
	remit_flag,
	creation_status,
	remit_method_code,
	last_update_date,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
		ARM.RECEIPT_METHOD_ID RECEIPT_METHOD_ID,
		ARM.NAME PAYMENT_METHOD_NAME ,
		ARM.RECEIPT_CLASS_ID RECEIPT_CLASS_ID,
		ARC.NAME RECEIPT_CLASS_NAME,
		ARC.CREATION_METHOD_CODE RECEIPT_CREATION_METHOD,
		ARC.REMIT_FLAG,
		ARC.CREATION_STATUS,
		ARC.REMIT_METHOD_CODE,
		ARM.last_update_date,
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
    || '-' || nvl(ARM.RECEIPT_METHOD_ID, 0) || '-' || nvl(ARM.RECEIPT_CLASS_ID, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		bec_ods.AR_RECEIPT_CLASSES ARC,
		bec_ods.AR_RECEIPT_METHODS ARM
	where
		1 = 1
		and ARC.RECEIPT_CLASS_ID = ARM.RECEIPT_CLASS_ID
		--AND ARM.RECEIPT_METHOD_ID = ARMAA.RECEIPT_METHOD_ID
		and ARM.END_DATE is null
			and (ARM.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_payment_methods'
				and batch_name = 'ar')
				or ARC.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_payment_methods'
				and batch_name = 'ar')
				)
 );
-- Soft delete

update
	bec_dwh.DIM_AR_PAYMENT_METHODS
set
	is_deleted_flg = 'Y'
where
	(RECEIPT_METHOD_ID,
	RECEIPT_CLASS_ID) not in (
	select
		ods.RECEIPT_METHOD_ID,
		ods.RECEIPT_CLASS_ID
	from
		bec_dwh.DIM_AR_PAYMENT_METHODS dw,
		(
		select
			ARM.RECEIPT_METHOD_ID,
			ARC.RECEIPT_CLASS_ID
		from
			(select * from bec_ods.AR_RECEIPT_CLASSES where is_deleted_flg <> 'Y') ARC,
			(select * from bec_ods.AR_RECEIPT_METHODS where is_deleted_flg <> 'Y') ARM
		where
			ARC.RECEIPT_CLASS_ID = ARM.RECEIPT_CLASS_ID
			and ARM.END_DATE is null) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.RECEIPT_METHOD_ID, 0)
|| '-' || nvl(ods.RECEIPT_CLASS_ID, 0)
);

commit;
end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ar_payment_methods' and batch_name = 'ar';

COMMIT;