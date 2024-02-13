/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
 
begin;
-- Delete Records

delete
from
	bec_ods.FA_RETIREMENTS
where
	(
	NVL(RETIREMENT_ID,0) 

	) in 
	(
	select
		NVL(stg.RETIREMENT_ID,0) AS RETIREMENT_ID 
	from
		bec_ods.FA_RETIREMENTS ods,
		bec_ods_stg.FA_RETIREMENTS stg
	where
	NVL(ods.RETIREMENT_ID,0) = NVL(stg.RETIREMENT_ID,0) 
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into bec_ods.FA_RETIREMENTS  (
	RETIREMENT_ID , 
    BOOK_TYPE_CODE  , 
    ASSET_ID , 
    TRANSACTION_HEADER_ID_IN , 
    DATE_RETIRED   , 
    DATE_EFFECTIVE   , 
    COST_RETIRED , 
    STATUS  , 
    LAST_UPDATE_DATE   , 
    LAST_UPDATED_BY , 
    RETIREMENT_PRORATE_CONVENTION  , 
    TRANSACTION_HEADER_ID_OUT , 
    UNITS  , 
    COST_OF_REMOVAL  , 
    NBV_RETIRED , 
    GAIN_LOSS_AMOUNT ,  
    PROCEEDS_OF_SALE , 
    GAIN_LOSS_TYPE_CODE , 
    RETIREMENT_TYPE_CODE , 
    ITC_RECAPTURED , 
    ITC_RECAPTURE_ID , 
    REFERENCE_NUM , 
    SOLD_TO , 
    TRADE_IN_ASSET_ID , 
    STL_METHOD_CODE , 
    STL_LIFE_IN_MONTHS , 
    STL_DEPRN_AMOUNT , 
    CREATED_BY , 
    CREATION_DATE  , 
    LAST_UPDATE_LOGIN , 
    ATTRIBUTE1 , 
    ATTRIBUTE2 , 
    ATTRIBUTE3 , 
    ATTRIBUTE4 , 
    ATTRIBUTE5 , 
    ATTRIBUTE6 , 
    ATTRIBUTE7 , 
    ATTRIBUTE8 , 
    ATTRIBUTE9 , 
    ATTRIBUTE10 , 
    ATTRIBUTE11 , 
    ATTRIBUTE12 , 
    ATTRIBUTE13 , 
    ATTRIBUTE14 , 
    ATTRIBUTE15 , 
    ATTRIBUTE_CATEGORY_CODE , 
    REVAL_RESERVE_RETIRED , 
    UNREVALUED_COST_RETIRED , 
    BONUS_RESERVE_RETIRED , 
    RECOGNIZE_GAIN_LOSS , 
    REDUCTION_RATE , 
    RECAPTURE_RESERVE_FLAG , 
    LIMIT_PROCEEDS_FLAG , 
    TERMINAL_GAIN_LOSS , 
    RESERVE_RETIRED , 
    RECAPTURE_AMOUNT , 
    EOFY_RESERVE , 
    IMPAIR_RESERVE_RETIRED , 
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
	RETIREMENT_ID , 
    BOOK_TYPE_CODE  , 
    ASSET_ID , 
    TRANSACTION_HEADER_ID_IN , 
    DATE_RETIRED   , 
    DATE_EFFECTIVE   , 
    COST_RETIRED , 
    STATUS  , 
    LAST_UPDATE_DATE   , 
    LAST_UPDATED_BY , 
    RETIREMENT_PRORATE_CONVENTION  , 
    TRANSACTION_HEADER_ID_OUT , 
    UNITS  , 
    COST_OF_REMOVAL  , 
    NBV_RETIRED , 
    GAIN_LOSS_AMOUNT ,  
    PROCEEDS_OF_SALE , 
    GAIN_LOSS_TYPE_CODE , 
    RETIREMENT_TYPE_CODE , 
    ITC_RECAPTURED , 
    ITC_RECAPTURE_ID , 
    REFERENCE_NUM , 
    SOLD_TO , 
    TRADE_IN_ASSET_ID , 
    STL_METHOD_CODE , 
    STL_LIFE_IN_MONTHS , 
    STL_DEPRN_AMOUNT , 
    CREATED_BY , 
    CREATION_DATE  , 
    LAST_UPDATE_LOGIN , 
    ATTRIBUTE1 , 
    ATTRIBUTE2 , 
    ATTRIBUTE3 , 
    ATTRIBUTE4 , 
    ATTRIBUTE5 , 
    ATTRIBUTE6 , 
    ATTRIBUTE7 , 
    ATTRIBUTE8 , 
    ATTRIBUTE9 , 
    ATTRIBUTE10 , 
    ATTRIBUTE11 , 
    ATTRIBUTE12 , 
    ATTRIBUTE13 , 
    ATTRIBUTE14 , 
    ATTRIBUTE15 , 
    ATTRIBUTE_CATEGORY_CODE , 
    REVAL_RESERVE_RETIRED , 
    UNREVALUED_COST_RETIRED , 
    BONUS_RESERVE_RETIRED , 
    RECOGNIZE_GAIN_LOSS , 
    REDUCTION_RATE , 
    RECAPTURE_RESERVE_FLAG , 
    LIMIT_PROCEEDS_FLAG , 
    TERMINAL_GAIN_LOSS , 
    RESERVE_RETIRED , 
    RECAPTURE_AMOUNT , 
    EOFY_RESERVE , 
    IMPAIR_RESERVE_RETIRED , 
	kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.FA_RETIREMENTS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(RETIREMENT_ID,0), 
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(RETIREMENT_ID,0) AS RETIREMENT_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.FA_RETIREMENTS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(RETIREMENT_ID,0) 
			)	
	);

commit;

-- Soft delete
update bec_ods.FA_RETIREMENTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_RETIREMENTS set IS_DELETED_FLG = 'Y'
where (RETIREMENT_ID)  in
(
select RETIREMENT_ID from bec_raw_dl_ext.FA_RETIREMENTS
where (RETIREMENT_ID,KCA_SEQ_ID)
in 
(
select RETIREMENT_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_RETIREMENTS
group by RETIREMENT_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;
 

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'fa_retirements';

commit;