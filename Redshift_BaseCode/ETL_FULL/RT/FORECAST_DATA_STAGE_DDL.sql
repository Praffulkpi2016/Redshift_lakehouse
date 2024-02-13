drop table if exists bec_install.FORECAST_DATA_STAGE;
CREATE TABLE bec_install.forecast_data_stage
( 
batch_id VARCHAR(100) ENCODE lzo,
Forecast_Designator VARCHAR(20) ENCODE lzo,
Part_Number VARCHAR(100)   ENCODE lzo,
Item_Description VARCHAR(50)   ENCODE lzo,
Primary_Uom_Code VARCHAR(50)   ENCODE lzo,
Forecast_Date VARCHAR(150)   ENCODE lzo,
Current_Forecast_Quantity NUMERIC(28,10)   ENCODE az64,
Original_Forecast_Quantity NUMERIC(28,10)   ENCODE az64,
Comments VARCHAR(50)   ENCODE lzo,
Bom_Item_Type_Desc VARCHAR(20) ENCODE lzo,
Mrp_Planning_Code_Desc VARCHAR(20) ENCODE lzo,
Ato_Forecast_Control_Desc VARCHAR(50) ENCODE lzo,
Pick_Components_Flag VARCHAR(2)   ENCODE lzo,
Bucket_Type NUMERIC(15,0)   ENCODE az64,
Site_ID VARCHAR(100)   ENCODE lzo,
Organization_Id NUMERIC(15,0)   ENCODE az64
)
--+++++++++++++++++++++++++++
copy bec_install.forecast_data_stage(
batch_id
forecast_designator,
part_number,
item_description,
primary_uom_code,
forecast_date,
current_forecast_quantity,
original_forecast_quantity,
comments,
bom_item_type_desc,
mrp_planning_code_desc,
ato_forecast_control_desc,
pick_components_flag,
bucket_type,
site_id,
organization_id
) from 's3://be-edp-uat-fileupload-s3bucket/Forecast 265-.csv' 
iam_role 'arn:aws:iam::979056539714:role/be-edp-uat-RedshiftSpectrumRole' csv 
ignoreheader 1;