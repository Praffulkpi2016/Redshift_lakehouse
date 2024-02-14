DROP table IF EXISTS bec_install.FORECAST_DATA_STAGE;
CREATE TABLE bec_install.forecast_data_stage
( 
batch_id string ,
Forecast_Designator string ,
Part_Number string   ,
Item_Description string   ,
Primary_Uom_Code string   ,
Forecast_Date string   ,
Current_Forecast_Quantity NUMERIC(28,10)   ,
Original_Forecast_Quantity NUMERIC(28,10)   ,
Comments string   ,
Bom_Item_Type_Desc string ,
Mrp_Planning_Code_Desc string ,
Ato_Forecast_Control_Desc string ,
Pick_Components_Flag string   ,
Bucket_Type NUMERIC(15,0)   ,
Site_ID string   ,
Organization_Id NUMERIC(15,0)   
)
--+++++++++++++++++++++++++++
copy bec_install.forecast_data_stage(
batch_id,
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