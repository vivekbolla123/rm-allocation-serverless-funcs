UPDATE `QP_DW_RMALLOC`.`config_column_names`
SET
    `columns` = 'Sector1,Flight1,Sector2,Flight2,Perstart,PerEnd,DOW,Outbound_stop,Outbound_duration,carrierExclusionB2B,carrierExclusionB2C,Price_Strategy,DiscountFlag,Discount_Value,fareAnchor,Offset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,Currency,B2BBackstop,B2CBackstop,B2BFactor,analystName'
WHERE (
        `tableName` = 'market_list_connections'
    );

ALTER TABLE `QP_DW_RMALLOC`.`market_list_connections`
ADD COLUMN `carrierExclusionB2C` VARCHAR(45) NULL AFTER `DiscountFlag`,
ADD COLUMN `carrierExclusionB2B` VARCHAR(45) NULL AFTER `carrierExclusionB2C`;