ALTER TABLE QP_DW_RMALLOC.market_list_international ADD COLUMN obSeats VARCHAR(50) AFTER B2BFactor;
ALTER TABLE QP_DW_RMALLOC.market_list_international ADD COLUMN obFare VARCHAR(50) AFTER obSeats;