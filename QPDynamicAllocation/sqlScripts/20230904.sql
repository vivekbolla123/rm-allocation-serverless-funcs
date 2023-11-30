CREATE TABLE `config_code_version` (
  `code_version` VARCHAR(15) NOT NULL,
  `isActive` BIT(1) NULL,
  PRIMARY KEY (`code_version`));

INSERT INTO `config_code_version` (`code_version`, `isActive`) VALUES ('3.3.1', b'1');

CREATE TABLE `config_column_names` (
  `tableName` text,
  `columns` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `config_column_names` VALUES ('Curves','NDO,CurveID,LF,B2C_Avg_Fare,B2B_Avg_Fare'),('Fares','SECTOR,Org,Dst,RBD,TOTAL'),('market_list','Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor'),('market_list_adhoc','Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor');
