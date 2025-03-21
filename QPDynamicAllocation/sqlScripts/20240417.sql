CREATE TABLE `QP_DW_RMALLOC`.`config_clusters` (
  `cluster` INT NOT NULL,
  `startTime` TIME NULL,
  `endTime` TIME NULL,
  PRIMARY KEY (`cluster`));

INSERT INTO `QP_DW_RMALLOC`.`config_clusters` (`cluster`, `startTime`, `endTime`) VALUES ('0', '2:30', '7:30');
INSERT INTO `QP_DW_RMALLOC`.`config_clusters` (`cluster`, `startTime`, `endTime`) VALUES ('1', '7:30', '11:00');
INSERT INTO `QP_DW_RMALLOC`.`config_clusters` (`cluster`, `startTime`, `endTime`) VALUES ('2', '11:00', '14:30');
INSERT INTO `QP_DW_RMALLOC`.`config_clusters` (`cluster`, `startTime`, `endTime`) VALUES ('3', '14:30', '18:00');
INSERT INTO `QP_DW_RMALLOC`.`config_clusters` (`cluster`, `startTime`, `endTime`) VALUES ('4', '18:00', '21:00');
INSERT INTO `QP_DW_RMALLOC`.`config_clusters` (`cluster`, `startTime`, `endTime`) VALUES ('5', '21:00', '2:30');
