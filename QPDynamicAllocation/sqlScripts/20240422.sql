ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD INDEX `origin` (`Origin` ASC) VISIBLE,
ADD INDEX `destin` (`Destin` ASC) VISIBLE,
ADD INDEX `depdate` (`DepDate` ASC) INVISIBLE,
ADD INDEX `fltnum` (`FltNum` ASC) VISIBLE;

