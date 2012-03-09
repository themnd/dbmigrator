-- Schema version table
-- let me see this second comment

CREATE TABLE `schemaversion` (
  `id` INT  NOT NULL AUTO_INCREMENT,
  `major` INT  NOT NULL,
  `minor` INT  NOT NULL,
  `date` DATETIME  NOT NULL,
  `script` VARCHAR(255),
  `description` VARCHAR(255),
  `md5` VARCHAR(255),
  PRIMARY KEY (`id`)
)
ENGINE = InnoDB;

