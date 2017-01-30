CREATE SCHEMA IF NOT EXISTS `SAMPLE`;

USE `SAMPLE`;

-- Eye Candy - sample in creating and populating a table
CREATE TABLE IF NOT EXISTS `Person` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(40),
    `birth` DATE,
    `notes` VARCHAR(40),
    PRIMARY KEY (`id`))
ENGINE = InnoDB;

TRUNCATE TABLE `Person`;

INSERT INTO `Person` (name, birth, notes) VALUES('John Doe','1980-01-01','Nobody');
INSERT INTO `Person` (name, birth, notes) VALUES('Adam Smith','1723-06-05','Philosopher');
INSERT INTO `Person` (name, birth, notes) VALUES('Max Weber','1864-04-21','Political Economist');
INSERT INTO `Person` (name, birth, notes) VALUES('Thomas Jefferson','1801-03-04','Founding Father and President of USA');


CREATE TABLE IF NOT EXISTS `Messages` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `message` TEXT,
    PRIMARY KEY (`id`))
ENGINE = InnoDB;

TRUNCATE TABLE `Messages`;


-- H2 CREATE USER IF NOT EXISTS dbuser PASSWORD `dbuser`;

-- GRANT ALL ON Person TO dbuser;
-- GRANT ALL ON Messages TO dbuser;

CREATE USER 'dbuser'@'localhost' IDENTIFIED BY 'dbuser';
CREATE USER 'dbuser'@'%' IDENTIFIED BY 'dbuser';

GRANT CREATE, DROP, GRANT OPTION, REFERENCES, ALTER, DELETE, INDEX, INSERT, SELECT, UPDATE ON TABLE `Messages` TO 'dbuser'@'localhost';
GRANT CREATE, DROP, GRANT OPTION, REFERENCES, ALTER, DELETE, INDEX, INSERT, SELECT, UPDATE ON TABLE `Messages` TO 'dbuser'@'%';

GRANT CREATE, DROP, GRANT OPTION, REFERENCES, ALTER, DELETE, INDEX, INSERT, SELECT, UPDATE ON TABLE `Person` TO 'dbuser'@'localhost';
GRANT CREATE, DROP, GRANT OPTION, REFERENCES, ALTER, DELETE, INDEX, INSERT, SELECT, UPDATE ON TABLE `Person` TO 'dbuser'@'%';
