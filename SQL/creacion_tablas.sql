DROP DATABASE IF EXISTS desafiorkd;

CREATE DATABASE desafiorkd WITH
    OWNER = postgres
	ENCODING = 'UTF-8'
	CONNECTION LIMIT = -1;

DROP TABLE IF EXISTS show;
DROP TABLE IF EXISTS catalogo;

CREATE TABLE show (
	show_id VARCHAR(10) NOT NULL,
	title VARCHAR(180) NOT NULL,
    duration VARCHAR(20),
    description VARCHAR(350),
    catalog_name VARCHAR(20) NOT NULL,
    PRIMARY KEY (show_id, catalog_name)
);

CREATE TABLE team (
	show_id VARCHAR(10) NOT NULL,
	catalog_name VARCHAR(20) NOT NULL,
	ccast VARCHAR(950),
	director VARCHAR(250),
	PRIMARY KEY (show_id, catalog_name),
	CONSTRAINT fk_show FOREIGN KEY (show_id, catalog_name) REFERENCES show (show_id, catalog_name)
);

CREATE TABLE year (
	show_id VARCHAR(10) NOT NULL,
	catalog_name VARCHAR(20) NOT NULL,
	date_added DATE,
	release_year INT,
	PRIMARY KEY (show_id, catalog_name),
	CONSTRAINT fk_show FOREIGN KEY (show_id, catalog_name) REFERENCES show (show_id, catalog_name)
);

CREATE TABLE category (
	show_id VARCHAR(10) NOT NULL,
	catalog_name VARCHAR(20) NOT NULL,
	listed_in VARCHAR(150),
	rating VARCHAR(10),
	type VARCHAR(20),
	country VARCHAR(150),
	PRIMARY KEY (show_id, catalog_name),
	CONSTRAINT fk_show FOREIGN KEY (show_id, catalog_name) REFERENCES show (show_id, catalog_name)
);
