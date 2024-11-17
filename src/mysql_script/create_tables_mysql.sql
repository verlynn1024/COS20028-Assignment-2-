-- Drop and create database

DROP DATABASE IF EXISTS indigenous;

CREATE DATABASE indigenous;

USE indigenous;



-- Create entity tables (solid boundary)

CREATE TABLE lng_id (

    id INT AUTO_INCREMENT,

    PRIMARY KEY (id)

);



CREATE TABLE lng_code (

    code VARCHAR(60) NOT NULL,

    PRIMARY KEY (code)

);



CREATE TABLE lng_name (

    name VARCHAR(255) NOT NULL,

    name_full TEXT,

    PRIMARY KEY (name)

);



CREATE TABLE lng_synonym (

    synonym VARCHAR(255) NOT NULL,

    synonym_full TEXT,

    PRIMARY KEY (synonym)

);



CREATE TABLE lng_thl (

    thl VARCHAR(255) NOT NULL,

    thl_full TEXT,

    PRIMARY KEY (thl)

);



CREATE TABLE lng_thp (

    thp VARCHAR(255) NOT NULL,

    thp_full TEXT,

    PRIMARY KEY (thp)

);



CREATE TABLE lng_st (

    st VARCHAR(20) NOT NULL,

    PRIMARY KEY (st)

);



CREATE TABLE lng_uri (

    uri VARCHAR(255) NOT NULL,

    uri_full TEXT,

    PRIMARY KEY (uri)

);



-- Create weak entity tables (dashed boundary)

CREATE TABLE rel_code_name (

    lng_code VARCHAR(60),

    lng_name VARCHAR(255),

    PRIMARY KEY (lng_code, lng_name),

    FOREIGN KEY (lng_code) REFERENCES lng_code(code),

    FOREIGN KEY (lng_name) REFERENCES lng_name(name)

);



CREATE TABLE rel_code_synonym (

    lng_code VARCHAR(60),

    lng_synonym VARCHAR(255),

    PRIMARY KEY (lng_code, lng_synonym),

    FOREIGN KEY (lng_code) REFERENCES lng_code(code),

    FOREIGN KEY (lng_synonym) REFERENCES lng_synonym(synonym)

);



CREATE TABLE rel_code_thl (

    lng_code VARCHAR(60),

    lng_thl VARCHAR(255),

    PRIMARY KEY (lng_code, lng_thl),

    FOREIGN KEY (lng_code) REFERENCES lng_code(code),

    FOREIGN KEY (lng_thl) REFERENCES lng_thl(thl)

);



CREATE TABLE rel_code_thp (

    lng_code VARCHAR(60),

    lng_thp VARCHAR(255),

    PRIMARY KEY (lng_code, lng_thp),

    FOREIGN KEY (lng_code) REFERENCES lng_code(code),

    FOREIGN KEY (lng_thp) REFERENCES lng_thp(thp)

);



CREATE TABLE rel_code_st (

    lng_code VARCHAR(60),

    lng_st VARCHAR(20),

    PRIMARY KEY (lng_code, lng_st),

    FOREIGN KEY (lng_code) REFERENCES lng_code(code),

    FOREIGN KEY (lng_st) REFERENCES lng_st(st)

);



CREATE TABLE rel_code_uri (

    lng_code VARCHAR(60),

    lng_uri VARCHAR(255),

    PRIMARY KEY (lng_code, lng_uri),

    FOREIGN KEY (lng_code) REFERENCES lng_code(code),

    FOREIGN KEY (lng_uri) REFERENCES lng_uri(uri)

);



CREATE TABLE rel_code_latlong (

    lng_code VARCHAR(60),

    a_lng_lat DECIMAL(10,6),

    a_lng_lng DECIMAL(10,6),

    PRIMARY KEY (lng_code),

    FOREIGN KEY (lng_code) REFERENCES lng_code(code)

);
