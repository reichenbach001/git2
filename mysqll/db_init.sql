CREATE DATABASE IF NOT EXISTS tset_db;

CREATE TABLE IF NOT EXISTS tset_db.last_check(share_id bigint PRIMARY KEY,last_update DATE DEFAULT '1980-01-01');	
