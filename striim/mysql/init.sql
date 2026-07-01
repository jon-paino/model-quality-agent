-- Init for the Striim CDC source (Quality Agent Layer 2 Phase 1).
-- Runs once on first container start (docker-entrypoint-initdb.d).

CREATE DATABASE IF NOT EXISTS qualitydemo;
USE qualitydemo;

-- The 6 feature columns come FIRST, in the exact CSV/DSVParser order the OPs
-- read positionally (data[0..5]). The surrogate PK is LAST so CDC data[0..5]
-- stay aligned with the inference contract:
--   data[0]=pickup_geohash, 1=hour_of_day, 2=day_of_week, 3=is_weekend,
--   4=trip_distance, 5=passenger_count, 6=trip_id.
CREATE TABLE IF NOT EXISTS trips (
    pickup_geohash  VARCHAR(12) NOT NULL,
    hour_of_day     INT         NOT NULL,
    day_of_week     INT         NOT NULL,
    is_weekend      TINYINT     NOT NULL,
    trip_distance   FLOAT       NOT NULL,
    passenger_count INT         NOT NULL,
    trip_id         BIGINT      NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (trip_id)
);

-- CDC user. mysql_native_password avoids caching_sha2 handshake issues with
-- the binlog connector shipped in Striim 5.2.0.4.
CREATE USER IF NOT EXISTS 'striim'@'%' IDENTIFIED WITH mysql_native_password BY 'striim_cdc_pw';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'striim'@'%';
FLUSH PRIVILEGES;
