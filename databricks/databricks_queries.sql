-- creating warehouse, database and schema 
-- the tables were created automotacially with the upload of the .parquet files 
create warehouse weather_wh;
create database weather_dwh;
create schema weather_dwh.gold_layer;
GRANT USAGE ON DATABASE weather_dwh TO ROLE accountadmin;
GRANT USAGE ON SCHEMA weather_dwh.gold_layer TO ROLE accountadmin;




-- adding region column 
ALTER TABLE WEATHER_DWH.GOLD_LAYER.WEATHER_DATA
ADD COLUMN REGION STRING;

-- adding sub_region column 
alter table weather_dwh.gold_layer.weather_data
add column sub_region string;

-- updating weather data, substituting the lat/lot with the correct regions and sub regions 
UPDATE WEATHER_DWH.GOLD_LAYER.WEATHER_DATA
SET REGION = CASE
  WHEN ROUND(LATITUDE,4)=35.3251 AND ROUND(LONGITUDE,4)=25.2046 THEN 'Crete'
  WHEN ROUND(LATITUDE,4)=36.4499 AND ROUND(LONGITUDE,4)=28.1984 THEN 'Aegean'
  WHEN ROUND(LATITUDE,4)=37.9965 AND ROUND(LONGITUDE,4)=23.7097 THEN 'Attica'
  WHEN ROUND(LATITUDE,4)=38.2777 AND ROUND(LONGITUDE,4)=21.7703 THEN 'Peloponnese, Western Greece and the Ionian'
  WHEN ROUND(LATITUDE,4)=39.6134 AND ROUND(LONGITUDE,4)=22.3440 THEN 'Thessaly and Central Greece'
  WHEN ROUND(LATITUDE,4)=39.6837 AND ROUND(LONGITUDE,4)=20.8750 THEN 'Epirus and Western Macedonia'
  WHEN ROUND(LATITUDE,4)=40.6678 AND ROUND(LONGITUDE,4)=22.9462 THEN 'Macedonia and Thrace'
  ELSE 'Unknown'
END;

UPDATE WEATHER_DWH.GOLD_LAYER.WEATHER_DATA
SET SUB_REGION = CASE
  WHEN REGION = 'Attica' THEN 'Decentralized Administration of Attica'
  WHEN REGION = 'Crete' THEN 'Decentralized Administration of Crete'
  WHEN REGION = 'Epirus and Western Macedonia' THEN 'Decentralized Administration of Epirus and Western Macedonia'
  WHEN REGION = 'Macedonia and Thrace' THEN 'Decentralized Administration of Macedonia and Thrace'
  WHEN REGION = 'Peloponnese, Western Greece and the Ionian' THEN 'Decentralized Administration of Peloponnese, Western Greece and the Ionian'
  WHEN REGION = 'Aegean' THEN 'Decentralized Administration of the Aegean'
  WHEN REGION = 'Thessaly and Central Greece' THEN 'Decentralized Administration of Thessaly and Central Greece'
  ELSE NULL
END;





select * from weather_dwh.gold_layer.mobility_data