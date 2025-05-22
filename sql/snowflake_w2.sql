CREATE DATABASE TAPIR_ZOO_DB;

CREATE SCHEMA ZOO_SCHEMA;
USE SCHEMA ZOO_SCHEMA;

CREATE OR REPLACE STAGE TAPIR_ZOO_DB.ZOO_SCHEMA.STAGE_EXTERNAL
FILE_FORMAT = (TYPE = 'JSON');

LIST @TAPIR_ZOO_DB.ZOO_SCHEMA.STAGE_EXTERNAL;

CREATE OR REPLACE TABLE zoo_table (data VARIANT);

COPY INTO zoo_table
FROM @TAPIR_ZOO_DB.ZOO_SCHEMA.STAGE_EXTERNAL/data.json
FILE_FORMAT = (TYPE = 'JSON');

SELECT * FROM ZOO_TABLE;

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON';


SELECT
  $1:zooName::STRING AS zoo_name,
  $1:location::STRING AS zoo_location
FROM @TAPIR_ZOO_DB.ZOO_SCHEMA.STAGE_EXTERNAL/data.json (FILE_FORMAT => 'json_format');

SELECT
  $1:director.name::STRING AS director_name,
  $1:director.species::STRING AS director_species
FROM @TAPIR_ZOO_DB.ZOO_SCHEMA.STAGE_EXTERNAL/data.json (FILE_FORMAT => 'json_format');

CREATE OR REPLACE TABLE zoo_creatures (creatures VARIANT);

COPY INTO zoo_creatures
FROM @TAPIR_ZOO_DB.ZOO_SCHEMA.STAGE_EXTERNAL/data.json
FILE_FORMAT = (TYPE = 'JSON')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT * from zoo_creatures;

SELECT
  c.value:name::STRING AS creature_name,
  c.value:species::STRING AS creature_species
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_CREATURES,
LATERAL FLATTEN(input => creatures) c;

SELECT
  c.value:name::STRING AS creature_name,
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_CREATURES,
LATERAL FLATTEN(input => creatures) c
WHERE c.value:originPlanet::STRING = 'Xylar';

CREATE OR REPLACE TABLE zoo_habitats (habitats VARIANT);

COPY INTO zoo_habitats
FROM @TAPIR_ZOO_DB.ZOO_SCHEMA.STAGE_EXTERNAL/data.json
FILE_FORMAT = (TYPE = 'JSON')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT * FROM ZOO_HABITATS;

SELECT
  h.value:name::STRING AS habitat_name,
  h.value:environmentType::STRING AS habitat_env_type
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_HABITATS,
LATERAL FLATTEN(input => habitats) h
WHERE h.value:sizeSqMeters::INT > 2000;

SELECT
  c.value:name::STRING AS creature_name,
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_CREATURES,
LATERAL FLATTEN(input => creatures) c
WHERE ARRAY_CONTAINS('Camouflage'::VARIANT, c.value:specialAbilities);

SELECT
  c.value:name::STRING AS creature_name,
  c.value:healthStatus.status::STRING AS creature_status
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_CREATURES,
LATERAL FLATTEN(input => creatures) c
WHERE c.value:healthStatus.status::STRING != 'Excellent';

SELECT 
 s.value:name::STRING AS staff_name,
 s.value:role::STRING AS staff_role
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_TABLE,
LATERAL FLATTEN(input => data, path => 'staff') s
WHERE ARRAY_CONTAINS('H001'::VARIANT, s.value:assignedHabitatIds);

SELECT
 h.value:id::STRING AS habitat_id,
 COUNT(*) AS creature_count
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_TABLE,
LATERAL FLATTEN(input => data:habitats) h,
LATERAL FLATTEN(input => data:creatures) c,
WHERE c.value:habitatId::STRING = h.value:id::STRING
GROUP BY h.value:id::STRING;

SELECT
 DISTINCT f.value::STRING AS habitat_feature
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_TABLE,
LATERAL FLATTEN(input => data:habitats) h,
LATERAL FLATTEN(input => h.value:features) f;

SELECT
 e.value:eventId::STRING AS event_id,
 e.value:type::STRING AS event_type,
 e.value:scheduledTime::TIMESTAMP AS event_time
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_TABLE,
LATERAL FLATTEN(input => data:upcomingEvents) e;

SELECT
  c.value:name::STRING AS creature_name,
  h.value:environmentType::STRING AS creature_env
FROM TAPIR_ZOO_DB.ZOO_SCHEMA.ZOO_TABLE,
LATERAL FLATTEN(input => data:creatures) c,
LATERAL FLATTEN(input => data:habitats) h
WHERE c.value:habitatId::STRING = h.value:id::STRING;