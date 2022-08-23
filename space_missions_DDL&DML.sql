-- create database "space_missions_OLTP" with owner postgres;
-- comment on database "space_missions_OLTP" is 'OLTP database for space mission analysis';
-- create schema sp_missions;
-- alter schema sp_missions owner to postgres;

-- Select schema 'sp_missions':
set search_path to sp_missions;

-- DDL of newly created table from .csv dataset:
create table space_missions
(
	mission_id integer not null constraint space_missions_pk primary key,
	company varchar(100),
	launch_location varchar(255),
	launch_datetime varchar(255),
	rocket_payload varchar(255),
	rocket_status varchar(20),
	cost text,
	status varchar(20)
);

-- Rename table from space_missions to mission:
alter table space_missions rename to mission;

-- Update column launch_location to contain only main location (country, ocean, sea) of mission:
update mission set launch_location = TRIM(SPLIT_PART(launch_location, ',', -1));

-- Rename column launch_location to mission_location:
alter table mission rename column launch_location to mission_location;

-- Translate launch_datetime column mission_date:
-- launch_datetime = (Fri Aug 07, 2020 05:12 UTC) and text type
-- mission_date = (2020-08-07) and date type:
update mission set launch_datetime =  REPLACE (launch_datetime, ',', '');
update mission set launch_datetime = split_part(launch_datetime, ' ', 3)
                                         ||' '||split_part(launch_datetime, ' ', 2)
                                         ||' '||split_part(launch_datetime, ' ', 4);
alter table mission add column mission_date date;
update mission set mission_date = TO_DATE(launch_datetime, 'DD Mon YYYY');
alter table mission drop column launch_datetime;

-- Column rocket_payload containt data of format ROCKET | PAYLOAD OBJECT:
-- We are interested in rockets only so we update column to contain only required data:
alter table mission rename column rocket_payload to rocket;
update mission set rocket = TRIM(split_part(rocket, '|', 1));

-- Our table contains rocket_status column with 2 distinct values of StatusRetired and StatusActive
-- We first replace these values to retired and active respectively
-- We then translate that data into boolean column is_rocket_active
-- Finally we drop rocket_status column that was replaced by is_rocket_active column:
update mission
set rocket_status = case when rocket_status = 'StatusRetired' then 'retired'
                         when rocket_status = 'StatusActive' then 'active' end;
alter table mission add column is_rocket_active boolean;
update mission set is_rocket_active = case when rocket_status = 'retired' then false when rocket_status = 'active' then true end;
alter table mission drop column rocket_status;

-- There some cost values with commas.
-- In additon it is unreal to make calculations on numbers in text format.
-- We cannon cast such records into numeric format so we have to eliminate commas in cost column:
update mission set cost = replace(cost, ',', '');
-- We then create new column for mission cost and populate it with cost values in numeric format:
alter table mission add column mission_cost numeric;
update mission set mission_cost = cast(cost as numeric);
-- Finally we drop cost column as it was replaced with mission_cost column:
alter table mission drop column cost;
-- Rename column mission_cost to cost:
alter table mission rename column mission_cost to cost;

-- Lets check again column for null values:
select count(*) from mission where mission_id is null; -> 0
select count(*) from mission where company is null; -> 0
select count(*) from mission where mission_location is null; -> 0
select count(*) from mission where rocket is null; -> 0
select count(*) from mission where status is null; -> 0
select count(*) from mission where mission_date is null; -> 0
select count(*) from mission where is_rocket_active is null; -> 0
select count(*) from mission where cost is null; -> 3360

-- Check company names for wrong values:
select distinct company from mission;

-- We have one company with strange value: ("Arm??e de l'Air")
-- After googling we got "French Air and Space Force" corresponding to this value.
-- We replace this value with english translation to correct errors in encoding:
update mission
set company = 'French Air and Space Force'
where mission_id in (select mission_id from mission where company = 'Arm??e de l''Air');

-- Check mission location for wrong values:
select distinct mission_location from mission;

-- Check rocket names for wrong values:
select distinct rocket from mission;

-- Check cost for wrong values:
select * from mission where cost is not null
order by cost desc;

-- backup table:
-- auto-generated definition
create table public.mission
(
    mission_id       integer not null constraint space_missions_pk primary key,
    company          varchar(100),
    mission_location varchar(255),
    rocket           varchar(255),
    status           varchar(20),
    mission_date     date,
    is_rocket_active boolean,
    cost             numeric
);

alter table public.mission owner to postgres;

insert into public.mission select * from mission order by mission_id;

-- DATA MODEL:

-- FUNCTION THAT CREATES DIMENSION TABLE FOR SPECIFIED COLUMN AND MODIFIES FACTS TABLE BY CREATING FK REFERENCE
-- INPUT:
-- ptable -> MAIN TABLE NAME
-- pcolumn -> COLUMN THAT WE WANT TO MOVE INTO SEPARATE TABLE
-- pnewtablename -> NAME OF NEW TABLE WE WANT TO CREATE

-- OUTPUT:
-- RETURNS SUCCESS MESSAGE OR ERROR MESSAGE.

CREATE OR REPLACE FUNCTION generate_table_for_column(IN ptable VARCHAR, IN pcolumn VARCHAR, IN pnewtablename VARCHAR)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
    DECLARE
        col_type text;
        new_table_pk_name text;
    begin
        ptable := lower(ptable);
        pcolumn := lower(pcolumn);
        pnewtablename := lower(pnewtablename);

        -- FIND DATA TYPE OF TARGET COLUMN:
        EXECUTE format('SELECT pg_typeof(%I) FROM %I LIMIT 1', pcolumn, ptable) INTO col_type;
        -- CREATE COLUMN NAME FOR PK OF NEW TABLE:
        SELECT pnewtablename || '_id' INTO new_table_pk_name;
        -- CREATE NEW TABLE FOR TARGET COLUMN WITH PK AND COLUMN FOR VALUES:
        EXECUTE format('CREATE TABLE %I( %I INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, %I %s NOT NULL, CREATED_AT DATE NOT NULL DEFAULT CURRENT_DATE)', pnewtablename, new_table_pk_name, pcolumn, col_type);
        -- FILL NEW TABLE WITH DATA FROM SOURCE TABLE:
        EXECUTE format('INSERT INTO %I (%I) SELECT DISTINCT %I FROM %I', pnewtablename, pcolumn, pcolumn, ptable);
        -- ADD COLUMN FOR FOREIGN KEY RELATIONSHIP TO REFERENCE ID OF VALUE IN NEWLY CREATED TABLE:
        EXECUTE format('ALTER TABLE %I ADD COLUMN %I INT', ptable, new_table_pk_name);
        -- FILL FK OF SOURCE TABLE COLUMN WITH PK VALUES FROM DIMENSION TABLE:
        EXECUTE 'UPDATE '||quote_ident(ptable)||' t1 SET '||quote_ident(new_table_pk_name)||' = t2.'||quote_ident(new_table_pk_name)||' FROM '||quote_ident(pnewtablename)||' t2 WHERE t1.'||quote_ident(pcolumn)||' = t2.'||quote_ident(pcolumn);
        -- ADD FOREIGN KEY CONSTRAINT:
        EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %s FOREIGN KEY (%I) REFERENCES %I(%I) ON UPDATE CASCADE ON DELETE RESTRICT', ptable, ptable||'_'||new_table_pk_name||'_fk', new_table_pk_name, pnewtablename, new_table_pk_name);
        -- CREATE INDEX ON NEW FK COLUMN:
        EXECUTE format('CREATE INDEX %s ON %I(%I)', ptable||'_'||new_table_pk_name||'_fk_index', ptable, new_table_pk_name);
        -- DROP COLUMN FROM SOURCE TABLE THAT CONTAINED VALUES THAT WERE REPLACED BY ID'S OF THAT VALUES TAKEN FROM NEW TABLE:
        EXECUTE format('ALTER TABLE %I DROP COLUMN %I', ptable, pcolumn);
        -- RETURN MESSAGE:
        RETURN 'TABLE '|| pnewtablename || ' WAS CREATED !';
        -- IF ERROR RETURN ERROR MESSAGE:
        EXCEPTION WHEN OTHERS THEN RETURN SQLERRM;
    end;
$$;

-- CALL FUNCTION:
SELECT generate_table_for_column('mission', 'company', 'company');
SELECT generate_table_for_column('mission', 'mission_location', 'mission_location');
SELECT generate_table_for_column('mission', 'rocket', 'rocket');

alter table rocket add column is_rocket_active boolean;
update rocket
set is_rocket_active = mission.is_rocket_active
from mission
where rocket.rocket_id = mission.rocket_id;
alter table mission drop column is_rocket_active;

