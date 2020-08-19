create table if not exists ods.o_nsfc_experience_study(
psn_code string,
start_date string,
end_date string,
org_name string,
profession string,
degree string,
country string,
award_year string,
advisor string
) stored as orc;

create table if not exists ods.o_nsfc_experience_work(
psn_code string,
start_date string,
end_date string,
org_name string,
deparment string,
prof_title string
) stored as orc;

create table if not exists ods.o_nsfc_experience_postdoctor(
psn_code string,
start_date string,
end_date string,
org_id string,
org_name string,
advisor string,
onjob string
) stored as orc;


create table if not exists dwd.wd_nsfc_experience_study(
person_name string,
person_id string,
start_date string,
end_date string,
org_name string,
org_id string,
profession string,
degree string,
country string,
award_year string,
advisor string,
source string
) stored as orc;

create table if not exists dwd.wd_nsfc_experience_work(
person_name string,
person_id string,
start_date string,
end_date string,
org_name string,
org_id string,
deparment string,
prof_title string,
source string
) stored as orc;

create table if not exists dwd.wd_nsfc_experience_postdoctor(
person_name string,
person_id string,
start_date string,
end_date string,
org_name string,
org_id string,
advisor string,
onjob string,
source string
) stored as orc;


create table if not exists dwd.wd_nsfc_experience_study like ods.o_nsfc_experience_study;
create table if not exists dwd.wd_nsfc_experience_work like ods.o_nsfc_experience_work;
create table if not exists dwd.wd_nsfc_experience_postdoctor like ods.o_nsfc_experience_postdoctor;
drop  table  ods.o_nsfc_experience_study;
drop  table  ods.o_nsfc_experience_work;
drop  table  ods.o_nsfc_experience_postdoctor;

drop  table  dwd.wd_nsfc_experience_study;
drop  table  dwd.wd_nsfc_experience_work;
drop  table  dwd.wd_nsfc_experience_postdoctor;




create table if not exists dm.dm_neo4j_experience_study(
person_id string,
org_id string,
start_date string,
end_date string,
profession string,
degree string,
country string,
award_year string,
advisor string,
source string
) stored as orc;

create table if not exists dm.dm_neo4j_experience_work(
person_id string,
org_id string,
start_date string,
end_date string,
deparment string,
prof_title string,
source string
) stored as orc;

create table if not exists dm.dm_neo4j_experience_postdoctor(
person_id string,
org_id string,
start_date string,
end_date string,
advisor string,
onjob string,
source string
) stored as orc;
