ALTER TABLE dm.dm_es_organization CHANGE org_id id String



org_id string,
org_code string,
org_name string,
en_name string,
alias string,
registration_date string,
org_type string,
nature string,
isLegalPersonInstitution string,
legal_person string,
contact_psn_code string,
belongtocode string,
address string,
country string,
province string,
city string,
district string

CREATE TABLE `o_tmp_organization_name`(
  `org_name` string,
     source                       string
  ) stored as orc;



select count(*),count(distinct org_name),count(distinct org_id) from dwb.wb_organization_manual_nsfc;

select count(*) from dwd.wd_organization_nsfc
dm_neo4j_person
create table if not exists dm.dm_neo4j_organization(
   id                       string,
   org_name                     string,
   en_name                      string,
   alias                        string,
   org_type                     string,
   country                      string,
   province                     string,
   city                         string
) stored as orc;
create table if not exists dwd.wd_organization_sts(
   org_id                       string,
   social_credit_code           string,
   org_name                     string,
   en_name                      string,
   alias                        string,
   registration_date            string,
   org_type                     string,
   nature                       string,
   isLegalPersonInstitution     string,
   legal_person                 string,
   belongtocode                 string,
   address                      string,
   country                      string,
   province                     string,
   city                         string,
   district                     string,
   zipCode                      string,
   source                       string
) stored as orc;

create table if not exists dwb.wb_organization(
   org_id                       string,
   social_credit_code           string,
   org_name                     string,
   en_name                      string,
   alias                        string,
   registration_date            string,
   org_type                     string,
   nature                       string,
   isLegalPersonInstitution     string,
   legal_person                 string,
   belongtocode                 string,
   address                      string,
   country                      string,
   province                     string,
   city                         string,
   district                     string,
   zipCode                      string,
   source                       string,
   flag    string
) stored as orc;

create table if not exists dwb.wb_organization_comparison_table(
   non_standard_name                       string,
   standard_name           string
) stored as orc;

create table if not exists dwb.wb_organization_person(
   person_id                       string,
   organization_id           string
) stored as orc;

insert into dwd.wd_organization_nsfc
select
    org_id
   ,null as social_credit_code
   ,name as org_name
   ,en_name
   ,null as alias
   ,concat(regyear,'-01-01') as registration_date
   ,c.zh_cn_caption as org_type
   ,d.zh_cn_caption as nature
   ,null as isLegalPersonInstitution
   ,faren as legal_person
   ,b.zh_cn_caption   as   belongtocode
   ,null as address
   ,null as country
   ,province
   ,city
   ,null as district
   ,zipCode
   ,source
from ods.o_organization_nsfc a
left join ods.o_const_dictionary_nsfc b
on b.category ='unit_relation' and  a.belongtocode=b.code
left join ods.o_const_dictionary_nsfc c
on c.category ='org_type' and  a.orgtype=c.code
left join ods.o_const_dictionary_nsfc d
on d.category ='91' and  a.nature=d.code ;


create table if not exists dwd.wd_organization_manual like dwd.wd_organization_nsfc;
create table if not exists dwd.wd_organization_nsfc like ;




DROP TABLE dwd.wd_organization_nsfc;
DROP TABLE dwb.wb_organization_manual_nsfc;

ALTER TABLE dwb.wb_organization_manual_nsfc RENAME TO dwb.wb_organization_manual_sks;
ALTER TABLE dwb.wb_organization_manual_nsfc_rel RENAME TO dwb.wb_organization_manual_sks_rel;

ALTER TABLE ods.o_manual_organization_sks RENAME TO ods.o_manual_organization_sts;
ALTER TABLE dwb.wb_organization_manual_sks_rel RENAME TO dwb.wb_organization_manual_sts_rel;

create table  dwb.wb_organization_manual_sts_nsfc like dwb.wb_organization_manual_sts;
create table  dwb.wb_organization_manual_sts_nsfc_rel like dwb.wb_organization_manual_sts_rel;


DROP TABLE dwb.wb_organization_manual_sts_rel;
truncate table ods.o_manual_organization_sks;
create table if not exists ods.o_manual_organization_sks_tmp
(
org_name string,
registration_date string,
social_credit_code string,
registered_capital string,
address string,
longitude string,
latitude string,
registration_management_department_name string,
business_time_from string,
business_time_to string,
org_type string,
economic_industry_code string,
economic_industry_name string,
business_scope string,
business_status string,
legal_person string,
name_used_before string)
row format delimited
fields terminated by '\t';
DROP TABLE ods.o_manual_organization_sks_tmp;

load data local inpath '/root/aa.txt' into table ods.o_manual_organization_sks_tmp;

insert overwrite table o_manual_organization_sks select * from ods.o_manual_organization_sks_tmp;

create table if not exists ods.o_manual_organization_grid
(
org_id string,
org_name string,
city string,
state string,
country string,
alias string,
wikipedia_url string,
email_address string,
established string,
`org_type` string,
link string
) stored as orc;

drop table o_manual_organization_grid;







create table if not exists dm.dm_es_organization(
   id                       string,
   social_credit_code           string,
   org_name                     string,
   en_name                      string,
   alias                        string,
   registration_date            string,
   org_type                     string,
   nature                       string,
   isLegalPersonInstitution     string,
   legal_person                 string,
   belongtocode                 string,
   address                      string,
   country                      string,
   province                     string,
   city                         string,
   district                     string,
   zipCode                      string,
   source                       string
) stored as orc;


CREATE TABLE `dwb.wb_organization_add`(
  `org_id` string,
  `social_credit_code` string,
  `org_name` string,
  `en_name` string,
  `alias` string,
  `registration_date` string,
  `org_type` string,
  `nature` string,
  `islegalpersoninstitution` string,
  `legal_person` string,
  `belongtocode` string,
  `address` string,
  `country` string,
  `province` string,
  `city` string,
  `district` string,
  `zipcode` string,
  `source` string) PARTITIONED BY  (flag string) stored as orc;
