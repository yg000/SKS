
drop table dwb.wb_keyword;
drop table dwb.wb_relation_product_keyword;
drop table dwb.wb_relation_project_keyword;
drop table dwb.wb_relation_person_keyword;

drop table dwd.wd_project_person_keyword_split_nsfc_translated_en;
drop table dwd.wd_project_person_keyword_split_nsfc_translated;
drop table dwd.wd_product_keyword_split_nsfc_translated_en;
drop table dwd.wd_product_keyword_split_nsfc_translated;


create table if not exists dwb.wb_keyword(
  `keyword_id` string,
  `zh_keyword` string,
  `en_keyword` string,
  `source` string
) stored as orc;


create table if not exists dwb.wb_relation_product_keyword(
  `product_id` string,
  `keyword_id` string
) stored as orc;

create table if not exists dwb.wb_relation_project_keyword(
  `project_id` string,
  `keyword_id` string
) stored as orc;
create table if not exists dwb.wb_relation_person_keyword(
  `person_id` string,
  `keyword_id` string
) stored as orc;

alter  table nsfc.o_product_keywords_nsfc_split rename to nsfc.o_product_keyword_nsfc_split;

ALTER TABLE dwd.wd_project_person_keywords_split_nsfc CHANGE zh_keywords  zh_keyword STRING;
ALTER TABLE dwd.wd_project_person_keywords_split_nsfc CHANGE en_keywords  en_keyword STRING;
ALTER TABLE dwd.wd_product_keyword_split_nsfc_translated_en CHANGE zh_keywords  zh_keyword STRING;
ALTER TABLE dwd.wd_product_keyword_split_nsfc_translated_en CHANGE en_keywords  en_keyword STRING;
create table if not exists dwd.wd_project_person_keyword_split_nsfc_translated_en(
  `id` string,
  `type` string,
  `code` string,
  `zh_keyword` string,
  `en_keyword` string,
  `stanard_or_not` string,
  `source` string
) stored as orc;

create table if not exists dwd.wd_product_keyword_split_nsfc_translated_en(
  `product_id` string,
  `zh_keyword` string,
  `en_keyword` string,
  `stanard_or_not` string,
  `source` string
) stored as orc;

create table dwd.wd_project_person_keyword_split_nsfc_translated like dwd.wd_project_person_keyword_split_nsfc_translated_en;
create table dwd.wd_product_keyword_split_nsfc_translated like dwd.wd_product_keyword_split_nsfc_translated_en;



