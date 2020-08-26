


create table if not exists dwb.wb_keywords(
  `keyword_id` string,
  `zh_keyword` string,
  `en_keyword` string,
  `source` string)
) stored as orc;


create table if not exists dwb.wb_relation_product_keywords(
  `product_id` string,
  `keyword_id` string)
) stored as orc;

create table if not exists dwb.wb_relation_project_keywords(
  `project_id` string,
  `keyword_id` string)
) stored as orc;
create table if not exists dwb.wb_relation_person_keywords(
  `person_id` string,
  `keyword_id` string)
) stored as orc;

create table if not exists dwd.wd_project_person_keywords_split_nsfc_translated_en(
  `id` string,
  `type` string,
  `code` string,
  `zh_keywords` string,
  `en_keywords` string,
  `stanard_or_not` string,
  `source` string)
) stored as orc;

create table if not exists dwd.wd_product_keywords_split_nsfc_translated_en(
  `product_id` string,
  `zh_keywords` string,
  `en_keywords` string,
  `stanard_or_not` string,
  `source` string)
) stored as orc;

create table dwd.wd_project_person_keywords_split_nsfc_translated like dwd.wd_project_person_keywords_split_nsfc_translated_en;
create table dwd.wd_product_keywords_split_nsfc_translated like dwd.wd_product_keywords_split_nsfc_translated_en;



