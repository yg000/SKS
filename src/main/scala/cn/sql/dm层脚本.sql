CREATE TABLE dm.`dm_neo4j_product_journal`(
  `id` string,
  `paper_type` string,
  `chinese_name` string, 
  `english_name` string,
  `authors` string,
  `publish_date` string,  
  `language` string, 
  `includes` string, 
  `journal` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_person_journal`(
  `person_id` string,
  `achievement_id` string)
row format delimited fields terminated by '☔' stored as orc;	

CREATE TABLE dm.`dm_neo4j_product_conference`(
  `id` string,
  `paper_type` string,
  `chinese_name` string, 
  `english_name` string,
  `authors` string,
  `publish_date` string,  
  `language` string, 
  `includes` string, 
  `conference` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_person_conference`(
  `person_id` string,
  `achievement_id` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_patent`(
  `id` string,
  `chinese_name` string, 
  `english_name` string,
  `authors` string,  
  `language` string, 
  `applicant` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_person_patent`(
  `person_id` string,
  `achievement_id` string)
row format delimited fields terminated by '☔' stored as orc;	




CREATE TABLE dm.`dm_neo4j_product_criterion`(
  `id` string,
  `chinese_name` string, 
  `english_name` string,
  `authors` string,  
  `language` string, 
  `applicant` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_person_criterion`(
  `person_id` string,
  `achievement_id` string)
row format delimited fields terminated by '☔' stored as orc;	


CREATE TABLE dm.`dm_neo4j_product_monograph`(
  `id` string,
  `chinese_name` string, 
  `english_name` string,
  `authors` string,  
  `language` string, 
  `book_name` string,
  `editor` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_person_monograph`(
  `person_id` string,
  `achievement_id` string)
row format delimited fields terminated by '☔' stored as orc;	



CREATE TABLE dm.`dm_neo4j_product_subject_journal`(
  `id` string,
  `one_rank_id` string,
  `one_rank_no` string, 
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,  
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_product_subject_conference`(
  `id` string,
  `one_rank_id` string,
  `one_rank_no` string, 
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,  
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_product_subject_patent`(
  `id` string,
  `one_rank_id` string,
  `one_rank_no` string, 
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,  
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_subject_criterion`(
  `id` string,
  `one_rank_id` string,
  `one_rank_no` string, 
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,  
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_subject_monograph`(
  `id` string,
  `one_rank_id` string,
  `one_rank_no` string, 
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,  
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_subject`(
  `one_rank_id` string,
  `one_rank_no` string, 
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,  
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_keyword_journal`(
  `id` string,
  `keyword_id` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_product_keyword_conference`(
  `id` string,
  `keyword_id` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_keyword`(
  `keyword_id` string,
  `keyword` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_journal_rel_journal`(
  `id` string,
  `journal_id` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_journal`(
  `journal_id` string,
  `chinese_name` string,
  `english_name` string
  )
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_product_conference_rel_conference`(
  `id` string,
  `conference_id` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_conference`(
  `conference_id` string,
  `chinese_name` string,
  `english_name` string
  )
row format delimited fields terminated by '☔' stored as orc;



CREATE TABLE if not exists dm.`dm_es_product_journal`(
  `id` string, 
  `paper_type` string, 
  `chinese_name` string, 
  `english_name` string, 
  `doi` string, 
  `handle` string, 
  `first_author` string, 
  `first_author_id` string, 
  `correspondent_author` string, 
  `authors` string, 
  `fund_project` string, 
  `publish_date` string, 
  `article_no` string, 
  `keywords` string, 
  `includes` string, 
  `references` string, 
  `paper_rank` string, 
  `language` string, 
  `volume` string, 
  `issue` string, 
  `page_start` string, 
  `page_end` string, 
  `received_time` string, 
  `revised_time` string, 
  `accepted_time` string, 
  `firstonline_time` string, 
  `print_issn` string, 
  `online_issn` string, 
  `paper_award` string, 
  `isfulltext` string, 
  `fulltext_url` string, 
  `fulltext_path` string, 
  `abstract` string, 
  `citation` string, 
  `field_id` string, 
  `field_name` string, 
  `field_sub_id` string, 
  `field_sub_name` string, 
  `journal` string, 
  `journal_id` string, 
  `source` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE if not exists dm.`dm_es_product_conference`(
  `id` string, 
  `paper_type` string, 
  `chinese_name` string, 
  `english_name` string, 
  `doi` string, 
  `first_author` string, 
  `first_author_id` string, 
  `correspondent_author` string, 
  `authors` string, 
  `fund_project` string, 
  `publish_date` string, 
  `article_no` string, 
  `article_type` string, 
  `keywords` string, 
  `includes` string, 
  `references` string, 
  `paper_rank` string, 
  `language` string, 
  `page_start` string, 
  `page_end` string, 
  `received_time` string, 
  `revised_time` string, 
  `accepted_time` string, 
  `firstonline_time` string, 
  `print_issn` string, 
  `online_issn` string, 
  `paper_award` string, 
  `isfulltext` string, 
  `fulltext_url` string, 
  `fulltext_path` string, 
  `abstract` string, 
  `citation` string, 
  `conference` string, 
  `conference_type` string, 
  `conference_address` string, 
  `organization` string, 
  `start_date` string, 
  `end_date` string,
  `country` string, 
  `city` string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_es_product_patent`(
  `id` string, 
  `chinese_name` string, 
  `english_name` string, 
  `abstract` string, 
  `requirement` string, 
  `doi` string, 
  `authors` string,
  `language` string, 
  `apply_date` string, 
  `award_date` string, 
  `patent_no` string, 
  `patent_type` string, 
  `patent_type_original` string, 
  `country` string, 
  `applicant` string, 
  `application_address` string, 
  `category_no` string, 
  `secondary_category_no` string, 
  `cpic_no` string, 
  `part_no` string, 
  `publish_no` string, 
  `agent` string, 
  `agency` string, 
  `agent_address` string, 
  `patentee` string, 
  `ipc` string, 
  `cpc` string, 
  `issue_unit` string, 
  `current_status` string, 
  `publish_agency` string, 
  `hasfullext` string, 
  `fulltext_url` string, 
  `province` string, 
  `one_rank_id` string, 
  `one_rank_no` string, 
  `one_rank_name` string, 
  `two_rank_id` string, 
  `two_rank_no` string, 
  `two_rank_name` string,
  `source` string
  )
row format delimited fields terminated by '☔' stored as orc;



 CREATE TABLE dm.`dm_es_product_criterion`(
  `id` string, 
  `chinese_name` string, 
  `english_name` string, 
  `status` string, 
  `publish_date` string, 
  `implement_date` string, 
  `abolish_date` string, 
  `criterion_no` string, 
  `china_citerion_classification_no` string, 
  `in_criterion_classification_no` string, 
  `language` string, 
  `applicant` string, 
  `authors` string, 
  `charge_department` string, 
  `responsibility_department` string, 
  `publish_agency` string, 
  `hasfulltext` string, 
  `fulltext_url` string, 
  `source` string)
  row format delimited fields terminated by '☔' stored as orc;
  
 CREATE TABLE dm.`dm_es_product_monograph`(
  `id` string, 
  `chinese_name` string, 
  `english_name` string, 
  `authors` string, 
  `publish_date` string, 
  `hasfulltext` string, 
  `book_name` string, 
  `bookseriesname` string, 
  `language` string, 
  `status` string, 
  `isbn` string, 
  `editor` string, 
  `page_range` string, 
  `word_count` string, 
  `publisher` string, 
  `source` string)
  row format delimited fields terminated by '☔' stored as orc;
 
CREATE TABLE dm.`dm_es_journal`(
`id` string, 
`chinese_name` string, 
`english_name` string, 
`former_name` string, 
`cn` string, 
`issn` string, 
`language` string, 
`url` string, 
`cover_page` string, 
`organizer` string, 
`impact_factor` string, 
`fullimpact` string, 
`compleximpact` string, 
`publish_cycle` string, 
`establishtime` string, 
`publish_region` string, 
`field` string, 
`field_sub` string, 
`honor` string, 
`database_include` string, 
`include` string)
 row format delimited fields terminated by '☔' stored as orc;
 
CREATE TABLE dm.`dm_es_conference`(
`id` string, 
`conference` string, 
`conference_type` string, 
`conference_address` string, 
`organization` string, 
`country` string, 
`city` string)
 row format delimited fields terminated by '☔' stored as orc;
 
 CREATE TABLE dm.`dm_es_society`(
  `id` string, 
  `chinese_name` string, 
  `english_name` string, 
  `system` string, 
  `subject_category` string, 
  `address` string, 
  `establish_date` string, 
  `governing_body` string, 
  `support_unit` string, 
  `appointed_time` string, 
  `unit_member` string, 
  `individual_member` string, 
  `branch_num` string, 
  `picture_id` string)
row format delimited fields terminated by '☔' stored as orc;

