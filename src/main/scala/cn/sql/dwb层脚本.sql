

drop table dwb.wb_person_subject_tmp;
drop table dwb.wb_person_subject;
CREATE TABLE if not exists dwb.`wb_person_subject`(
  `person_id` string,
  `one_rank_id` string,
  `one_rank_no` string,
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,
  `two_rank_name` string,
  `one_rank_count` string,
  `two_rank_count` string)stored as orc;




CREATE TABLE if not exists dwb.`wb_keyword_subject`(
  `keyword_id` string,
  `one_rank_id` string,
  `one_rank_no` string,
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,
  `two_rank_name` string)stored as orc;
















drop table dwb.wb_product_conference_ms_nsfc_orcid_rel;
drop table dwb.wb_product_conference_ms_nsfc_rel;
drop table dwb.wb_product_criterion_csai_nsfc_rel		;
drop table dwb.wb_product_journal_csai_nsfc_ms_orcid_rel;
drop table dwb.wb_product_journal_csai_nsfc_ms_rel		;
drop table dwb.wb_product_journal_csai_nsfc_rel			;
drop table dwb.wb_product_monograph_csai_nsfc_ms_rel	;
drop table dwb.wb_product_monograph_csai_nsfc_rel		;
drop table dwb.wb_product_patent_csai_nsfc_ms_rel		;
drop table dwb.wb_product_patent_csai_nsfc_rel			;

CREATE TABLE if not exists dwb.wb_product_criterion_csai_nsfc_rel(
  `achievement_id_from` string,
  `achievement_id_to` string,
  `product_type` string,
  `flow_source` string
  );
CREATE TABLE if not exists dwb.wb_product_conference_ms_nsfc_orcid_rel LIKE dwb.wb_product_criterion_csai_nsfc_rel;
CREATE TABLE if not exists dwb.wb_product_conference_ms_nsfc_rel          LIKE dwb.wb_product_criterion_csai_nsfc_rel;
CREATE TABLE if not exists dwb.wb_product_criterion_csai_nsfc_rel		  LIKE dwb.wb_product_criterion_csai_nsfc_rel;
CREATE TABLE if not exists dwb.wb_product_journal_csai_nsfc_ms_orcid_rel  LIKE dwb.wb_product_criterion_csai_nsfc_rel;
CREATE TABLE if not exists dwb.wb_product_journal_csai_nsfc_ms_rel		  LIKE dwb.wb_product_criterion_csai_nsfc_rel;
CREATE TABLE if not exists dwb.wb_product_journal_csai_nsfc_rel			  LIKE dwb.wb_product_criterion_csai_nsfc_rel;
CREATE TABLE if not exists dwb.wb_product_monograph_csai_nsfc_ms_rel	  LIKE dwb.wb_product_criterion_csai_nsfc_rel;
CREATE TABLE if not exists dwb.wb_product_monograph_csai_nsfc_rel		  LIKE dwb.wb_product_criterion_csai_nsfc_rel;
CREATE TABLE if not exists dwb.wb_product_patent_csai_nsfc_ms_rel		  LIKE dwb.wb_product_criterion_csai_nsfc_rel;
CREATE TABLE if not exists dwb.wb_product_patent_csai_nsfc_rel			  LIKE dwb.wb_product_criterion_csai_nsfc_rel;




ALTER TABLE dwb.`wb_product_criterion_csai_nsfc` CHANGE achivement_id  achievement_id STRING;

//conference
drop table  dwb.`wb_product_conference_ms_nsfc_rel`;
CREATE TABLE if not exists dwb.`wb_product_conference_ms_nsfc_rel`(
  `achievement_id_ms` string,
  `achievement_id_nsfc` string,
  `product_type` string,
  `flow_source` string
  )
row format delimited fields terminated by '☔' stored as orc;

drop table  dwb.`wb_product_conference_ms_nsfc_orcid_rel`;
CREATE TABLE if not exists dwb.`wb_product_conference_ms_nsfc_orcid_rel`(
  `achievement_id_ms_nsfc` string,
  `achievement_id_orcid` string,
  `product_type` string,
  `flow_source` string
  )
row format delimited fields terminated by '☔' stored as orc;

drop table  dwb.`wb_product_conference_ms_nsfc`;
CREATE TABLE if not exists dwb.`wb_product_conference_ms_nsfc`(
  `achievement_id` string,
  `product_type` string,
  `chinese_title` string,
  `english_title` string,
  `doi` string,
  `first_author` string,
  `first_author_id` string,
  `correspondent_author` string,
  `authors` string,
  `fund_project` string,
  `publish_date` string,
  `article_no` string,
  `article_type` string,
  `keyword` string,
  `include` string,
  `reference` string,
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
  `flow_source` string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;

drop table  dwb.`wb_product_conference_ms_nsfc_orcid`;
CREATE TABLE if not exists dwb.`wb_product_conference_ms_nsfc_orcid`(
  `achievement_id` string,
  `product_type` string,
  `chinese_title` string,
  `english_title` string,
  `doi` string,
  `first_author` string,
  `first_author_id` string,
  `correspondent_author` string,
  `authors` string,
  `fund_project` string,
  `publish_date` string,
  `article_no` string,
  `article_type` string,
  `keyword` string,
  `include` string,
  `reference` string,
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
  `flow_source` string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;

--journal
drop table  dwb.`wb_product_journal_csai_nsfc_rel`;
CREATE TABLE if not exists dwb.`wb_product_journal_csai_nsfc_rel`(
  `achievement_id_csai` string,
  `achievement_id_nsfc` string,
  `product_type` string,
  `flow_source` string)
row format delimited fields terminated by '☔' stored as orc;


drop table  dwb.`wb_product_journal_ms_csai_nsfc_rel`;
CREATE TABLE if not exists dwb.`wb_product_journal_csai_nsfc_ms_rel`(
  `achievement_id_csai_nsfc` string,
  `achievement_id_ms` string,
  `product_type` string,
  `flow_source` string
  )
row format delimited fields terminated by '☔' stored as orc;

drop table  dwb.`wb_product_journal_ms_csai_nsfc_orcid_rel`;
CREATE TABLE if not exists dwb.`wb_product_journal_csai_nsfc_ms_orcid_rel`(
  `achievement_id_csai_nsfc_ms` string,
  `achievement_id_orcid` string,
  `product_type` string,
  `flow_source` string
  )
row format delimited fields terminated by '☔' stored as orc;

drop table  dwb.`wb_product_journal_csai_nsfc`;
CREATE TABLE if not exists dwb.`wb_product_journal_csai_nsfc`(
  `achievement_id` string,
  `paper_type` string,
  `chinese_title` string,
  `english_title` string,
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
  `flow_source` string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;

drop table  dwb.`wb_product_journal_ms_csai_nsfc`;
CREATE TABLE if not exists dwb.`wb_product_journal_csai_nsfc_ms`(
  `achievement_id` string,
  `paper_type` string,
  `chinese_title` string,
  `english_title` string,
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
  `flow_source` string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;

drop table  dwb.`wb_product_journal_ms_csai_nsfc_orcid`;
CREATE TABLE if not exists dwb.`wb_product_journal_csai_nsfc_ms_orcid`(
  `achievement_id` string,
  `paper_type` string,
  `chinese_title` string,
  `english_title` string,
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
  `flow_source` string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;

ALTER TABLE dwb.`wb_product_patent_csai_nsfc` CHANGE english_title  english_title STRING
---patent
drop table dwb.`wb_product_patent_csai_nsfc_rel`;
CREATE TABLE if not exists dwb.`wb_product_patent_csai_nsfc_rel`(
  `achievement_id_csai` string,
  `achievement_id_nsfc` string,
  `product_type` string,
  `flow_source` string)
row format delimited fields terminated by '☔' stored as orc;

drop table dwb.`wb_product_patent_csai_nsfc_ms_rel`;
CREATE TABLE if not exists dwb.`wb_product_patent_csai_nsfc_ms_rel`(
  `achievement_id_csai_nsfc` string,
  `achievement_id_ms` string,
  `product_type` string,
  `flow_source` string)
row format delimited fields terminated by '☔' stored as orc;

drop table dwb.`wb_product_patent_csai_nsfc`;
CREATE TABLE dwb.`wb_product_patent_csai_nsfc`(
  `achievement_id` string,
  `chinese_title` string,
  `english_title` string,
  `abstract` string,
  `requirement` string,
  `doi` string,
  `inventor` string,
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
  `flow_source` string,
  `source` string
  )
row format delimited fields terminated by '☔' stored as orc;

drop table dwb.`wb_product_patent_ms_csai_nsfc`;
CREATE TABLE dwb.`wb_product_patent_csai_nsfc_ms`(
  `achievement_id` string,
  `chinese_title` string,
  `english_title` string,
  `abstract` string,
  `requirement` string,
  `doi` string,
  `inventor` string,
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
  `flow_source` string,
  `source` string
  )
row format delimited fields terminated by '☔' stored as orc;




drop table dwb.`wb_product_criterion_csai_nsfc_rel`;
CREATE TABLE if not exists dwb.`wb_product_criterion_csai_nsfc_rel`(
  `achievement_id_csai` string,
  `achievement_id_nsfc` string,
  `product_type` string,
  `flow_source` string)
row format delimited fields terminated by '☔' stored as orc;


drop table dwb.`wb_product_criterion_csai_nsfc`;
  CREATE TABLE dwb.`wb_product_criterion_csai_nsfc`(
  `achievement_id` string,
  `chinese_title` string,
  `english_title` string,
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
  `flow_source` string,
  `source` string)
  row format delimited fields terminated by '☔' stored as orc;

drop table dwb.`wb_product_monograph_csai_nsfc_rel`;
CREATE TABLE if not exists dwb.`wb_product_monograph_csai_nsfc_rel`(
  `achievement_id_csai` string,
  `achievement_id_nsfc` string,
  `product_type` string,
  `flow_source` string
  )
row format delimited fields terminated by '☔' stored as orc;

drop table dwb.`wb_product_monograph_ms_csai_nsfc_rel`;
CREATE TABLE if not exists dwb.`wb_product_monograph_csai_nsfc_ms_rel`(
  `achievement_id_csai_nsfc` string,
  `achievement_id_ms` string,
  `product_type` string,
  `flow_source` string
  )
row format delimited fields terminated by '☔' stored as orc;

drop table dwb.`wb_product_monograph_csai_nsfc`;
CREATE TABLE dwb.`wb_product_monograph_csai_nsfc`(
  `achievement_id` string,
  `chinese_title` string,
  `english_title` string,
  `author` string,
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
  `flow_source` string,
  `source` string)
  row format delimited fields terminated by '☔' stored as orc;

 drop table dwb.`wb_product_monograph_ms_csai_nsfc`;
  CREATE TABLE dwb.`wb_product_monograph_csai_nsfc_ms`(
  `achievement_id` string,
  `chinese_title` string,
  `english_title` string,
  `author` string,
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
  `flow_source` string,
  `source` string)
  row format delimited fields terminated by '☔' stored as orc;











  
CREATE TABLE if not exists dwb.`wb_product_reward_csai_nsfc_rel`(
  `achievement_id_csai` string, 
  `achievement_id_nsfc` string,
  `product_type` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dwb.`wb_product_reward_csai_nsfc`(
  `achievement_id` string,  
  `chinese_title` string, 
  `english_title` string,
  `language` string,  
  `project_owner` string, 
  `date` string, 
  `reward_type` string, 
  `reward_level` string, 
  `reward_rank` string, 
  `issued_by` string, 
  `project_no` string, 
  `source` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dwb.`wb_product_all_rel`(
  `achievement_id` string,  
  `original_achievement_id` string, 
  `type` string)
row format delimited fields terminated by '☔' stored as orc;



CREATE TABLE dwb.`wb_product_all_subject`(
  `achievement_id` string,  
  `one_rank_id` string, 
  `one_rank_no` string,
  `one_rank_name` string,  
  `two_rank_id` string, 
  `two_rank_no` string, 
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dwb.`wb_product_all_person`(
  `person_id` string,  
  `achievement_id` string,
   product_type string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dwb.`wb_product_project_rel`(
  `project_id` string,  
  `achievement_id` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dwb.`wb_product_all_keyword`(
  `achievement_id` string,  
  `keyword_id` string, 
  `keyword` string,
  `type` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dwb.`wb_keyword_subject`(
  `keyword_id` string,
  `keyword` string,
  `one_rank_id` string, 
  `one_rank_no` string,
  `one_rank_name` string,  
  `two_rank_id` string, 
  `two_rank_no` string, 
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dwb.`wb_product_organization_all`(
  `achievement_id` string,
  `org_id` string,
  `org_name` string, 
  `type` string)
row format delimited fields terminated by '☔' stored as orc;

  
CREATE TABLE dwb.`wb_product_journal_rel_journal`(
  `achievement_id` string,  
  `journal_id` string, 
  `journal_name` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dwb.`wb_product_conference_rel_conference`(
  `achievement_id` string,  
  `conference_id` string, 
  `conference` string)
row format delimited fields terminated by '☔' stored as orc;









  
  

