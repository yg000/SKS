
drop table dm.`dm_neo4j_product_journal`;
CREATE TABLE dm.`dm_neo4j_product_journal`(
  `id` string,
  `paper_type` string,
  `chinese_name` string, 
  `english_name` string,
  `authors` string,
  `publish_date` string,  
  `language` string, 
  `includes` string, 
  `journal` string,
  flow_source string,
  source string)
row format delimited fields terminated by '☔' stored as orc;

drop table dm.`dm_neo4j_product_conference`;
CREATE TABLE dm.`dm_neo4j_product_conference`(
  `id` string,
  `paper_type` string,
  `chinese_name` string,
  `english_name` string,
  `authors` string,
  `publish_date` string,
  `language` string,
  `includes` string,
  `conference` string,
  flow_source string,
  source string)
row format delimited fields terminated by '☔' stored as orc;

drop table dm.`dm_neo4j_product_patent`;
CREATE TABLE dm.`dm_neo4j_product_patent`(
  `id` string,
  `paper_type` string,
  `chinese_name` string,
  `english_name` string,
  `authors` string,
  `language` string,
  `applicant` string,
  flow_source string,
  source string)
row format delimited fields terminated by '☔' stored as orc;

drop table dm.`dm_neo4j_product_criterion`;
CREATE TABLE dm.`dm_neo4j_product_criterion`(
  `id` string,
  `paper_type` string,
  `chinese_name` string,
  `english_name` string,
  `authors` string,
  `language` string,
  `applicant` string,
  flow_source string,
  source string)
row format delimited fields terminated by '☔' stored as orc;


drop table dm.`dm_neo4j_product_monograph`;
CREATE TABLE dm.`dm_neo4j_product_monograph`(
  `id` string,
  `paper_type` string,
  `chinese_name` string,
  `english_name` string,
  `authors` string,
  `language` string,
  `book_name` string,
  `editor` string,
  flow_source string,
  source string)
row format delimited fields terminated by '☔' stored as orc;

drop table dm.`dm_neo4j_person`;
CREATE TABLE `dm.dm_neo4j_person`(
  `id` string,
  `zh_name` string,
  `en_name` string,
  `gender` string,
  `nation` string,
  `birthday` string,
  `birthplace` string,
  `org_name` string,
  `prof_title` string,
  `nationality` string,
  `province` string,
  `city` string,
  `degree` string,
  flow_source string,
  source string
  )stored as orc;



CREATE TABLE `dm.dm_neo4j_project`(
  `id` string COMMENT '唯一标识',
  `zh_title` string COMMENT '项目中文名称',
  `en_title` string COMMENT '项目英文名称',
  `prj_no` string COMMENT '项目批准号',
  `person_id` string COMMENT '项目负责人id',
  `psn_name` string COMMENT '项目负责人名称',
  `org_name` string COMMENT '单位名字',
  `grant_code` string COMMENT '资助类别',
  `grant_name` string COMMENT '资助类别代码',
  `subject_code1` string COMMENT '申请代码1',
  `subject_code2` string COMMENT '申请代码2',
  `start_date` string COMMENT '开始日期',
  `end_date` string COMMENT '截止日期',
  `approval_year` string COMMENT '批准年份',
  `duration` string COMMENT '期限',
  `status` string COMMENT '项目状态',
  `source` string COMMENT '数据来源')stored as orc;

drop table dm.`dm_neo4j_organization`;
CREATE TABLE dm.`dm_neo4j_organization`(
  `id` string,
  `org_name` string,
  `en_name` string,
  `alias` string,
  `org_type` string,
  `country` string,
  `province` string,
  `city` string,
  source string)stored as orc;



CREATE TABLE dm.`dm_neo4j_keyword`(
  `id` string,
  `zh_keyword` string,
  `en_keyword` string
  )row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_subject`(
  `id` string,
  `one_rank_no` string,
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_journal`(
  `id` string,
  `chinese_name` string,
  `english_name` string,
  `language` string,
  `organizer` string,
  `fullimpact` string,
  `compleximpact` string,
  `publish_region` string

  ) stored as orc;

CREATE TABLE dm.`dm_neo4j_conference`(
  `id` string,
  `conference` string,
  `organization` string)
stored as orc;

CREATE TABLE if not exists dm.`dm_neo4j_society`(
  `id` string,
  `chinese_name` string,
  `english_name` string,
  `subject_category` string,
  `address` string,
  `governing_body` string,
  `support_unit` string,
  `unit_member` string,
  `individual_member` string
  )stored as orc;







CREATE TABLE dm.`dm_neo4j_person_product`(
  `person_id` string,
  `achievement_id` string)
row format delimited fields terminated by '☔' stored as orc;


CREATE TABLE dm.`dm_neo4j_product_keyword`(
  `achievement_id` string,
  `keyword_id` string)
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_product_subject`(
  `achievement_id` string,
  `one_rank_id` string,
  `one_rank_no` string,
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,
  `two_rank_name` string)
row format delimited fields terminated by '☔' stored as orc;



CREATE TABLE dm.`dm_neo4j_journal_rel_product_journal`(
  `journal_id` string,
  `achievement_id` string
  )
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_conference_rel_product_conference`(
  `conference_id` string,
  `achievement_id` string
  )
row format delimited fields terminated by '☔' stored as orc;

CREATE TABLE dm.`dm_neo4j_project_product`(
  `project_id` string,
  `achievement_id` string)
stored as orc;


CREATE TABLE dm.`dm_neo4j_organization_product`(
  `organization_id` string,
  `achievement_id` string)
stored as orc;

CREATE TABLE dm.`dm_neo4j_society_organization`(
  `society_id` string,
  `organization_id` string)
stored as orc;

CREATE TABLE dm.`dm_neo4j_journal_organization`(
  `journal_id` string,
  `organization_id` string)
stored as orc;


CREATE TABLE dm.`dm_neo4j_person_organization`(
  `person_id` string,
  `organization_id` string)
stored as orc;

CREATE TABLE dm.`dm_neo4j_project_keyword`(
  `project_id` string,
  `keyword_id` string)
stored as orc;


CREATE TABLE dm.`dm_neo4j_person_keyword`(
  `person_id` string,
  `keyword_id` string)
stored as orc;


CREATE TABLE dm.`dm_neo4j_project_subject`(
  `project_id` string,
  `two_rank_id` string)
stored as orc;


CREATE TABLE if not exists dm.`dm_neo4j_person_subject`(
  `person_id` string,
  `one_rank_id` string,
  `one_rank_no` string,
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,
  `two_rank_name` string,
  `one_rank_count` string,
  `two_rank_count` string
  )stored as orc;


CREATE TABLE if not exists dm.`dm_neo4j_keyword_subject`(
  `keyword_id` string,
  `one_rank_id` string,
  `one_rank_no` string,
  `one_rank_name` string,
  `two_rank_id` string,
  `two_rank_no` string,
  `two_rank_name` string
  )stored as orc;

CREATE TABLE if not exists dm.`dm_neo4j_society_person`(
  `society_id` string,
  `person_id` string
  )stored as orc;




















create table dm.dm_es_project(
 id                           string    comment   '唯一标识'
,zh_title                     string    comment   '项目中文名称'
,en_title                     string    comment   '项目英文名称'
,prj_no                       string    comment   '项目批准号'
,person_id                    string    comment   '项目负责人id'
,psn_name                     string    comment   '项目负责人名称'
,org_name                     string    comment   '单位名字'
,grant_code                   string    comment   '资助类别'
,grant_name                   string    comment   '资助类别代码'
,subject_code1                string    comment   '申请代码1'
,subject_code2                string    comment   '申请代码2'
,start_date                   string    comment   '开始日期'
,end_date                     string    comment   '截止日期'
,approval_year                string    comment   '批准年份'
,duration                     string    comment   '期限'
,status                       string    comment   '项目状态'
,csummary                     string    comment   '中文摘要'
,esummary                     string    comment   '英文摘要'
,post_dr_no                   string    comment   '博士后人数'
,dr_candidate_no              string    comment   '博士生人数'
,ms_candidate_no              string    comment   '硕士生人数'
,middle_no                    string    comment   '中级人数'
,junior_no                    string    comment   '初级人数'
,senior_no                    string    comment   '高级人数'
,no_of_unit                   string    comment   '单位数'
,inv_no                       string    comment   '总人数'
,total_amt                    string    comment   '资助金额（直接经费）'
,total_inamt                  string    comment   '已拨入金额'
,change_amt                   string    comment   '调整金额'
,all_amt                      string    comment   '项目总经费'
,indirect_amt                 string    comment   '间接经费'
,sbgz_amt                     string    comment   '设备购置费'
,ind_inamt                    string    comment   '间接经费已拨入金额'
,source                       string    comment   '数据来源'
) stored as orc ;

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

