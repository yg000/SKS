
create table  IF NOT EXISTS   `o_criterion`(
  `achivement_id` string, 
  `chinese_name` string, 
  `english_name` string, 
  `status` string, 
  `release_date` string, 
  `apply_date` string, 
  `abolish_date` string, 
  `criterion_no` string, 
  `ch_criterion_no` string, 
  `in_criterion_no` string, 
  `language` string, 
  `draft_org` string, 
  `draft_author` string, 
  `charge_department` string, 
  `responsibility_department` string, 
  `publish_org` string, 
  `source` string)
row format delimited fields terminated by '☔' stored as orc;


insert overwrite table o_criterion
select
criterion_id as achivement_id
,chinese_name
,english_name
,status
,release_date
,apply_date
,abolish_date
,criterion_no
,ch_criterion_no
,in_criterion_no
,language
,draft_org
,draft_author
,charge_department
,responsibility_department
,publish_org
,if(true,'csai',null) as source
from csai_basic.criterion;


create table  IF NOT EXISTS   o_criterion_subject(
  `achivement_id` string, 
  `rank_id` string, 
  `rank_no` string, 
  `one_rank_id` string, 
  `one_rank_name` string, 
  `one_rank_no` string, 
  `two_rank_id` string, 
  `two_rank_name` string, 
  `two_rank_no` string, 
  `three_rank_id` string, 
  `three_rank_name` string, 
  `three_rank_no` string, 
  `source` string)
  row format delimited fields terminated by '☔' stored as orc;
  
 
insert overwrite table o_criterion_subject
select
criterion_id as achivement_id
,rank_id
,rank_no
,one_rank_id
,one_rank_name
,one_rank_no
,two_rank_id
,two_rank_name
,two_rank_no
,three_rank_id
,three_rank_name
,three_rank_no
,if(true,'csai',null) as source
from csai_basic.criterion_rank;


create table  IF NOT EXISTS   o_criterion_person(
  person_id string,
  chinese_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_criterion_person
select
new_person_id as person_id
,chinese_name
,if(true,'csai',null) as source
from csai_basic.person_1;

 
create table  IF NOT EXISTS   o_criterion_org(
  achivement_id string,
  org_id string,
  org_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_criterion_org
select
 criterion_id as achivement_id
,org_id
,org_name
,if(true,'csai',null) as source
from csai_basic.criterion_org;



create table  IF NOT EXISTS   o_criterion_author(
  achivement_id string,
  person_id string,
  person_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_criterion_author
select
 criterion_id as achivement_id
,new_person_id as person_id
,person_name
,if(true,'csai',null) as source
from csai_basic.criterion_author_1;

--论文

create table  IF NOT EXISTS   o_paper_person(
  person_id string,
  chinese_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_person
select
new_person_id as person_id
,chinese_name
,if(true,'csai',null) as source
from csai_basic.person_2_2_2_2_2;



create table  IF NOT EXISTS   o_paper_person_work_experience(
  person_id string,
  start_date string,
  end_date string,
  org_id string,
  org_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_person_work_experience
select
new_person_id as person_id
,start_date
,end_date
,org_id
,org_name
,if(true,'csai',null) as source
from csai_basic.work_experience_22;

--论文人的研究领域

create table  IF NOT EXISTS   o_paper_person_research_area(
  person_id string,
  research_area string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_person_research_area
select
new_person_id as person_id
,research_area
,if(true,'csai',null) as source
from csai_basic.research_area_1_1_1_1_1;


create table  IF NOT EXISTS   o_keyword(
  keyword_id string,
  keyword string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_keyword
select
 keyword_id
,keyword
,if(true,'csai',null) as source
from csai_basic.keyword;

create table  IF NOT EXISTS   o_paper_keyword(
  achivement_id string,
  keyword_id string,
  keyword string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_keyword
select
 paper_id as achivement_id
,keyword_id
,keyword
,if(true,'csai',null) as source
from csai_basic.paper_keyword;


create table  IF NOT EXISTS   o_paper_author(
  achivement_id string,
  person_id string,
  person_name string,
  org_id string,
  org_name string,
  isfirstauthor string,
  url string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_author
select
 paper_id as achivement_id
,new_person_id as person_id
,person_name
,org_id
,org_name
,isfirstauthor
,create_source as url
,if(true,'csai',null) as source
from csai_basic.paper_author_11;


create table  IF NOT EXISTS   o_paper_ref(
  achivement_id string,
  ref_id string,
  ref_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_ref
select
 paper_id as achivement_id
,ref_id
,ref_name
,if(true,'csai',null) as source
from csai_basic.paper_ref;


create table  IF NOT EXISTS   o_paper(
  achievement_id string,
  chinese_name string,
  paper_rank string,
  first_author string,
  
  authors string,
  fund_project string,
  first_author_id string,
  publish_date string,
  keywords string,
  
  
  includes string,
  references string,
  journal string,
  language string,
  volume string,
  
  issue string,
  fulltext_url string,
  abstract string,
  citation string,
  field_name string,
  
  
  field_sub_name string,
  field_id string,
  field_sub_id string,
  journal_id string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper
select
 paper_id as achievement_id
,chinese_name
,paper_rank
,first_author
,authors

,fund_project
,first_author_id
,publish_date
,keywords
,includes

,references
,journal
,language
,volume
,issue

,fulltext_url
,abstract
,citation
,field_name
,field_sub_name

,field_id
,field_sub_id
,journal_id
,if(true,'csai',null) as source
from csai_basic.paper;

--论文的引用量
create table  IF NOT EXISTS   o_paper_citation(
  achievement_id string,
  citation string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_citation
select
 paper_id as achivement_id
,if(citation is null,"0",citation) as citation
,if(true,'csai',null) as source
from csai_basic.paper_citation;
--论文收录情况
create table  IF NOT EXISTS   o_paper_include(
  achievement_id string,
  include_id string,
  include_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_include
select
 paper_id as achievement_id
,include_id
,include_name
,if(true,'csai',null) as source
from csai_basic.paper_include;

--论文对应的一级领域
create table  IF NOT EXISTS   o_paper_field(
  achievement_id string,
  field_id string,
  field_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_field
select
 paper_id as achievement_id
,field_id
,field_name
,if(true,'csai',null) as source
from csai_basic.paper_field;

--论文对应的二级领域

create table  IF NOT EXISTS   o_paper_field_sub(
  achievement_id string,
  field_sub_id string,
  field_sub_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_field_sub
select
 paper_id as achievement_id
,field_sub_id
,field_sub_name
,if(true,'csai',null) as source
from csai_basic.paper_field_sub;

--论文对应的期刊

create table  IF NOT EXISTS   o_paper_journal(
  achievement_id string,
  journal_id string,
  paper_name string,
  journal_name string,
  volume string,
  issue string,
  publish_year string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_journal
select
 paper_id as achievement_id
,journal_id
,paper_name
,journal_name
,volume
,issue
,publish_year
,if(true,'csai',null) as source
from csai_basic.paper_journal;

--论文的学科


create table  IF NOT EXISTS   o_paper_subject(
  `achievement_id` string, 
  `rank_id` string, 
  `rank_no` string, 
  `one_rank_id` string, 
  `one_rank_name` string, 
  `one_rank_no` string, 
  `two_rank_id` string, 
  `two_rank_name` string, 
  `two_rank_no` string, 
  `three_rank_id` string, 
  `three_rank_name` string, 
  `three_rank_no` string, 
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_paper_subject
select
paper_id as achievement_id
,rank_id
,rank_no
,one_rank_id
,one_rank_name
,one_rank_no
,two_rank_id
,two_rank_name
,two_rank_no
,three_rank_id
,three_rank_name
,three_rank_no
,if(true,'csai',null) as source
from csai_basic.paper_rank;
  
--论文资助项目
create table  IF NOT EXISTS   o_paper_fund_project(
  achievement_id string,
  project_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_paper_fund_project
select
 paper_id as achievement_id
,project_name
,if(true,'csai',null) as source
from csai_basic.fund_project;

--收录的种类
create table  IF NOT EXISTS   o_include(
  id string,
  name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_include
select
 id
,name
,if(true,'csai',null) as source
from csai_basic.paper_collection;

--专利的主体表
create table  IF NOT EXISTS   o_patent(
  achievement_id string,
  chinese_name string,
  abstract string,
  requirement string,
  apply_date string,
  
  award_date string,
  patent_no string,
  patent_type string,
  patent_type_original string,
  country string,
  applicant string,
  
  
  is_person string,
  address string,
  rank_no string,
  rank_no_2 string,
  cpic_no string,
  
  part_no string,
  publish_no string,
  agent_person string,
  agent_org string,
  agent_address string,
  
  
  ipc_classify string,
  loc_classify string,
  inventor string,
  province string,
  one_rank_id string,
  
  `one_rank_no` string,
  one_rank_name string,
  two_rank_id string,
  two_rank_no string,
  two_rank_name string,
  
  
  part string,
  category string,
  ipc_category string,
  ipc_sub_part string,
  loc_name string,
  
  loc_sub_category string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_patent
select
 patent_id as achievement_id
,chinese_name
,abstract
,requirement
,apply_date

,award_date
,patent_no
,patent_type
,patent_type_original
,country
,applicant

,is_person
,address
,rank_no
,rank_no_2
,cpic_no

,part_no
,publish_no
,agent_person
,agent_org
,agent_address

,ipc_classify
,loc_classify
,inventor
,province
,one_rank_id

,one_rank_no
,one_rank_name
,two_rank_id
,two_rank_no
,two_rank_name

,part
,category
,ipc_category
,ipc_sub_part
,loc_name

,loc_sub_category
,if(true,'csai',null) as source
from csai_basic.patent;

--专利的作者

create table  IF NOT EXISTS   o_patent_inventor(
  achievement_id string,
  person_id string,
  person_name string,
  org_id string,
  org_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_patent_inventor
select
 patent_id as achievement_id
,new_person_id as person_id
,person_name
,org_id
,org_name
,if(true,'csai',null) as source
from csai_basic.patent_inventor_11;

--专利的学科

create table  IF NOT EXISTS   o_patent_subject(
  `achievement_id` string, 
  `rank_id` string, 
  `rank_no` string, 
  `one_rank_id` string, 
  `one_rank_name` string, 
  `one_rank_no` string, 
  `two_rank_id` string, 
  `two_rank_name` string, 
  `two_rank_no` string, 
  `three_rank_id` string, 
  `three_rank_name` string, 
  `three_rank_no` string, 
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_patent_subject
select
patent_id as achievement_id
,rank_id
,rank_no
,one_rank_id
,one_rank_name
,one_rank_no
,two_rank_id
,two_rank_name
,two_rank_no
,three_rank_id
,three_rank_name
,three_rank_no
,if(true,'csai',null) as source
from csai_basic.patent_rank;
  
--杰出人才的人

create table  IF NOT EXISTS   o_outstanding_person(
  `person_id` string, 
  `chinese_name` string, 
  `gender` string, 
  `birthday` string, 
  `ethnicity` string, 
  `birthplace` string, 
  `nationality` string, 
  `brief_description` string, 
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_outstanding_person
select
new_person_id as person_id
,chinese_name
,gender
,birthday
,ethnicity
,birthplace
,nationality
,brief_description
,if(true,'csai',null) as source
from csai_basic.out_person_1;

--杰出人才人的社会任职

create table  IF NOT EXISTS   o_outstanding_person_social(
  `person_id` string, 
  `start_date` string, 
  `end_date` string, 
  `org_id` string, 
  `org_name` string, 
  `title` string, 
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_outstanding_person_social
select
new_person_id as person_id
,start_date
,end_date
,org_id
,org_name
,title
,if(true,'csai',null) as source
from csai_basic.out_social_1_1;

--杰出人才的教育经历

create table  IF NOT EXISTS   o_outstanding_person_education(
  `person_id` string, 
  `start_date` string, 
  `end_date` string, 
  `major` string, 
  `degree` string, 
  `degree_year` string, 
  `org_id` string, 
  `org_name` string, 
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_outstanding_person_education
select
new_person_id as person_id
,start_date
,end_date
,major
,degree
,degree_year
,org_id
,nationality
,org_name
,if(true,'csai',null) as source
from csai_basic.out_education_1_1;

--杰出人才人的研究领域

create table  IF NOT EXISTS   o_outstanding_person_area(
  `person_id` string, 
  `research_area` string, 
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_outstanding_person_area
select
new_person_id as person_id
,research_area
,if(true,'csai',null) as source
from csai_basic.out_area_1;

--杰出人才的荣获奖项


create table  IF NOT EXISTS   o_outstanding_person_awards(
  `person_id` string, 
  `award_date` string, 
   award_name string,
   award_rank string,
   content string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_outstanding_person_awards
select
new_person_id as person_id
,`date` as award_date
,award_name
,award_rank
,content
,if(true,'csai',null) as source
from csai_basic.out_awards_1;

--杰出人才的称号

create table  IF NOT EXISTS   o_outstanding_person_title(
  `person_id` string, 
  `title` string, 
  `selection_time` string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_outstanding_person_title
select
new_person_id as person_id
,`name` as title
,`date` as selection_time
,if(true,'csai',null) as source
from csai_basic.out_outstanding_1;

--杰出人才的工作经历

create table  IF NOT EXISTS   o_outstanding_person_work_experience(
  `person_id` string, 
  `start_date` string, 
  `end_date` string,
   org_id string,
   org_name string,
   title string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_outstanding_person_work_experience
select
new_person_id as person_id
,`start_date`
,`end_date`
,org_id
,org_name
,title
,if(true,'csai',null) as source
from csai_basic.out_work_1_1;

--期刊的一级领域

create table  IF NOT EXISTS   o_field(
  `field_id` string, 
   field_name string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_field
select
 field_id
,field_name
,if(true,'csai',null) as source
from csai_basic.field;

--期刊的二级领域

create table  IF NOT EXISTS   o_field_sub(
  `field_sub_id` string, 
   field_sub_name string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_field_sub
select
 field_sub_id
,field_sub_name
,if(true,'csai',null) as source
from csai_basic.field_sub;

--领域的关系表


 create table  IF NOT EXISTS   o_field_ref(
  `field_id` string, 
   field_sub_id string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_field_sub
select
 field_id
,field_sub_id
,if(true,'csai',null) as source
from csai_basic.field_ref;
 
  
--期刊表

 create table  IF NOT EXISTS   o_journal(
  `journal_id` string, 
   chinese_name string,
  `english_name` string,
  
  `former_name` string, 
   cn string,
  `issn` string,
  
  `language` string, 
   url string,
  `organizer` string,
  
  `cover_page` string, 
   impact_factor string,
  `fullimpact` string,
  
  
  `compleximpact` string, 
   publish_cycle string,
  `establishtime` string,
  
  `publish_region` string, 
   field string,
  `field_sub` string,
  
  `honor` string, 
   database_include string,
  `include` string,
  
  `citation` string,
   source string
   
  
  )
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_journal
select
 journal_id
,chinese_name
,english_name
,former_name
,cn
,issn
,`language`
,url
,organizer
,cover_page
,impact_factor
,fullimpact
,compleximpact
,publish_cycle
,establishtime
,publish_region
,field
,field_sub
,honor
,database_include
,include
,citation
,if(true,'csai',null) as source
from csai_basic.journal;

--专著的作者表

create table  IF NOT EXISTS   o_monograph_author(
  achievement_id string,
  person_id string,
  person_name string,
  org_id string,
  org_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_monograph_author
select
 monograph_id as achievement_id
,new_person_id as person_id
,person_name
,org_id
,org_name
,if(true,'csai',null) as source
from csai_basic.monograph_author_1;


--专著表

create table  IF NOT EXISTS   o_monograph(
  achievement_id string,
  chinese_name string,
  author string,
  publish string,
  publish_time string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_monograph
select
 monograph_id as achievement_id
,chinese_name
,author
,publish
,publish_time
,if(true,'csai',null) as source
from csai_basic.monograph;

--奖励的完成人

create table  IF NOT EXISTS   o_reward_person(
  achievement_id string,
  person_id string,
  person_name string,
  isfirstowner string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_reward_person
select
 project_id as achievement_id
,new_person_id as person_id
,person_name
,isfirstowner
,if(true,'csai',null) as source
from csai_basic.reward_person_1;

--奖励的机构

create table  IF NOT EXISTS   o_reward_org(
  achievement_id string,
  org_id string,
  org_name string,
  isfirstorg string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_reward_org
select
 project_id as achievement_id
,org_id
,org_name
,isfirstorg
,if(true,'csai',null) as source
from csai_basic.reward_org;

--奖励项目的主体表

create table  IF NOT EXISTS   o_reward_project(
  achievement_id string,
  chinese_name string,
  reward_date string,
  reward_type string,
  reward_field string,
  
  field_no string,
  country string,
  `language` string,
  reward_rank string,
  project_no string,
  
  reward_recommended_org string,
  url string,
  source string
  
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_reward_project
select
 project_id as achievement_id
,chinese_name
,reward_date
,reward_type
,reward_field

,field_no
,country
,`language`
,reward_rank
,project_no

,reward_recommended_org
,url
,if(true,'csai',null) as source
from csai_basic.reward_project;

--人直接获得的项目

create table  IF NOT EXISTS   o_relation_person_own_highest_coop(
  person_id string,
  person_name string,
  gender string,
  birthday string,
  death_date string,
  
  research_area string,
  nationality string,
  academician string,
  title string,
  org string,
  
  reward_type string,
  `date` string,
  source string
  
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_relation_person_own_highest_coop
select
 person_id
,person_name
,gender
,birthday
,death_date

,research_area
,nationality
,academician
,title
,org

,reward_type
,`date`
,source
from csai_basic.relation_person_own_highest_coop;

--学会的支撑单位

create table  IF NOT EXISTS   o_society_support_org(
  society_id string,
  org_category string,
  org_id string,
  org_name string,
  source string
  
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_society_support_org
select
 society_id
,org_category
,org_id
,org_name
,if(true,'csai',null) as source
from csai_basic.society_support_org_11;

--学会的id

create table  IF NOT EXISTS   o_society_id(
  society_id string,
  id_society string,
  name string,
  source string
  
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_society_id
select
 society_id
,id_society 
,chinese_name as name
,if(true,'csai',null) as source
from csai_basic.society_id;

--学会的人

create table  IF NOT EXISTS   o_society_person(
  person_id string,
  person_name string,
  society_id string,
  
  society_name string,
  secondary_attribute string,
  secondary string,
  
  society_job string,
  legal_person string,
  start_time string,
  end_time string,
  source string
  
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_society_person
select
 new_person_id as person_id
,person_name 
,society_id
,society_name
,secondary_attribute
,secondary
,society_job
,legal_person
,start_time
,end_time
,if(true,'csai',null) as source
from csai_basic.society_person_1;

--学会的主体表

create table  IF NOT EXISTS   o_society(
  society_id string,
  chinese_name string,
  system string,
  
  subject_category string,
  address string,
  establish_date string,
  
  governing_body string,
  support_unit string,
  appointed_time string,
  unit_member string,
  individual_member string,
  
  branch_num string,
  source string
  
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_society
select
 society_id
,chinese_name 
,system
,subject_category
,address

,establish_date
,governing_body
,support_unit
,appointed_time
,unit_member

,individual_member
,branch_num
,if(true,'csai',null) as source
from csai_basic.society;

--学会人的工作经历

create table  IF NOT EXISTS   o_society_person_work(
  society_id string,
  person_id string,
  person_name string,
  
  org_id string,
  org_name string,
  source string
  
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_society_person_work
select
 society_id
,new_person_id as person_id 
,person_name
,org_id
,org_name
,if(true,'csai',null) as source
from csai_basic.society_work_11;


--师承数据

create table  IF NOT EXISTS   o_student_teacher(
  student_id string,
  student_name string,
  student_org string,
  student_org_id string,
  
  teacher_id string,
  teacher_name string,
  teacher_org string,
  teacher_org_id string,
  
  `year` string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_student_teacher
select
 student_id
,student_name
,student_org
,student_org_id

,teacher_id
,teacher_name
,teacher_org
,teacher_org_id
,`year`
,if(true,'csai',null) as source
from csai_basic.student_teacher_11;

--总人表

create table  IF NOT EXISTS   o_person_all(
  `person_id` string, 
  `chinese_name` string,
  `english_name` string,  
  `gender` string, 
  `birthday` string, 
  `ethnicity` string, 
  `birthplace` string, 
  `nationality` string, 
  `brief_description` string,
  current_organization string,
  title string,
  isacademician string,
  isoutstanding string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_person_all
select
new_person_id as person_id
,chinese_name
,english_name
,gender
,birthday
,ethnicity
,birthplace
,nationality
,brief_description
,current_organization
,title
,isacademician
,isoutstanding
,if(true,'csai',null) as source
from csai_basic.person_all;

--总人的工作经历表

create table  IF NOT EXISTS   o_person_work_experience_all(
  `person_id` string, 
  `start_date` string, 
  `end_date` string, 
  `org_id` string, 
  `org_name` string, 
  `title` string, 
  nationality string,
  province string,
  city string,
  address string,
  isservice string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_person_work_experience_all
select
new_person_id as person_id
,start_date
,end_date
,org_id
,org_name
,title
,nationality
,province
,city
,address
,isservice
,if(true,'csai',null) as source
from csai_basic.wrok_experience_all;

--总机构表

create table  IF NOT EXISTS   o_organization_all(
  `org_id` string, 
  `org_name` string, 
  `type` string, 
  `nationality` string, 
  `province` string, 
  `city` string, 
  address string,
  longitude string,
  latitude string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_organization_all
select
org_id
,org_name
,`type`
,nationality
,province
,city
,address
,longitude
,latitude
,if(true,'csai',null) as source
from csai_basic.organization_2;

--微软期刊表
create table  IF NOT EXISTS   o_ms_journal(
  `journal_id` string, 
   english_name string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_ms_journal
select
journal_id
,english_name
,if(true,'csai_ms',null) as source
from csai_basic.ms_journal;

--微软机构表
create table  IF NOT EXISTS   o_ms_organization(
  `org_id` string,
   org_ms_id string,  
   org_name string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_ms_organization
select
 org_id
,org_ms_id
,org_en_name as org_name
,if(true,'csai_ms',null) as source
from csai_basic.ms_organization;


--微软人表
create table  IF NOT EXISTS   o_ms_person(
  `author_id` string,
   person_id string,  
   english_name string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_ms_person
select
 author_id
,person_id
,english_name
,if(true,'csai_ms',null) as source
from csai_basic.ms_person;

--微软人的工作经历表
create table  IF NOT EXISTS   o_ms_person_work_experience(
  `person_id` string,
   org_id string,  
   org_name string,
  `source` string)
row format delimited fields terminated by '☔' stored as orc;
  

insert overwrite table o_ms_person_work_experience
select
 person_id
,org_id
,org_name
,if(true,'csai_ms',null) as source
from csai_basic.ms_work_experience;

--微软论文作者表

create table  IF NOT EXISTS   o_ms_paper_author(
  id string,
  achivement_id string,
  person_id string,
  person_name string,
  org_id string,
  org_name string,
  isfirstauthor string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_ms_paper_author
select
 id
,paper_id as achievement_id
,person_id
,person_name
,org_id
,org_name
,isfirstauthor
,if(true,'csai',null) as source
from csai_basic.ms_paper_author;


--微软学术论文的参照关系数据

create table  IF NOT EXISTS   o_ms_paper_ref(
  achievement_id string,
  ref_id string,
  ref_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_ms_paper_ref
select
 paper_id as acheivement_id
,ref_id
,ref_name
,if(true,'csai',null) as source
from csai_basic.ms_paper_ref;

--微软学术论文的引用关系数据

create table  IF NOT EXISTS   o_ms_paper_cite(
  achievement_id string,
  cite_id string,
  cite_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_ms_paper_cite
select
 paper_id as acheivement_id
,cite_id
,cite_name
,if(true,'csai',null) as source
from csai_basic.ms_paper_city;

--微软学术论文的相关联关系数据

create table  IF NOT EXISTS   o_ms_paper_relationship(
  achievement_id string,
  rela_id string,
  rela_name string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_ms_paper_relationship
select
 paper_id as acheivement_id
,rela_id
,rela_name
,if(true,'csai',null) as source
from csai_basic.ms_paper_relationship;

--微软论文主体表

create table  IF NOT EXISTS   o_ms_paper(
  id string,
  achievement_id string,
  paper_type string,
  english_name string,
  paper_rank string,
  first_author string,
  
  authors string,
  fund_project string,
  first_author_id string,
  publish_date string,
  keywords string,
  
  
  includes string,
  references string,
  journal string,
  language string,
  volume string,
  
  issue string,
  fulltext_url string,
  abstract string,
  citation string,
  field_name string,
  
  
  field_sub_name string,
  field_id string,
  field_sub_id string,
  journal_id string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_ms_paper
select
 id
,paper_id as achievement_id
,paper_type
,english_name
,paper_rank
,first_author
,authors

,fund_project
,first_author_id
,publish_date
,keywords
,includes

,references
,journal
,language
,volume
,issue

,fulltext_url
,abstract
,citation
,field_name
,field_sub_name

,field_id
,field_sub_id
,journal_id
,if(true,'csai',null) as source
from csai_basic.ms_paper;



--微软论文对应的期刊

create table  IF NOT EXISTS   o_ms_paper_journal(
  achievement_id string,
  journal_id string,
  paper_name string,
  journal_name string,
  volume string,
  issue string,
  publish_year string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_ms_paper_journal
select
 paper_id as achievement_id
,journal_id
,paper_name
,journal_name
,volume
,issue
,publish_year
,if(true,'csai',null) as source
from csai_basic.ms_paper_journal;


--微软论文对应的topic

create table  IF NOT EXISTS   o_ms_paper_topic(
  id string,
  paper_id string,
  `level` string,
  tag string,
  tag_id string,
  parent_id string,
  source string
)
 row format delimited fields terminated by '☔' stored as orc;
 
insert overwrite table o_ms_paper_topic
select
 id
,paper_id as achievement_id
,`level`
,tag
,tag_id
,parent_id
,if(true,'csai',null) as source
from csai_basic.ms_paper_topic;


CREATE TABLE ods.`o_const_conference`(
  `conference_id` string,  
  `conference` string, 
  `conference_type` string,
  
  `conference_address` string,  
  `organization` string, 
  `start_date` string,
  
  `end_date` string,  
  `country` string, 
  `city` string
  )
row format delimited fields terminated by '☔' stored as orc;

  