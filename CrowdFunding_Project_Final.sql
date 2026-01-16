create database crowdfunding_project;
use crowdfunding_project;

SET GLOBAL local_infile = 1;
show variables like 'local_infile';

-- Location Table
create table location (
    location_id int,
    displayable_name text,
    location_type text,
    location_name text,
    outcome text,
    short_name text,
    is_root text,
    country text,
    localized_name text
);
select * from location;

load data local infile "D:/Data Analysis/Croudfunding Datasets/Dataset & KPI/CrowdFunding/New folder/Crowdfunding_Location.csv"
into table location
character set latin1
fields terminated by ',' 
optionally enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows
(location_id,
 displayable_name,
 location_type,
 location_name,
 outcome,
 short_name,
 is_root,
 country,
 localized_name);

-- Category Table
create table category
(category_id int,
category_name varchar(255), 
parent_id text default null, 
position_no text default NULL); 

load data local infile "D:/Data Analysis/Croudfunding Datasets/Dataset & KPI/CrowdFunding/New folder/Crowdfunding_Category.csv"
into table category
character set latin1
fields terminated by ',' 
optionally enclosed by '"'
lines terminated by '\r\n'
IGNORE 1 ROWS
(category_id,
category_name,
parent_id,
position_no);

select * from category;

-- Creator Table
create table creators 
(creator_id int default null, 
 creator_name text default null, 
 choosen_currency varchar(20) default null);

select * from creators;
describe creators;

load data local infile "D:/Data Analysis/Croudfunding Datasets/Dataset & KPI/CrowdFunding/New folder/Crowdfunding_Creator.csv"
into table creators
character set latin1
fields terminated by ',' 
optionally enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows
(creator_id, creator_name, choosen_currency);

-- ---------------------------------------------------------------------------------------------------------------------------------
-- Q1. Convert Epoch time in Natural Time
create table if not exists KPI1_project_dates as
select name as project_name,
  FROM_UNIXTIME(created_at) as created_date,
  FROM_UNIXTIME(deadline) as deadline_date
from projects;

select * from KPI1_project_dates;
-- ---------------------------------------------------------------------------------------------------------------------------------
-- q2. build calendar
create table if not exists kpi2_calendar_table as
select
  from_unixtime(created_at) as date,
  year(from_unixtime(created_at)) as year,
  month(from_unixtime(created_at)) as month_no,
  monthname(from_unixtime(created_at)) as month_full_name,
  concat('q', quarter(from_unixtime(created_at))) as quarter,
  date_format(from_unixtime(created_at), '%Y-%b') as yearmonth,
  dayofweek(from_unixtime(created_at)) as weekday_no,
  dayname(from_unixtime(created_at)) as weekday_name,
  case
    when month(from_unixtime(created_at)) >= 4 then month(from_unixtime(created_at)) - 3
    else month(from_unixtime(created_at)) + 9
  end as financial_month,
  concat('fq-', quarter(date_add(from_unixtime(created_at), interval 3 month))) as financial_quarter
from projects;

select * from kpi2_calendar_table;
-- ---------------------------------------------------------------------------------------------------------------------------------
-- Q4. Convert the Goal amount into USD using the static USD rate.
create table if not exists KPI4_goal_usd as
select
  name as project_name, goal, static_usd_rate,
  ROUND(goal * static_usd_rate, 2) as goal_usd
from projects;

select * from KPI4_goal_usd;
-- ---------------------------------------------------------------------------------------------------------------------------------
/* Q5. PROJECT OVERVIEW KPI: */
-- Q5.A Total Number of Projects based on outcome.
create table if not exists KPI5A_Project_No_outcome as
select state, COUNT(*) as total_projects
from projects
group by state;

select * from KPI5A_Project_No_outcome;
-- ----------------------------------------------------
-- Q5.B Total Number of Projects based on Locations.

create table if not exists KPI5B_Project_No_Location as
select 
    l.Displayable_Name, count(*) AS total_projects
from projects p
join location l on p.location_id = l.location_id
group by  l.Displayable_Name
order by total_projects DESC;

SELECT * FROM KPI5B_Project_No_Location;
-- ----------------------------------------------------
-- Q5.C Total Number of Projects based on Catogory.
drop table kpi5_project_no_category;
create table if not exists kpi5C_project_no_category as
select 
    c.category_name, count(*) as total_projects
from projects p
join category c on p.category_id = c.category_id
group by c.category_name
order by total_projects desc;

select * from kpi5C_project_no_category;
-- ----------------------------------------------------
-- Q5.D Total Number of Projects based on Year, Month, Quarter.
create table if not exists kpi5D_noproject_by_time as
select
    year(from_unixtime(created_at)) as year,
    concat('q', quarter(from_unixtime(created_at))) as quarter,
    month(from_unixtime(created_at)) as month_no,
    monthname(from_unixtime(created_at)) as month,
    count(*) as total_projects
from projects
group by year, quarter, month_no, month
order by year, quarter, month_no;

select * from KPI5D_NoProject_by_time;
-- ---------------------------------------------------------------------------------------------------------------------------------
/* Q6. SUCCESSFUL PROJECTS*/
create table if not exists KPI6_successful_projects as
select
  SUM(goal * static_usd_rate) as total_amount_raised,
  SUM(backers_count) as total_backers,
  ROUND(AVG(DATEDIFF(FROM_UNIXTIME(deadline), FROM_UNIXTIME(created_at))), 2) as avg_days
from projects
where state = 'successful';
select * from KPI6_successful_projects;
-- ---------------------------------------------------------------------------------------------------------------------------------
/* Q7. TOP SUCCESSFUL PROJECTS*/
-- Q7.A Based on No. of Backers
create table if not exists KPI7A_topsuccessful_by_backers as
select name as project_name, backers_count
from projects
where state = 'successful'
order by backers_count desc
limit 10;

select * from KPI7A_topsuccessful_by_backers;
-- -----------------------------------------------------------------
-- Q7.B Based on Amount raised
drop table kpi8_success_perct_category;
create table if not exists KPI7B_topsuccessful_by_amtraised as
select 
	name as project_name, 
    round(goal * static_usd_rate,2) as amount_raised
from projects
where state = 'successful'
order by amount_raised desc
limit 10;

select * from KPI7B_topsuccessful_by_amtraised;
-- ---------------------------------------------------------------------------------------------------------------------------------
/* Q8. SUCCESSFUL PROJECTS PERCENTAGE*/
-- Q8.A Percentage of successful projects overall
create table if not exists KPI8A_Success_perct_ova as
select
  round(SUM(CASE WHEN state = 'successful' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_percentage
from projects;

select * from KPI8A_Success_perct_ova;
-- --------------------------------------------------------------------------
-- Q8.B Percentage of Successful projects by Catrgory
create table if not exists kpi8B_success_perct_category as
select 
    n.category_name,
    n.project_count,
    round(n.project_count * 100.0 / (select count(*) from projects where state = 'successful'), 2) as percentage
from (
    select c.category_name, count(p.projectid) as project_count
    from category c left join projects p on c.category_id = p.category_id
    where p.state = 'successful'
    group by c.category_name
) as n
order by percentage desc;

select * from kpi8B_success_perct_category;
-- --------------------------------------------------------------------------
-- Q8.C Percentage of successful projects by Year, Month
drop table kpi8_success_perct_ym;
create table if not exists kpi8C_success_perct_ym as
select year, month, project_count,
    round(project_count * 100.0 / (select count(*) from projects where state = 'successful'), 2) as percentage
from (
    select 
        count(projectid) as project_count,
        year(from_unixtime(created_at)) as year,
        monthname(from_unixtime(created_at)) as month,
        month(from_unixtime(created_at)) as month_number
    from projects
    where state = 'successful'
    group by year, month, month_number
) as timewise_success_summary
order by year, month_number;

select * from kpi8C_success_perct_ym;
-- --------------------------------------------------------------------------
-- Q8.D Percentage of Successful projects by Goal Range

create table if not exists KPI8D_Success_goal_range as
select outcome, total_projects, goal_range, 
    round(total_projects * 100.0 / (select count(*) from projects where state = 'successful'), 2) as success_percentage
from (
    select outcome, count(*) as total_projects, goal_range
    from (
        select state as outcome,
            case 
                when usd_goal <= 1000 then '0-1000'
                when usd_goal > 1000 and usd_goal <= 5000 then '1001-5000'
                when usd_goal > 5000 and usd_goal <= 10000 then '5001-10000'
                when usd_goal > 10000 and usd_goal <= 20000 then '10001-20000'
                else '20000+'
            end as goal_range
        from (
            select state, goal, static_usd_rate, goal * static_usd_rate as usd_goal
            from projects where state = 'successful'
        ) as successful_goal
    ) as project_category
    group by outcome, goal_range
) as data_summarized
order by goal_Range;

select * from KPI8D_Success_goal_range;
-- -----------------------------------------------------------------------------------------------
-- Table execution queries compiled 
select * from KPI1_project_dates;
select * from kpi2_calendar_table;
select * from KPI4_goal_usd;
select * from KPI5A_Project_No_outcome;
SELECT * FROM KPI5B_Project_No_Location;
select * from kpi5C_project_no_category;
select * from KPI5D_NoProject_by_time;
select * from KPI6_successful_projects;
select * from KPI7A_topsuccessful_by_backers;
select * from KPI7B_topsuccessful_by_amtraised;
select * from KPI8A_Success_perct_ova;
select * from kpi8B_success_perct_category;
select * from kpi8C_success_perct_ym;
select * from KPI8D_Success_goal_range;