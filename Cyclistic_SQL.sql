
/*using the database */
use capstone ;


/*creating the table for the analysis */
create table cyclistic (
ride_id varchar (65) ,
rideable_type ENUM('classic_bike','electric_bike','docked_bike'),
started_at DATETIME, 
ended_at DATETIME, 
start_station_id varchar(65), 
end_station_id varchar(65),  
member_casual ENUM('member','casual')
) ;


/* uploading the data files */
SET GLOBAL local_infile=1 ;

LOAD DATA LOCAL INFILE /*path*/  INTO TABLE cyclistic 
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'   IGNORE 1 LINES
(@col1,@col2,@col3,@col4,@col5,@col6,@col7,@col8,@col9,@col10,@col11,@col12,@col13,@col14) 
set ride_id=@col1,rideable_type=@col2,started_at=@col3,ended_at=@col4,start_station_id=@col6,end_station_id=@col8,member_casual=@col13  ;


/* creating a duplication of the table for backup */
create table cyclistic2 (
ride_id varchar (65) ,
rideable_type ENUM('classic_bike','electric_bike','docked_bike'),
started_at DATETIME, 
ended_at DATETIME, 
start_station_id varchar(65), 
end_station_id varchar(65),  
member_casual ENUM('member','casual')
) ;

/*populating the duplicate table with the data from the original table */
insert into cyclistic2(ride_id ,rideable_type,started_at,ended_at,start_station_id,end_station_id,member_casual) 
select ride_id,rideable_type,started_at,ended_at,start_station_id,end_station_id,member_casual
from cyclistic;



/* creating table for station names */
create table station_names (
start_station_id varchar(65) ,
start_station_name varchar (65) ,
end_station_id varchar (65) ,
end_station_name varchar (65) ) ;

/*populating the station names table  */
LOAD DATA LOCAL INFILE /*path*/   INTO TABLE station_names 
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'   IGNORE 1 LINES
(@col1,@col2,@col3,@col4,@col5,@col6,@col7,@col8,@col9,@col10,@col11,@col12,@col13,@col14) 
set start_station_id=@col6,start_station_name=@col5,end_station_id=@col8,end_station_name=@col7 ;

/* exporting the combined file */
SELECT *
FROM cyclistic
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/unified_24_months_cyclistic.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

/* glimpse at the data*/
select * from cyclistic limit 5;

/*check for duplications */
select count(ride_id) as 'count_ID', 
count(distinct(ride_id)) as 'count_distinct_ID' ,
(count(ride_id))-(count(distinct(ride_id))) as 'duplicates'  from cyclistic  ;


/*selecting the duplication for inspection */
with cte_dup AS (
select * , count(*) over (partition by ride_id) as 'count' from cyclistic)

select * from cte_dup where count>1 ;

/*selecting the duplication for inspection 2 */
with cte_dup AS (
select * , count(*) over (partition by ride_id) as 'count' from cyclistic)

select * from cte_dup where count>1 AND started_at > ended_at;
    
/* deleting the faulty duplications where start date is after the end date for each ride */
 delete from cyclistic  where ride_id IN (
	select ride_id from (select ride_id , row_number () 
		over(partition by ride_id order by ride_id) as 'duplicates' 
	from cyclistic) as temp_t where duplicates>1 AND started_at > ended_at ) ;


/*check for NULLs or missing values*/
select * from cyclistic where 
ride_id is null 
OR rideable_type is null 
OR started_at is null 
OR ended_at is null 
OR start_station_id is null 
OR end_station_id is null 
OR member_casual is null;

select * from cyclistic where ride_id=""  OR rideable_type= ""  OR member_casual="" ;

select count(*) from cyclistic where start_station_id="";
select count(*) from cyclistic where end_station_id="";
select count(*) from cyclistic where start_station_id="" AND end_station_id="" ;

/* deleting observations where start_station_id OR end_station_id is "" */
delete from cyclistic where start_station_id="" OR end_station_id="" ;


/* checking for distinct values */
select distinct(rideable_type),count(rideable_type) from cyclistic ;

select count(rideable_type),rideable_type from cyclistic group by rideable_type;

select distinct (member_casual) from cyclistic ;

/* period for study*/
select max(started_at) as period_start , 
min(started_at) as period_end , 
datediff(max(started_at),min(started_at)) as period_in_days 
from cyclistic ;




/*day of week from dates and percentage*/
SELECT dayofweek(ma.started_at) AS 'day of week ' 
     , COUNT(1) AS total
     , COUNT(1) / t.cnt * 100 AS `percentage`
  FROM cyclistic ma
  JOIN (SELECT COUNT(1) AS cnt FROM cyclistic) t
 GROUP
    BY dayofweek(ma.started_at)
     , t.cnt  
     order by 1;
     
     
/*by user */
SELECT member_casual 
	 , dayofweek(started_at) AS most_active_day_of_week  
     , count(1) AS total
  FROM cyclistic
 GROUP
    BY dayofweek(started_at)
     , member_casual 
     order by 1,3 desc ;
     
     
/* most active day by user type */     
with cte AS (
SELECT member_casual 
	 , dayofweek(started_at) AS most_active_day_of_week  
     , count(1) AS total
  FROM cyclistic 
 GROUP
    BY dayofweek(started_at)
     , member_casual 
     order by 1,3 desc)
     
     select member_casual ,most_active_day_of_week, max(total) as 'count users'  from cte group by 1 ;

/* least active day by user type */  
with cte AS (
SELECT member_casual 
	 , dayofweek(started_at) AS least_active_day_of_week  
     , COUNT(1) AS total
  FROM cyclistic 
 GROUP
    BY dayofweek(started_at)
     , member_casual 
     order by 1,3 asc)
     
     select member_casual ,least_active_day_of_week, min(total) as 'count users'  from cte group by 1 ;
     
/* months activity */
select date_format(started_at , '%Y-%m') , member_casual , count(extract(month from started_at)) from cyclistic group by 1,2 order by 2,1;


/* median ride duration per group */
select count(member_casual) from cyclistic where member_casual = 'member' ; /* 4540323 */
select count(member_casual) from cyclistic where member_casual = 'casual' ; /* 3395554 */

with cte AS (
select member_casual ,TIMEDIFF(ended_at ,started_at) as "duration" ,row_number() over(partition by member_casual order by "duration" ) as "ranks"  from cyclistic)

select member_casual ,duration,ranks from cte where member_casual = "member" and ranks = ceil(4540323/2)
UNION
select member_casual ,duration,ranks from cte where member_casual = "casual" and ranks = ceil(3395554/2);

/* number of casuals vs. members */
select member_casual , count(member_casual) as "sum"   from cyclistic group by 1 ;

/* favorite bike type per group */
select member_casual , 
rideable_type ,
count(rideable_type) as "count",
row_number()over(partition by member_casual order by count(rideable_type) desc) as "ranking" 
from cyclistic group by 1,2 ; 

/*most active hours */
select member_casual , 
hour(started_at) ,
count(extract(hour from started_at)) 
from cyclistic group by 1,2 order by 1,3 desc;

/*selecting from station_names */
create view top_start_stations AS
select start_station_id, count(start_station_id) as "summ" from cyclistic where member_casual = 'casual' group by 1 order by 2 desc limit 10 ;

/*creating views for top stations and then populating tables with the results, so
later i can use join to join with the station names table */
create view top_end_stations AS
select end_station_id, count(end_station_id) as "summ" from cyclistic where member_casual = 'casual' group by 1 order by 2 desc limit 10 ;

select a.* ,b.start_station_name from top_start_stations a join station_names b on a.start_station_id = b.start_station_id;

create table casual_start_stations (
start_station_id varchar(65) ,
summ int ) ;

insert into casual_start_stations (start_station_id , summ ) select * from top_start_stations a;

create table casual_end_stations (
end_station_id varchar(65) ,
summ int ) ;

insert into casual_end_stations (end_station_id , summ ) select * from top_end_stations a;

/* joining end and start stations */

select a.end_station_id , a.summ , b.end_station_name from casual_end_stations a
left join station_names b
on a.end_station_id = b.end_station_id group by 1 order by 2 desc;

select a.start_station_id , a.summ , b.start_station_name from casual_start_stations a
left join station_names b
on a.start_station_id = b.start_station_id group by 1 order by 2 desc;