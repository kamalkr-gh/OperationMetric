
/* Operation analytics */ 

/*TABLE SCHEMA*/
create table case1 (
id INT AUTO_INCREMENT UNIQUE PRIMARY KEY ,
ds DATE,
job_id INT ,
actor_id INT,
event VARCHAR(10),
languageVARCHAR(20),
time_spent INT,
org CHAR(5)
);
insert into case1(ds,job_id,actor_id,event,language,time_spent,org)
values("2020-11-30",21,1001,"skip","English",15,"A"),
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005, 'transfer', 'Persian', 22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');

/* Number of jobs reviewed: Amount of jobs reviewed over time. */

select ds ,sum(time_spent) as timeInSecondsPerDay,count(job_id) as
jobsReviewedPerDay ,
((3600 / sum(time_spent))*count(job_id)) as NoOfJobsPerHourPerday
from case1
group by ds
order by ds ASC ;

/* Throughput : It is the no. of events happening per second. */ 

select ds ,(count(event)/ sum(time_spent)) as throughput
from case1
group by ds
order by ds ASC ;

select AVG(throughput) as movingaverage
from
( 
    select (count(event)/ sum(time_spent)) as throughput from case1  
) as subquery ;

/* Percentage share of each language: Share of each language for different contents */

select language, ((count(id)/8)*100) as sharePercentage from case1
group by language
order by language ASC ;

/* Duplicate rows: Rows that have the same value present in them */

select actor_id , count(id) as actors from case1
group by actor_id
having actors > 1 ;

/* Investigation metric spike */

/* User Engagement: To measure the activeness of a user. Measuring if the user finds quality in a product/service */

select EXTRACT(WEEK FROM occurred_at)as Weeks,
count(DISTINCT user_id) as Users
from events
group by EXTRACT(WEEK FROM occurred_at)
order by Weeks;

/* User Growth: Amount of users growing over time for a product. */

select year, weeks, active_users,
sum(active_users)
over ( order by year, weeks rows between unbounded preceding and current row ) 
as user_growth
from
(
    select extract(year from u.activated_at) as year,
    extract (week from u.activated_at)as weeks,
    count(distinct user_id) as active_users
    from users u
    where state='active'
    group by year, weeks
    order by year, weeks
)u;

/* Weekly Retention: Users getting retained weekly after signing-up for a product */

WITH cohort AS (
SELECT EXTRACT(WEEK FROM occurred_at) AS CohortWeek,
COUNT(DISTINCT user_id) AS cohort_users
FROM events
WHERE event_name = 'complete_signup'
GROUP BY EXTRACT(WEEK FROM occurred_at)
),
retention AS (
SELECT EXTRACT(WEEK FROM activated_at) AS RetentionWeek,
COUNT(DISTINCT user_id) AS retention_users
FROM users
GROUP BY EXTRACT(WEEK FROM activated_at)
)
SELECT
retention.RetentionWeek as Weekly ,
retention.retention_users,
cohort.cohort_users,
round((cohort.cohort_users :: numeric / retention.retention_users* 100),2)
AS retention_percentage
FROM
retention
JOIN
cohort ON retention.RetentionWeek = cohort.CohortWeek
ORDER BY Weekly ;

/* Weekly Engagement: To measure the activeness of a user */

select extract(WEEK from occurred_at) as WeeklyEngagement, device ,
count(DISTINCT user_id) as EngagedUsers
from events
where event_type = 'engagement'
group by extract(WEEK from occurred_at), device
order by WeeklyEngagement;

/* Email Engagement: Users engaging with the email service */

SELECT
Weekly,
Round((SUM(weekly_digest) / SUM(total) * 100)) AS "Weekly_DigestRate",
Round((SUM(email_opens) / SUM(total) * 100)) AS "Email_OpenRate",
Round((SUM(email_clickthroughs) / SUM(total) * 100)) AS
"Email_ClickthroughRate",
Round((SUM(reengagement_emails) / SUM(total) * 100)) AS
"Reengagement_EmailRate"
FROM
(
SELECT
EXTRACT(WEEK FROM occurred_at) AS Weekly,
COUNT(CASE WHEN action = 'sent_weekly_digest' THEN user_id END) AS
weekly_digest,
COUNT(CASE WHEN action = 'email_open' THEN user_id END) AS
email_opens,
COUNT(CASE WHEN action = 'email_clickthrough' THEN user_id END) AS
email_clickthroughs,
COUNT(CASE WHEN action = 'sent_reengagement_email' THEN user_id
END) AS reengagement_emails,
COUNT(user_id) AS total
FROM email
GROUP BY Weekly
) sub
GROUP BY Weekly
ORDER BY Weekly;
