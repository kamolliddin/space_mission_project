-- SELECT SCHEMA 'sp_missions':
set search_path to sp_missions;


-- IN WHICH MONTH THE LAUNCH HAS HIGHEST POSSIBILITY TO SUCCEED:
CREATE VIEW success_mission_by_month AS
select CASE extract(month from m.mission_date)
           WHEN 1 THEN 'January'
           WHEN 2 THEN 'February'
           WHEN 3 THEN 'March'
           WHEN 4 THEN 'April'
           WHEN 5 THEN 'May'
           WHEN 6 THEN 'June'
           WHEN 7 THEN 'July'
           WHEN 8 THEN 'August'
           WHEN 9 THEN 'September'
           WHEN 10 THEN 'October'
           WHEN 11 THEN 'November'
           WHEN 12 THEN 'December'
       END as month,
       count(mission_id) as total_success_missions,
       sum(count(mission_id)) over () as tatal_missions,
       round(count(mission_id) / sum(count(mission_id)) over () * 100, 2) as percentage
from mission m
where m.status = 'Success'
group by extract(month from m.mission_date)
order by percentage asc;


-- NUMBER OF LAUNCHES BY EVERY COMPANY TOP 10:
CREATE VIEW launches_by_company AS
with cte_launch_data as (
    select company,
           status,
           total_missions_by_status,
           total_missions_by_company,
           dense_rank()
           over (order by total_missions_by_company desc, company groups between unbounded preceding and unbounded following) as rank
    from (
             select c.company,
                    m.status,
                    count(m.mission_id)                                    as total_missions_by_status,
                    sum(count(m.mission_id)) over (partition by c.company) as total_missions_by_company
             from mission m
                      inner join company c on m.company_id = c.company_id
             group by c.company, m.status) t
) select company, status, total_missions_by_status, total_missions_by_company from cte_launch_data where rank <= 10;


-- TOP 5 COMPANIES BY YEARLY INCREASE IN 5 YEAR PERIOD:
CREATE VIEW company_rank_by_increase AS
with cte_active_companies as (
     select distinct c.company_id, c.company
        from company c
        inner join mission m on c.company_id = m.company_id
        inner join rocket r on r.rocket_id = m.rocket_id
        where r.is_rocket_active = true
), cte_yearly_stats as (
select extract(year from m.mission_date) as year,
       ac.company,
       count(m.mission_id) filter ( where m.status = 'Success' ) as succes_missions,
       count(m.mission_id) filter ( where m.status in ('Partial Failure', 'Failure', 'Prelaunch Failure') ) as failed_missions
from  mission m
inner join cte_active_companies ac on ac.company_id = m.company_id
where extract(year from mission_date) between (select extract(year from max(mission_date)) - 5 from mission)
                                              and (select extract(year from max(mission_date)) from mission)
group by extract(year from m.mission_date), ac.company
order by company
), cte_change_stats as (
    select year,
       company,
       succes_missions - failed_missions as positive_mission_count,
       (succes_missions - failed_missions) - first_value(succes_missions - failed_missions) over (partition by company order by year) as change_from_first_year
from cte_yearly_stats
), cte_total_changee as (
    select year,
       company,
       positive_mission_count,
       change_from_first_year,
       sum(change_from_first_year) over (partition by company) as total_increase
    from cte_change_stats
), cte_ranking as (
   select cth.year,
          cth.company,
          cth.positive_mission_count,
          cth.change_from_first_year,
          cth.total_increase,
          dense_rank() over (order by total_increase desc, company groups between unbounded preceding and unbounded following) as rank
    from cte_total_changee cth
) select year,
         company,
         positive_mission_count,
         change_from_first_year,
         total_increase
from cte_ranking
where rank <= 5
and year  between (select extract(year from max(mission_date)) - 4 from mission)
                                              and (select extract(year from max(mission_date)) from mission)
order by company, year;


-- MISSION COST BY COMPANY, LOCATION, YEAR AND ROCKET:
CREATE VIEW company_mission_cost AS
select c.company,
       ml.mission_location,
       extract(year from mission_date) as year,
       r.rocket,
       avg(m.cost) as average_mission_cost
from mission m
inner join company c on c.company_id = m.company_id
inner join rocket r on m.rocket_id = r.rocket_id
inner join mission_location ml on m.mission_location_id = ml.mission_location_id
where company in ('CASC', 'SpaceX')
and m.cost is not null
and r.is_rocket_active = true
and ml.mission_location != 'Yellow Sea'
group by c.company, ml.mission_location,  extract(year from mission_date), r.rocket;