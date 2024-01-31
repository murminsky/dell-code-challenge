
-- EXERCISE 3
CREATE OR REPLACE VIEW public.v_countries_cases_rate_14_day
AS SELECT c.index,
    c.country,
    c.region,
    c.population,
    c.area_sq_mi,
    c.pop_density_per_sq_mi,
    c.coastline_coast_slash_area_ratio,
    c.net_migration,
    c.infant_mortality_per_1000_births,
    c."gdp_$_per_capita",
    c.literacy_percent,
    c.phones_per_1000,
    c.arable_percent,
    c.crops_percent,
    c.other_percent,
    c.climate,
    c.birthrate,
    c.deathrate,
    c.agriculture,
    c.industry,
    c.service,
    ecdc_date.rate_14_day,
    ecdc_date.latest_start_week_date
   FROM countries c
     JOIN ( SELECT ecdc.country,
            ecdc.rate_14_day,
            subq.latest_start_week_date
           FROM ecdc_covid19_weekly ecdc
             JOIN ( SELECT ecdc_covid19_weekly.country,
                    max(to_date(concat("left"(ecdc_covid19_weekly.year_week, 4), "right"(ecdc_covid19_weekly.year_week, 2)), 'IYYYIW'::text)) AS latest_start_week_date
                   FROM ecdc_covid19_weekly
                  GROUP BY ecdc_covid19_weekly.country) subq ON subq.country = ecdc.country AND subq.latest_start_week_date = to_date(concat("left"(ecdc.year_week, 4), "right"(ecdc.year_week, 2)), 'IYYYIW'::text)
          WHERE ecdc.indicator = 'cases'::text) ecdc_date ON ecdc_date.country = c.country;



-- EXERCISE 4
         
-- query 1 What is the country with the highest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?
         
-- updated to the week 31 of 2020 that started 2020-07-27
select country from ecdc_covid19_weekly ecd 
	where TO_DATE(CONCAT(left(year_week, 4), right(year_week, 2)), 'IYYYIW') = '2020-07-27' 
	and weekly_count/population*100000 = (
		select max(weekly_count/population*100000) from ecdc_covid19_weekly ecd where TO_DATE(CONCAT(left(year_week, 4), right(year_week, 2)), 'IYYYIW') = '2020-07-27' 
	);
	
with prefiltered_country as (
	select country, weekly_count, population, year_week from ecdc_covid19_weekly ecd 
	 	where TO_DATE(CONCAT(left(year_week, 4), right(year_week, 2)), 'IYYYIW') = '2020-07-27' 
)
select country from prefiltered_country ecd 
	where weekly_count/population*100000 = (
		select max(weekly_count/population*100000) from  prefiltered_country ecd 
			where TO_DATE(CONCAT(left(year_week, 4), right(year_week, 2)), 'IYYYIW') = '2020-07-27' 
	);
	
-- query 2 What is the top 10 countries with the lowest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?
	
-- updated to the week 31 of 2020 that started 2020-07-27
explain analyze select country from ecdc_covid19_weekly ecd 
	where TO_DATE(CONCAT(left(year_week, 4), right(year_week, 2)), 'IYYYIW') = '2020-07-27' 
	and indicator = 'cases'
	order by weekly_count/population*100000 limit 10;

-- query 3 What is the top 10 countries with the highest number of cases among the top 20 richest countries (by GDP per capita)?

-- ecdc source contains only 9 countries from 20 richest countries in the world, data enrichment required
select country, max(cumulative_count)  from ecdc_covid19_weekly ecw 
	where "indicator" ='cases' 
	and cumulative_count is not null 
	and country in (
		select country from countries c where gdp_$_per_capita is not null order by gdp_$_per_capita  desc limit 20
	)
	group by country order by max(cumulative_count) desc limit 10;

-- query 3 using who_covid19_daily dataset from enrichment

select country, max(cumulative_cases)  from who_covid19_daily  
	where cumulative_cases is not null 
	and country in (
		select country from countries c where gdp_$_per_capita is not null order by gdp_$_per_capita  desc limit 20
	)
	group by country order by max(cumulative_cases) desc limit 10;


--query 4 List all the regions with the number of cases per million of inhabitants and display information on population density, for 31/07/2020.

-- updated to the week 31 of 2020 that started 2020-07-27
select ecdc.country, ecdc.weekly_count/ecdc.population*1000000 as cases_per_milion_inhab , c.pop_density_per_sq_mi from countries c
	join (
			select * from ecdc_covid19_weekly ecdc where indicator = 'cases' and  TO_DATE(CONCAT(left(year_week, 4), right(year_week, 2)), 'IYYYIW') = '2020-07-27' 
		) ecdc
		on ecdc.country = c.country;

--query 5 Query the data to find duplicated records.
	
select country, count(*) from countries group by country having count(*) >1;

select country, indicator, year_week, count(*) from ecdc_covid19_weekly ecw  group by country, indicator , year_week  having count(*) >1;

select countriesandterritories , daterep, count(*) from ecdc_covid19_daily ecd  group by countriesandterritories, daterep  having count(*) >1;

--query 6 Analyze the performance of all the queries and describes what you see. Get improvements suggestions.

/* Suggestions:
 *  string operations and casting to date is overhead for the query and normalization of tables would improve query performance,
 * and using CTE with only required columns and filtered records reduced execution time - kept as a second example in query 1
 * sorting large datasets is costly operation, in query 3 I would suggest getting gdp numeric value and use it for filter
 * creating an index for most queried columns (country, dates) 
*/
