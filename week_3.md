Project submission for Week 3 of Advanced SQL
Jai Paton

```sql

/**
1. Total Unique Sessions
2. Average Length of sessions in Seconds
3. Average number of searches completed before displaying a recipe
4. ID of the Recipe that was most viewed
*/

/** BEGIN QUERY */

with

-- -- Parsed date list from available data
-- -- -- May encounter data gaps
date_list as (
	select
    	to_date(event_timestamp) as event_date
    from vk_data.events.website_activity

    group by 1

    order by event_date asc
),

-- -- Parsed table of event data
-- -- -- Extracts JSON data like recipe IDs and event types
parsed_events as (
    select
    	event_id,
        session_id,
        event_timestamp,
        to_date(event_timestamp) as event_date,
        event_details,
        coalesce(trim(parse_json(event_details):"recipe_id",'"'),'') as recipe_id,
        coalesce(trim(parse_json(event_details):"event", '"'),'') as event_type
    from vk_data.events.website_activity
    group by 1,2,3,4,5
),
-- Question 1
-- -- Count of unique sessions
unique_sessions as (
	select
        event_date,
        count(distinct session_id) as session_count
    from parsed_events
    group by 1

),
-- Question 2
-- -- Step 1/2 - Identify the max & min timestamp values for each session

session_duration_parse as (
	select 
    	max(event_timestamp) over(partition by session_id order by event_timestamp asc) as first_session_timestamp,
        min(event_timestamp) over(partition by session_id order by event_timestamp asc) as last_session_timestamp,
        session_id,
        event_date
    from parsed_events

),

-- -- Step 2/2 - Calculate the average session duration for each day
session_duration_calc as (
	select 
    	event_date,
    	avg(session_duration) as avg_session_duration
    from(
        select
        	event_date,
        	timestampdiff(second,last_session_timestamp,first_session_timestamp ) as session_duration
        from session_duration_parse
        )
    group by 1
),

-- Question 3
-- Isolate view_recipe events from parsed events
-- -- ID the first time a recipe is viewed within a session

first_session_display_recipe as (

	select
        session_id,
        min(event_timestamp) over(partition by session_id order by event_timestamp asc) as first_recipe_view_ts
    from
    	parsed_events
    where
    	event_type = 'view_recipe'

),
-- Isolate all search events before the first view_recipe event per session and find the average count of search events per day
pre_recipe_search_events as (
	select
   		event_date,
    	avg(search_event_count) as average_searches_before_recipe
        from(
        	select 
            	parsed_events.session_id,
                parsed_events.event_date as event_date,
                count(event_id) as search_event_count
                
            
            from first_session_display_recipe 
            left join parsed_events 
            	on  first_session_display_recipe.session_id = parsed_events.session_id 
                	and parsed_events.event_timestamp < first_session_display_recipe.first_recipe_view_ts
                    and parsed_events.event_type = 'search'

            group by 1,2
            )
        group by 1
),

-- Question 4
recipe_view_count as (
    select
        event_date,
        recipe_id,
        count(event_id) as recipe_views
        
    from
        parsed_events
    where
        event_type = 'view_recipe'
    group by 1,2
    
),

recipe_view_ranker as (
    select
        event_date,
        recipe_id,
        recipe_views,
        -- max(recipe_views) over(partition by event_date) max_recipe_views
        row_number() over(partition by event_date order by recipe_views desc) as recipe_view_rank
    from recipe_view_count
),

most_viewed_recipes_per_day as (
    select
    	event_date,
        recipe_id,
        recipe_views

    from recipe_view_ranker
    where recipe_view_rank = 1
),

values_to_date_list as (
	select
    	date_list.event_date as event_date,
        unique_sessions.session_count as unique_session_count,
        session_duration_calc.avg_session_duration as avg_session_duration,
        pre_recipe_search_events.average_searches_before_recipe as average_searches_before_recipe,
        most_viewed_recipes_per_day.recipe_id as most_viewed_recipe --,
        -- most_viewed_recipes_per_day.recipe_views as most_viewed_recipe_count
    
    from
    	date_list
        left join unique_sessions on date_list.event_date = unique_sessions.event_date
        left join session_duration_calc on date_list.event_date = session_duration_calc.event_date
        left join pre_recipe_search_events on date_list.event_date = pre_recipe_search_events.event_date
        left join most_viewed_recipes_per_day on date_list.event_date = most_viewed_recipes_per_day.event_date

)
    
select *
from values_to_date_list

```
