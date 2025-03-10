WITH user_events_with_next AS (
    SELECT 
        user_id,
        platform,
        event_type,
        event_time,
        LEAD(event_type) OVER (PARTITION BY user_id ORDER BY event_time) AS next_event_type,
        LEAD(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS next_event_time
    FROM `sevenapps-case-study-452916.sevenapps_case.user_events`
    ORDER BY user_id, event_time
),

funnel_counts AS (
    SELECT 
        platform,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT CASE WHEN event_type = 'PageView' THEN user_id END) AS pageview_count,
        
        COUNT(DISTINCT CASE 
            WHEN event_type = 'PageView' 
            AND next_event_type = 'Download' 
            AND TIMESTAMP_DIFF(next_event_time, event_time, HOUR) <= 72 
            THEN user_id 
        END) AS pageview_to_download_count,
        
        COUNT(DISTINCT CASE 
            WHEN event_type = 'Download' 
            AND next_event_type = 'Install' 
            AND TIMESTAMP_DIFF(next_event_time, event_time, HOUR) <= 72 
            THEN user_id 
        END) AS download_to_install_count,
        
        COUNT(DISTINCT CASE 
            WHEN event_type = 'Install' 
            AND next_event_type = 'SignUp' 
            AND TIMESTAMP_DIFF(next_event_time, event_time, HOUR) <= 72 
            THEN user_id 
        END) AS install_to_signup_count,
        
        COUNT(DISTINCT CASE 
            WHEN event_type = 'SignUp' 
            AND next_event_type = 'Subscription' 
            AND TIMESTAMP_DIFF(next_event_time, event_time, HOUR) <= 72 
            THEN user_id 
        END) AS signup_to_subscription_count
    FROM user_events_with_next
    GROUP BY platform
),

funnel_rates AS (
    SELECT 
        platform,
        unique_users,
        pageview_count,
        
        pageview_to_download_count,
        download_to_install_count,
        install_to_signup_count,
        signup_to_subscription_count,

        ROUND(SAFE_DIVIDE(pageview_to_download_count, pageview_count) * 100, 2) AS pageview_to_download_rate,
        ROUND(SAFE_DIVIDE(download_to_install_count, pageview_to_download_count) * 100, 2) AS download_to_install_rate,
        ROUND(SAFE_DIVIDE(install_to_signup_count, download_to_install_count) * 100, 2) AS install_to_signup_rate,
        ROUND(SAFE_DIVIDE(signup_to_subscription_count, install_to_signup_count) * 100, 2) AS signup_to_subscription_rate,

        ROUND(SAFE_DIVIDE(download_to_install_count, pageview_count) * 100, 2) AS pageview_to_install_rate,
        ROUND(SAFE_DIVIDE(install_to_signup_count, pageview_count) * 100, 2) AS pageview_to_signup_rate,
        ROUND(SAFE_DIVIDE(signup_to_subscription_count, pageview_count) * 100, 2) AS pageview_to_subscription_rate
    FROM funnel_counts
)

SELECT * FROM funnel_rates
ORDER BY platform;
