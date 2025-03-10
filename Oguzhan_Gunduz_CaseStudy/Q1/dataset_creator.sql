CREATE OR REPLACE TABLE `sevenapps-case-study-452916.sevenapps_case.user_events` AS
WITH users_base AS (
    SELECT 
        user_seq,
        TO_HEX(MD5(CAST(user_seq AS STRING))) AS user_id,
        MOD(ABS(FARM_FINGERPRINT(CAST(user_seq AS STRING))), 30) AS random_days,
        MOD(ABS(FARM_FINGERPRINT(CAST(user_seq AS STRING))), 1000) / 1000.0 AS random_val,
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), 
        INTERVAL MOD(ABS(FARM_FINGERPRINT(CAST(user_seq AS STRING))), 30) DAY) AS first_event_time
    FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS user_seq
),

users AS (
    SELECT 
        user_id,
        CASE WHEN random_val < 0.63 THEN 'android' ELSE 'ios' END AS platform,
        first_event_time,
        random_val,
        random_days
    FROM users_base
),

users_with_device AS (
    SELECT 
        user_id,
        platform,
        CASE 
            WHEN platform = 'ios' THEN 
                CASE WHEN random_val < 0.2 THEN 'iPad' ELSE 'iPhone' END 
            ELSE 
                CASE 
                    WHEN random_val < 0.4 THEN 'Samsung' 
                    WHEN random_val < 0.7 THEN 'Pixel' 
                    WHEN random_val < 0.85 THEN 'Xiaomi' 
                    ELSE 'Huawei' 
                END 
        END AS device_type,
        first_event_time,
        random_val,
        random_days
    FROM users
),

user_funnel AS (
    SELECT 
        user_id,
        platform,
        device_type,
        first_event_time,
        TRUE AS will_pageview,  
        MOD(ABS(FARM_FINGERPRINT(CONCAT(user_id, '1'))), 100) < 70 AS will_download,  
        MOD(ABS(FARM_FINGERPRINT(CONCAT(user_id, '2'))), 100) < 80 AS will_install_if_download,  
        MOD(ABS(FARM_FINGERPRINT(CONCAT(user_id, '3'))), 100) < 75 AS will_signup_if_install,  
        MOD(ABS(FARM_FINGERPRINT(CONCAT(user_id, '4'))), 100) < 30 AS will_subscription_if_signup  
    FROM users_with_device
),

funnel_progress AS (
    SELECT
        user_id,
        platform, 
        device_type,
        first_event_time,
        will_pageview,
        will_download,
        (will_download AND will_install_if_download) AS will_install,
        (will_download AND will_install_if_download AND will_signup_if_install) AS will_signup,
        (will_download AND will_install_if_download AND will_signup_if_install AND will_subscription_if_signup) AS will_subscription
    FROM user_funnel
),

event_times AS (
    SELECT
        fp.user_id,
        fp.platform,
        fp.device_type,
        fp.first_event_time AS pageview_time,

        TIMESTAMP_ADD(fp.first_event_time, 
            INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'download_time'))), 71) AS INT64) HOUR
        ) AS download_time,
        
        TIMESTAMP_ADD(
            TIMESTAMP_ADD(fp.first_event_time, 
                INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'download_time'))), 71) AS INT64) HOUR
            ),
            INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'install_time'))), 47) AS INT64) HOUR
        ) AS install_time,
        
        TIMESTAMP_ADD(
            TIMESTAMP_ADD(
                TIMESTAMP_ADD(fp.first_event_time, 
                    INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'download_time'))), 71) AS INT64) HOUR
                ),
                INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'install_time'))), 47) AS INT64) HOUR
            ),
            INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'signup_time'))), 23) AS INT64) HOUR
        ) AS signup_time,
        
        TIMESTAMP_ADD(
            TIMESTAMP_ADD(
                TIMESTAMP_ADD(
                    TIMESTAMP_ADD(fp.first_event_time, 
                        INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'download_time'))), 71) AS INT64) HOUR
                    ),
                    INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'install_time'))), 47) AS INT64) HOUR
                ),
                INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'signup_time'))), 23) AS INT64) HOUR
            ),
            INTERVAL CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(fp.user_id, 'subscription_time'))), 47) AS INT64) HOUR
        ) AS subscription_time
    FROM funnel_progress fp
),

events AS (
    SELECT 
        TO_HEX(MD5(CONCAT(et.user_id, 'pageview'))) AS event_id,
        et.user_id,
        'PageView' AS event_name,
        et.pageview_time AS event_time,
        et.platform,
        et.device_type
    FROM event_times et
    JOIN funnel_progress fp ON et.user_id = fp.user_id
    WHERE fp.will_pageview

    UNION ALL

    SELECT 
        TO_HEX(MD5(CONCAT(et.user_id, 'download'))) AS event_id,
        et.user_id,
        'Download',
        et.download_time AS event_time,
        et.platform,
        et.device_type
    FROM event_times et
    JOIN funnel_progress fp ON et.user_id = fp.user_id
    WHERE fp.will_download

    UNION ALL

    SELECT 
        TO_HEX(MD5(CONCAT(et.user_id, 'install'))) AS event_id,
        et.user_id,
        'Install',
        et.install_time AS event_time,
        et.platform,
        et.device_type
    FROM event_times et
    JOIN funnel_progress fp ON et.user_id = fp.user_id
    WHERE fp.will_install

    UNION ALL

    SELECT 
        TO_HEX(MD5(CONCAT(et.user_id, 'signup'))) AS event_id,
        et.user_id,
        'SignUp',
        et.signup_time AS event_time,
        et.platform,
        et.device_type
    FROM event_times et
    JOIN funnel_progress fp ON et.user_id = fp.user_id
    WHERE fp.will_signup

    UNION ALL

    SELECT 
        TO_HEX(MD5(CONCAT(et.user_id, 'subscription'))) AS event_id,
        et.user_id,
        'Subscription',
        et.subscription_time AS event_time,
        et.platform,
        et.device_type
    FROM event_times et
    JOIN funnel_progress fp ON et.user_id = fp.user_id
    WHERE fp.will_subscription
)
SELECT * FROM events 
ORDER BY user_id, event_time