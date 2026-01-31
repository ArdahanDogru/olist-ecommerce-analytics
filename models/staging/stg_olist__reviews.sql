WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'reviews') }}
),

renamed AS (
    SELECT
        -- IDs
        review_id,
        order_id,

        --About the review
        CAST(review_score AS INT64) AS review_score,
        review_comment_title,
        review_comment_message,

        --Date info
        CAST(review_creation_date AS TIMESTAMP) AS created_at,
        CAST(review_answer_timestamp AS TIMESTAMP) AS answered_at

    FROM source
    WHERE CAST(review_score AS INT64) BETWEEN 1 AND 5  -- Filter out invalid score (0)
)

SELECT * FROM renamed