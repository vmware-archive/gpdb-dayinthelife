\timing on
select af.feed_id, sum(sentiment_positive) total_sentiment_positive, sum(sentiment_negative) total_sentiment_negative, sum(sentiment_neutral) total_sentiment_neutral
from author_fact af
inner join bmg_author_dim ad on (af.author_id=ad.id)
inner join feed_dim f on (af.feed_id = f.id)
where af.feed_id = 53
group by 1;
