CREATE TABLE social_media.youtube(
	video_id text,
	trending_date timestamp,
	title text,
	channel_title text,
	category_id int,
	publish_time timestamp,
	tags text,
	views int,
	likes int,
	dislikes int,
	comment_count int,
	thumbnail_link text,
	comments_disabled boolean,
	ratings_disabled boolean,
	video_error_or_removed boolean,
	description text,
	PRIMARY KEY(video_id)
);
