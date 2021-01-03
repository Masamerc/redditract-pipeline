import datetime


def convert_timestamp(ts: str) -> str:
    """Helper function that converts timestamp to %m-%d-%Y format"""
    datetime_obj = datetime.datetime.fromtimestamp(ts)
    date = datetime.datetime.strftime(datetime_obj,"%m-%d-%Y")
    return date


def extract_attributes_from_subreddit(subreddit) -> dict:
    '''Helper function that extracts and stores attributes from Subreddit object'''
    return {
        "active_user_count": subreddit.active_user_count,
        "url": subreddit.url,
        "title": subreddit.title,
        "subscribers": subreddit.subscribers,
        "subreddit_type": subreddit.subreddit_type,
        "spoilers_enabled": subreddit.spoilers_enabled,
        "public_description": subreddit.public_description,
        "over18": subreddit.over18,
        "created": subreddit.created,
        "created_utc": subreddit.created_utc,
        "lang": subreddit.lang,
        "videos_allowed": subreddit.allow_videos,
        "images_allowed": subreddit.allow_images
    }