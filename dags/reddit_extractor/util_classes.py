import datetime

from dataclasses import dataclass
from .util_funcs import convert_timestamp

date_asof = datetime.datetime.strftime(datetime.datetime.now(),"%m-%d-%Y")

@dataclass
class Comment:
    """Data Container for comment attributes"""
    author: str
    created: int
    body: str
    ups: int
    downs: int
    subreddit: str
    submission: str
        
    def __post_init__(self):
        self.body = self.body.replace(',', ' ')
        self.created = convert_timestamp(self.created)


def comment_factory(comment: str):
    """"Helper function for creating comment dataclass"""
    return Comment(
        comment.author.name,
        comment.created,
        comment.body,
        comment.ups,
        comment.downs,
        comment.subreddit.title,
        comment.submission.title
    )


@dataclass
class Submission:
    """Data container for submission attributes."""
    subreddit_url: str
    subreddit_name: str
    title: str
    selftext: str
    author: str
    created: int
    
    over_18: bool
    edited: bool
    is_original_content: bool
    locked: bool
    spoiler: bool
    
    num_comments: int
    num_crossposts: int
    num_duplicates: int
    num_reports: int
    num_upvotes: int
    num_downvotes: int

    scraped_date: str
    
    def __post_init__(self):
        self.title = self.title.replace(',', ' ')
        self.selftext = self.selftext.replace(',', ' ')
        self.created = convert_timestamp(self.created)


def submission_factory(submission_obj, scraped_date):
    """Helper function that returns Submission dataclass"""
    try:
        return  Submission(
        submission_obj.subreddit.url,
        submission_obj.subreddit.url.split('/')[-2],
        submission_obj.title,
        submission_obj.selftext,
        submission_obj.author.name,
        submission_obj.created,
        submission_obj.over_18,
        submission_obj.edited,
        submission_obj.is_original_content,
        submission_obj.locked,
        submission_obj.spoiler,
        submission_obj.num_comments,
        submission_obj.num_crossposts,
        submission_obj.num_duplicates,
        submission_obj.num_reports,
        submission_obj.ups,
        submission_obj.downs,
        scraped_date
        )
        
    except AttributeError as e:
        print('One or more atrributes are missing from Subreddit input')
        print('Skipping...')
        print(e)
        pass
