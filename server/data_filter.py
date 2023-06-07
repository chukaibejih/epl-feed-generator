import logging
from peewee import fn
from server.database import db, Post

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

epl_key_names = [
    'arsenal',
    'aston villa',
    'brentford',
    'brighton',
    'burnley',
    'chelsea',
    'crystal palace',
    'everton',
    'leeds united',
    'leicester city',
    'liverpool',
    'manchester city',
    'manchester united',
    'newcastle united',
    'norwich city',
    'southampton',
    'tottenham hotspur',
    'watford',
    'west ham united',
    'wolverhampton wanderers',
    'premier league',
    'epl',
    'man utd',
    'man city',
]


def operations_callback(ops):
    # Here we can filter, process, run ML classification, etc.
    # After our feed alg we can save posts into our DB
    # Also, we should process deleted posts to remove them from our DB and keep it in sync

    # For example, let's create our custom feed that will contain all posts that contain EPL-related keywords

    posts_to_create = []
    for created_post in ops['posts']['created']:
        record = created_post['record']

        # Only EPL-related posts
        if any(key in record['text'].lower() for key in epl_key_names):
            reply_parent = None
            if record['reply'] and record['reply']['parent']['uri']:
                reply_parent = record['reply']['parent']['uri']

            reply_root = None
            if record['reply'] and record['reply']['root']['uri']:
                reply_root = record['reply']['root']['uri']

            post_dict = {
                'uri': created_post['uri'],
                'cid': created_post['cid'],
                'reply_parent': reply_parent,
                'reply_root': reply_root,
            }
            posts_to_create.append(post_dict)

    posts_to_delete = [p['uri'] for p in ops['posts']['deleted']]
    if posts_to_delete:
        Post.delete().where(Post.uri.in_(posts_to_delete)).execute()
        logger.info(f'Deleted from feed: {len(posts_to_delete)}')

    if posts_to_create:
        with db.atomic():
            Post.insert_many(posts_to_create).execute()
        logger.info(f'Added to feed: {len(posts_to_create)}')
