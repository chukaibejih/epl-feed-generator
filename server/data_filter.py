import logging
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


def operations_callback(ops: dict) -> None:
    posts_to_create = []
    for created_post in ops['posts']['created']:
        record = created_post['record']
        post_text = record.text.lower()

        # Check if the post text contains English Premier League key names
        if any(key_name in post_text for key_name in epl_key_names):
            reply_parent = record.reply.parent.uri if record.reply and record.reply.parent.uri else None
            reply_root = record.reply.root.uri if record.reply and record.reply.root.uri else None

            post_dict = {
                'uri': created_post['uri'],
                'cid': created_post['cid'],
                'reply_parent': reply_parent,
                'reply_root': reply_root,
            }
            posts_to_create.append(post_dict)

    posts_to_delete = [p['uri'] for p in ops['posts']['deleted']]
    if posts_to_delete:
        delete_query = Post.delete().where(Post.uri.in_(posts_to_delete))
        delete_query.execute()
        logger.info(f'Deleted from feed: {len(posts_to_delete)}')

    if posts_to_create:
        with db.atomic():
            for post_dict in posts_to_create:
                post = Post.create(**post_dict)
        logger.info(f'Added to feed: {len(posts_to_create)}')
