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
    'man utd',
    'man city',
    '#epl',
    '#EPL'
]


def operations_callback(ops, record):
    # Filter posts with epl keywords
    # After our feed alg, save posts into the DB
    # Also, we should process deleted posts to remove them from our DB and keep it in sync

    posts_to_create = [
        {
            'uri': created_post['uri'],
            'cid': created_post['cid'],
            'reply_parent': record.reply.parent.uri if record.reply else None,
            'reply_root': record.reply.root.uri if record.reply else None,
        }
        for created_post in ops['posts']['created']
        if any(key.lower() in record['text'] for key in epl_key_names)
    ]

    posts_to_delete = [p['uri'] for p in ops['posts']['deleted']]
    if posts_to_delete:
        with db.atomic():
            Post.delete().where(Post.uri.in_(posts_to_delete))
            logger.info(f'Deleted from feed: {len(posts_to_delete)}')

    if posts_to_create:
        with db.atomic():
            for post_dict in posts_to_create:
                Post.create(**post_dict)
        logger.info(f'Added to feed: {len(posts_to_create)}')


