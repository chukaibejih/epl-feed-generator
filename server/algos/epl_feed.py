from datetime import datetime
from typing import Optional

from server import config
from server.database import Post
from server.data_filter import epl_key_names
uri = config.EPL_FEED_URI

def handler(cursor: Optional[str], limit: int) -> dict:
    query = Post.select().order_by(Post.indexed_at.desc(), Post.cid.desc()).limit(limit)

    if cursor:
        cursor_parts = cursor.split('::')
        if len(cursor_parts) != 2:
            raise ValueError('Malformed cursor')

        indexed_at, cid = cursor_parts
        indexed_at = datetime.fromtimestamp(int(indexed_at) / 1000).isoformat()
        query = query.where(Post.indexed_at < indexed_at).where(Post.cid < cid)

    # Modify the database query to retrieve EPL-related posts
    # Add conditions to match EPL-related posts using the epl_key_names list
    # query = query.where(Post.record.text.contains('premier league') |
    #                    Post.record.text.contains('epl') |
    #                    Post.record.text.contains('english premier league'))

    # for keyword in epl_key_names:
    #     query = query.where(Post.record.text.contains(keyword))

    posts = [post for post in query]

    feed = [{'post': post.uri} for post in posts]

    cursor = None
    last_post = posts[-1] if posts else None
    if last_post:
        cursor = cursor = f"{int(datetime.fromisoformat(last_post.indexed_at.isoformat()).timestamp() * 1000)}::{last_post.cid}"


    return {
        'cursor': cursor,
        'feed': feed
    }
