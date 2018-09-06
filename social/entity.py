import helpers


def batches(iterable, n=10):
    """divide a single list into a list of lists of size n """
    batchLen = len(iterable)
    for ndx in range(0, batchLen, n):
        yield list(iterable[ndx:min(ndx + n, batchLen)])


class SocialStatements:

    def __init__(self, logger, engine=None):
        self.users = []
        self.engine = engine
        self.logger = logger
        self.tracks = []

    user_schema = {
        "table_name": "user",
        "options": {
            "primary_key": ["uri", "date"],
            "order_by": ["date desc"]
        },
        "columns": {
            "uri": "text",
            "date": "date",
            "avatar_url": "text",
            "first_name": "text",
            "last_name": "text",
            "city": "text",
            "country": "text",
            "country_code": "text",
            "screenname": "text",
            "url": "text",
            "username": "text",
            "id": "int",
            "following": "int",
            "followers": "int",
            "track_count": "int",
            "reposts_count": "int",
            "comments_count": "int",
            "website": "text",
            "description": "text",
            "contact_info": "map<text, text>",
            "links": "set<text>"
        }
    }

    track_schema = {
        "table_name": "track",
        "options": {
            "primary_key": ["uri", "date"]
        },
        "columns": {
            "uri": "text",
            "date": "date",
            "score": "decimal",
            "url": "text",
            "artwork_url": "text",
            "comment_count": "int",
            "duration": "int",
            "description": "text",
            "genre": "text",
            "label_name": "text",
            "likes_count": "int",
            "playback_count": "int",
            "created_at": "text",
            "reposts_count": "int",
            "tag_list": "text",
            "title": "text",
            "user_id": "int"
        }
    }

    def save(self, batch_size=20, users=None, tracks=None):
        """Write these social statements to the data engine in the appropriate manner."""
        self.users = users
        self.tracks = tracks
        if self.users:
            self.logger.info('about to send {} user statements to the data engine'.format(len(self.users)))
            self._write_batches(self.engine, self.logger, self.user_schema, self.users, batch_size)
        else:
            self.logger.debug('skipping user ingest, no records in these social statements')

        if self.tracks:
            self.logger.info('about to send {} tracks statements to the data engine'.format(len(self.tracks)))
            self._write_batches(self.engine, self.logger, self.track_schema, self.tracks, batch_size)
        else:
            self.logger.info('skipping track ingest, no records in these social statements')

    @staticmethod
    def _write_batches(engine, logger, schema, data, batch_size=20):
        for rows in batches(data, batch_size):
            logger.info('Rows: {}'.format(rows))
            res = engine.save(schema, list(rows)).result()
            logger.info(res)
