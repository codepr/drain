import redis


class RedisConnector:
    def __init__(self, redis_url, *args, **kwargs):
        self._redis_url = redis_url
        super().__init__(*args, **kwargs)

    @property
    def redis(self):
        if not hasattr(self, "_redis"):

            _redis = redis.from_url(self._redis_url)
            setattr(self, "_redis", _redis)
        return self._redis
