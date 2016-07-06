import redis


FUNC_PREFIX = 'hmq:function:'


class QueueManager(object):
    def __init__(self, *args, **kwargs):
        self._redis = redis.StrictRedis(*args, **kwargs)
        self._sha1 = dict()

    def _get_function_hash(self, function_name):
        """
        :type function_name: str
        """
        key = FUNC_PREFIX + function_name
        hash = self._sha1.get(function_name)
        if not hash:
            hash = self._redis.hget(key, 'sha1')
        if not hash:
            raise NotImplementedError('TODO create function from file')
        return hash

    def __getitem__(self, name):
        return Queue(self, name)


class Queue(object):
    def __init__(self, manager, name):
        self._manager = manager
        self._name = name

    def subscribe(self, event):
        """
        :type event: str
        """
        manager = self._manager
        subscribe_hash = manager._get_function_hash('subscribe')
        manager._redis.evalsha(subscribe_hash, 0, event, self._name)

    def publish(self, event, payload):
        """
        :type event: str
        :type payload: str
        """
        manager = self._manager
        publish_hash = manager._get_function_hash('publish')
        manager._redis.evalsha(publish_hash, 0, event, payload)

    def next_event(self, block=True):
        queue_name = self._name
        manager = self._manager
        if block:
            _, data = manager._redis.brpop(queue_name)
        else:
            data = manager._redis.rpop(queue_name)
        return data

    def listen(self):
        while True:
            data = self.next_event()
            yield data
