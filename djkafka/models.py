import json
import msgpack
from django.db import models

class KafkaConsumerOffsetManager(models.Manager):
    def update_offset(self, db, topic, partition, offset, server='default'):
        o, _ = self.using(db).update_or_create(
            topic=topic, partition=partition,
            defaults={
                'offset': offset
            })
        return o

class KafkaConsumerOffset(models.Model):
    server = models.CharField(max_length=100, db_index=True, default='default')
    topic = models.CharField(max_length=100, db_index=True)
    partition = models.IntegerField(default=0)
    offset = models.BigIntegerField()

    objects = KafkaConsumerOffsetManager()

    class Meta:
        db_table = 'kafka_offset'
        unique_together = [('server', 'topic', 'partition')]

    def __unicode__(self):
        return u'{}#{}'.format(self.topic, self.partition)

class KafkaBufferManager(models.Manager):
    def add_to_buffer(self, db, topic, data, server='default', serialize='json', partition=0):
        if serialize == 'json':
            data = json.dumps(data)
        elif serialize == 'msgpack':
            data = msgpack.dumps(data)
        return self.using(db).create(
            server=server,
            topic=topic,
            partition=partition,
            data=data)

class KafkaBuffer(models.Model):
    '''
    this model is not necessary since we can directly
    push data to kafka within a database transaction
    '''
    id = models.BigAutoField(primary_key=True)
    is_sent = models.BooleanField(default=False, db_index=True)
    server = models.CharField(max_length=100, default='default')
    topic = models.CharField(max_length=100)
    partition = models.IntegerField(default=0)
    data = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    objects = KafkaBufferManager()

    class Meta:
        db_table = 'kafka_buffer'
        index_together = [
            ('is_sent', 'id'),
            ('is_sent', 'created_at')
        ]

    def __unicode__(self):
        return self.topic
