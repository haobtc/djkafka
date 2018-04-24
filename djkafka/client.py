import json
import time
import sys
import msgpack
from datetime import timedelta
from django.db import transaction
from django.utils import timezone
from django.utils.encoding import smart_bytes
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from .defaults import config
from .models import KafkaConsumerOffset, KafkaBuffer

testing = sys.argv[1:2] == ['test']
def get_topic(topic):
    if testing:
        # under test mode, add a prefix to avoid name conflict
        return 'test-' + topic
    else:
        return topic

class Consumer:
    def __init__(self, db, topic, server='default', partition=None, serialize='json', offset_reset='latest', **kwargs):
        self.db = db
        self.topic = topic

        servers = config['SERVERS'][server]['BOOTSTRAP_SERVERS']
        kwargs.setdefault('bootstrap_servers', servers)

        if serialize == 'json':
            kwargs['value_deserializer'] = lambda v:json.loads(v, encoding='utf-8')
        elif serialize == 'msgpack':
            kwargs['value_deserializer'] = lambda v:msgpack.loads(v, encoding='utf-8')

        if offset_reset == 'latest':
            kwargs.setdefault('auto_offset_reset', 'latest')
        else:
            kwargs.setdefault('auto_offset_reset', 'earlest')

        kwargs.setdefault('enable_auto_commit', False)
        kwargs.setdefault('consumer_timeout_ms', 1000)

        # TODO: support topic group, using partition
        self.consumer = KafkaConsumer(**kwargs)

        if partition is not None:
            tps = [TopicPartition(get_topic(self.topic), partition)]
        else:
            tps = self.get_topic_partitions(self.topic)
        self.consumer.assign(tps)

        for tp in tps:
            try:
                offset = KafkaConsumerOffset.objects.using(self.db).get(
                    topic=self.topic,
                    partition=partition)
                self.consumer.seek(tp, offset.offset)
            except KafkaConsumerOffset.DoesNotExist:
                if offset_reset == 'latest':
                    self.consumer.seek_to_end(tp)
                else:
                    self.consumer.seek_to_begining(tp)

    def get_topic_partitions(self, topic):
        k_topic = get_topic(topic)
        partitions = self.consumer.partitions_for_topic(
            k_topic)
        if not partitions:
            return [TopicPartition(k_topic, 0)]
        return [TopicPartition(
            k_topic, p) for p in partitions]

    def save_offset(self, msg):
        assert msg.topic == get_topic(self.topic)
        return KafkaConsumerOffset.objects.update_offset(
            self.db,
            self.topic,
            msg.partition,
            msg.offset)

class Producer:
    def __init__(self, db, server='default', **kwargs):
        self.db = db
        self.producer_kwargs = kwargs
        self._producers = {}

    def get_producer(self, server):
        if server not in self._producers:
            kwargs = self.producer_kwargs.copy()
            servers = config['SERVERS'][server]['BOOTSTRAP_SERVERS']
            kwargs.setdefault('bootstrap_servers', servers)
            producer = KafkaProducer(**kwargs)
            self._producers[server] = producer
        return self._producers[server]

    def send(self, topic, data, server='default'):
        data = smart_bytes(data)
        return self.get_producer(server).send(get_topic(topic), data)

    def send_json(self, topic, data, server='default'):
        data = json.dumps(data)
        return self.send(topic, data, server=server)

    def send_msgpack(self, topic, data, server='default'):
        data = msgpack.dumps(data)
        return self.send(topic, data, server=server)

    def add_to_buffer(self, db, topic, data, server='default', **kwargs):
        return KafkaBuffer.objects.add_to_buffer(
            db, topic, data, server=server, **kwargs)

    def push_buffer(self, times=10000, wait_on_idle=None):
        '''
        push buffered data to kafka server
        '''
        #assert self.serialize == 'plain'

        cnt_pushed = 0
        for _ in range(times):
            with transaction.atomic(using=self.db):
                buf = KafkaBuffer.objects.using(
                    self.db).select_for_update().filter(
                        is_sent=False).first()
                if buf:
                    self.send(buf.topic, smart_bytes(buf.data), server=buf.server)
                    KafkaBuffer.objects.using(
                        self.db).filter(id=buf.id).update(
                            is_sent=True)
                    cnt_pushed += 1
                elif wait_on_idle is None:
                    break
            if not buf and wait_on_idle is not None:
                time.sleep(wait_on_idle)
        return cnt_pushed

    def clear_buffer(self, seconds_ago=86400, max_size=10000):
        clear_start = timezone.now() - timedelta(seconds=seconds_ago)
        return KafkaBuffer.objects.using(self.db).filter(
            is_sent=True,
            created_at__lt=clear_start
        ).order_by('id')[:max_size].delete()
