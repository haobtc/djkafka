import logging
import json
import time
from django.test import TestCase
from django.db import transaction
from .client import Consumer, Producer
from .models import KafkaConsumerOffset, KafkaBuffer
from .helpers import add_to_buffer

class KafkaTestCase(TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)
        KafkaConsumerOffset.objects.all().delete()
        KafkaBuffer.objects.all().delete()

    def testConsumer(self):

        '''
        typical usage:
        for msg in c.consumer:
            with transaction.atomic(using='xxxx'):
                ....
                c.save_offset(msg)
        '''
        c = Consumer('default', 'msglist')
        p = Producer('default')
        p.send_json('msglist', 'aaa')
        p.send_json('msglist', 'bbb')
        msgs = []
        while len(msgs) < 2:
            for msg in c.consumer:
                with transaction.atomic(using='default'):
                    msgs.append(msg)
                    c.save_offset(msg)

        self.assertEquals(msgs[0].value, 'aaa')
        self.assertEquals(msgs[1].value, 'bbb')
        offset = KafkaConsumerOffset.objects.filter(
            topic='msglist')[0]
        self.assertEquals(msgs[1].offset, offset.offset)

    def testMsgpackConsumer(self):
        '''
        typical usage:
        for msg in c.consumer:
            with transaction.atomic(using='xxxx'):
                ....
                c.save_offset(msg)
        '''
        c = Consumer('default', 'msglist2', serialize='msgpack')
        p = Producer('default')
        p.send_msgpack('msglist2', 'aaa')
        p.send_msgpack('msglist2', ['bbb', 'ccc'])
        msgs = []
        while len(msgs) < 2:
            for msg in c.consumer:
                with transaction.atomic(using='default'):
                    msgs.append(msg)
                    c.save_offset(msg)

        self.assertEquals(msgs[0].value, 'aaa')
        self.assertEquals(msgs[1].value, ['bbb', 'ccc'])
        offset = KafkaConsumerOffset.objects.filter(
            topic='msglist2')[0]
        self.assertEquals(msgs[1].offset, offset.offset)

    def testBuffer(self):
        add_to_buffer('default', 'msglist1', 'ccc')
        add_to_buffer('default', 'msglist1', 'ddd')

        old_count = KafkaBuffer.objects.filter(is_sent=False).count()

        c = Consumer('default', 'msglist1')
        p = Producer('default')
        cnt = p.push_buffer()
        self.assertEquals(cnt, 2)
        self.assertEquals(KafkaBuffer.objects.filter(is_sent=False).count(), old_count - 2)
        msgs = []
        while len(msgs) < 2:
            for msg in c.consumer:
                with transaction.atomic(using='default'):
                    msgs.append(msg)
                    c.save_offset(msg)

        self.assertEquals(msgs[0].value, 'ccc')
        self.assertEquals(msgs[1].value, 'ddd')

