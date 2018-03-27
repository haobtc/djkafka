import json
from .models import KafkaConsumerOffset, KafkaBuffer

def add_to_buffer(db, topic, data, use_json=True, partition=0):
    return KafkaBuffer.objects.add_to_buffer(
        db,
        topic, data, use_json=use_json,
        partition=partition)
