import time
from django.core.management.base import BaseCommand
from ...client import Producer

class Command(BaseCommand):
    help = 'push db buffered msg to kafka server'
    def add_arguments(self, parser):
        parser.add_argument(
            '--database',
            type=str,
            default='default',
            help='database used')

        parser.add_argument(
            '--sleep',
            type=float,
            default=0.01,
            help='sleep interval during each buffer pushing')

    def handle(self, *args, **options):
        p = Producer(options['database'])

        while True:
            cnt = p.push_buffer()
            if cnt <= 0:
                time.sleep(options['sleep'])
