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
            default=0.02,
            help='sleep interval during each buffer pushing')

        parser.add_argument(
            '--clear',
            type=bool,
            default=True,
            help='clear outdated buffer')

    def handle(self, *args, **options):
        p = Producer(options['database'])

        i = 0
        while True:
            i += 1
            cnt = p.push_buffer()
            if cnt <= 0:
                if options['clear'] and (i % 1000 == 0):
                    p.clear_buffer()
                time.sleep(options['sleep'])
