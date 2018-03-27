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

        last_time = time.time()
        while True:
            cnt = p.push_buffer()
            if cnt <= 0:
                if options['clear'] and time.time() > last_time + 600:
                    # clear buffer every 10 mins
                    p.clear_buffer()
                    last_time = time.time()
                time.sleep(options['sleep'])
