from skafossdk import *
from social.entity import SocialStatements
from soundcloud.soundcloud_proccessor import SoundcloudProcessor
from helpers.logger import get_logger


# Initialize the skafos sdk
ska = Skafos()

ingest_log = get_logger('user-fetch')

if __name__ == "__main__":
    ingest_log.info('Starting job')

    ingest_log.info('Fetching soundcloud user data')
    entity = SocialStatements(ingest_log, ska.engine) #,ska.engine
    processor = SoundcloudProcessor(entity, ingest_log).fetch()
