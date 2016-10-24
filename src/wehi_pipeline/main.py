'''
Created on 24Oct.,2016

@author: thomas.e
'''

import ruffus.cmdline as cmdline
from pipeline_base.config import Config
from pipeline_base.state import State
from pipeline_base.logger import Logger
from pipeline_base import error_codes
import sys


# default name of the pipeline configuration file
DEFAULT_CONFIG_FILE = 'pipeline.config'
# default place to save cluster job scripts
# (mostly useful for post-mortem debugging)
DEFAULT_JOBSCRIPT_DIR = 'jobscripts'


def parse_command_line(version):
    '''Parse the command line arguments of the pipeline'''
    parser = cmdline.get_argparse(description='Variant calling pipeline',
        ignored_args = ["version"] )
    
    parser.add_argument('--config', type=str, default=DEFAULT_CONFIG_FILE,
        help='Pipeline configuration file in YAML format, defaults to {}' \
            .format(DEFAULT_CONFIG_FILE))
    
    parser.add_argument('--jobscripts', type=str,
        default=DEFAULT_JOBSCRIPT_DIR,
        help='Directory to store cluster job scripts created by the ' \
             'pipeline, defaults to {}'.format(DEFAULT_JOBSCRIPT_DIR))
    
    parser.add_argument('--version', action='version',
        version='%(prog)s ' + version)
    
    parser.add_argument('--describe', action='describe')
    
    return parser.parse_args()


#def main():
def main(program_name, program_version, make_pipeline):
    
    '''Initialise the pipeline, then run it'''
    # Parse command line arguments
    options = parse_command_line(program_version)
    
    if options.describe:
        describe(make_pipeline)
        return;
    
    # Initialise the logger
    logger = Logger(__name__, options.log_file, options.verbose)
    # Log the command line used to run the pipeline
    logger.info(' '.join(sys.argv))
    # Parse the configuration file, and initialise global state
    config = Config(options.config)
    config.validate()
            
    state = State(options=options, config=config, logger=logger,
                  drmaa_session=drmaa_session(config))
    
    # Build the pipeline workflow
    pipeline = make_pipeline(state)
    # Run (or print) the pipeline
    cmdline.run(options)
    if drmaa_session is not None:
        # Shut down the DRMAA session
        drmaa_session.exit()

def drmaa_session(config):
    drmaa_session = None
    if not config.isLocal():
        try:
            # Set up the DRMAA session for running cluster jobs
            import drmaa
            drmaa_session = drmaa.Session()
            drmaa_session.initialize()
        except Exception as e:
            print("{progname} error using DRMAA library".format(progname=program_name), file=sys.stdout)
            print("Error message: {msg}".format(msg=e.message, file=sys.stdout))
            exit(error_codes.DRMAA_ERROR)
            
def describe(make_pipeline):
    print('hello')
    