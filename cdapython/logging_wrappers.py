import logging
import logging.config
import os
import sys
import yaml

#############################################################################################################################
#
# Enable console logging (on by default, but can be turned off).
#
#############################################################################################################################

def enable_console_logging():
    
    os.environ['__CDA_LOG_TO_CONSOLE'] = 'True'

#############################################################################################################################
#
# Disable console logging.
#
#############################################################################################################################

def disable_console_logging():
    
    os.environ['__CDA_LOG_TO_CONSOLE'] = 'False'

#############################################################################################################################
#
# Enable file logging.
#
#############################################################################################################################

def enable_file_logging( filename='cdapython_log.txt' ):
    
    os.environ['__CDA_LOG_TO_FILE'] = filename

#############################################################################################################################
#
# Disable file logging (off by default, but can be turned on).
#
#############################################################################################################################

def disable_file_logging():
    
    os.environ['__CDA_LOG_TO_FILE'] = ''

#############################################################################################################################
#
# get_logger(): Returns logger instance that uses config file settings and optional user config inputs to initialize
#
#############################################################################################################################

def get_logger() -> logging.Logger:
    """
    Returns logger instance that uses config file settings to initialize.

    Returns:
        log: logging tool that can be used to output messages of varying granularity
    """

    # Establish the current log level. If none exists, default to `logging.WARNING`.

    if '__CDA_LOG_LEVEL' not in os.environ:
        os.environ['__CDA_LOG_LEVEL'] = 'WARNING'

    level = os.environ['__CDA_LOG_LEVEL']

    # Echo log messages to standard output? (Default: yes)

    if '__CDA_LOG_TO_CONSOLE' not in os.environ:
        os.environ['__CDA_LOG_TO_CONSOLE'] = 'True'

    # Echo log messages to a file? (Default: no, i.e. __CDA_LOG_TO_FILE == None)

    if '__CDA_LOG_TO_FILE' not in os.environ:
        os.environ['__CDA_LOG_TO_FILE'] = ''

    # Load the default logger configuration.

    parent_dir = os.path.dirname( os.path.abspath( __file__ ) )
    logger_default_config_file = os.path.join( parent_dir, 'config', 'logger_default_config.yml' )

    with open( logger_default_config_file ) as IN:
        logger_configuration = yaml.safe_load( IN )

    # Set the log level in the configuration object to whatever the current environment dictates.

    logger_configuration['loggers']['default']['level'] = level

    # Modify logger configuration defaults according to user-modified session-level settings.

    if os.environ['__CDA_LOG_TO_CONSOLE'] == 'False':
        logger_configuration['loggers']['default']['handlers'].remove( 'console' )

    if os.environ['__CDA_LOG_TO_FILE'] != '':
        logger_configuration['loggers']['default']['handlers'].append( 'file' )
        logger_configuration['handlers']['file']['filename'] = os.environ['__CDA_LOG_TO_FILE']

    # Make sure we didn't remove all possible handlers.

    if len( logger_configuration['loggers']['default']['handlers'] ) == 0:
        print( 'ERROR: get_logger(): console and file output both disabled; can\'t create logger.', file=sys.stderr )
        return

    logging.config.dictConfig( logger_configuration )

    logger = logging.getLogger('default')

    return logger

#############################################################################################################################
#
# get_valid_log_levels(): Returns list of log level strings that can be used to set_log_level
#
#############################################################################################################################

def get_valid_log_levels():
    """
    Returns list of log level strings that can be passed to set_log_level().

    Returns:
        set of module-defined strings: all valid human-readable labels for log levels that can be passed to set_log_level().
    """
    return { 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL' }

#############################################################################################################################
#
# set_log_level(): Changes the current log level
#
#############################################################################################################################

def set_log_level( level=None ):
    """
    Changes the current log level. Valid values are 'debug', 'info', 'warning', 'error', and 'critical'.
    """
    if level is None:
        print( f"ERROR: set_log_level(): log level cannot be null. Try something like 'set_log_level( 'debug' )'.", file=sys.stderr )
        return

    level = level.upper()

    if level not in get_valid_log_levels():
        print( f"ERROR: set_log_level(): log level '{level}' invalid. Try help( set_log_level ) for a list of valid level names.", file=sys.stderr )
        return

    os.environ['__CDA_LOG_LEVEL'] = level

#############################################################################################################################
#
# get_log_level(): Returns the current log level
#
#############################################################################################################################

def get_log_level( level=None ):
    """
    Returns the current user-specified log level, if set, or the default log level, if not.
    """

    if '__CDA_LOG_LEVEL' in os.environ:
        
        level = os.environ['__CDA_LOG_LEVEL']

    if level is None:
        
        return 'WARNING'

    else:
        
        return level


