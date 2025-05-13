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
    """
    Enable console logging (log messages are written to standard output: this feature is enabled by default, but can be turned off).
    """

    os.environ['__CDA_LOG_TO_CONSOLE'] = 'True'

#############################################################################################################################
#
# Disable console logging.
#
#############################################################################################################################

def disable_console_logging():
    """
    Prevent log messages from being displayed to standard output.
    """

    os.environ['__CDA_LOG_TO_CONSOLE'] = 'False'

#############################################################################################################################
#
# Enable file logging.
#
#############################################################################################################################

def enable_file_logging( filename='cdapython_log.txt' ):
    """
    Write log messages to `filename`. This does not prevent log messages from being written to standard output as well:
    see disable_console_logging() to switch that feature off if desired.
    """

    os.environ['__CDA_LOG_TO_FILE'] = filename

#############################################################################################################################
#
# Disable file logging (off by default, but can be turned on).
#
#############################################################################################################################

def disable_file_logging():
    """
    Stop writing log messages to a local file.
    """

    os.environ['__CDA_LOG_TO_FILE'] = ''

#############################################################################################################################
#
# get_logger(): Returns logger instance that uses config file settings and optional user config inputs to initialize
#
#############################################################################################################################

def get_logger() -> logging.Logger:
    """
    Returns a logging.Logger instance initalized according to settings in config/logger_default_config.yml.

    Returns:
        logging.Logger object: an interface that can be used to output messages of varying granularity/severity.
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
    Changes the current log level. A list of valid values is returned by get_valid_log_levels().
    """
    if level is None:
        print( f"ERROR: set_log_level(): log level cannot be null. Try something like 'set_log_level( 'debug' )'.", file=sys.stderr )
        return

    level = level.upper()

    if level not in get_valid_log_levels():
        print( f"ERROR: set_log_level(): log level '{level}' invalid. Try get_valid_log_levels() for a list of valid level names.", file=sys.stderr )
        return

    os.environ['__CDA_LOG_LEVEL'] = level

#############################################################################################################################
#
# get_log_level(): Returns the current log level
#
#############################################################################################################################

def get_log_level():
    """
    Returns the current user-specified log level, if set, or the default log level, if not.
    """

    level = None

    if '__CDA_LOG_LEVEL' in os.environ:
        
        level = os.environ['__CDA_LOG_LEVEL']

    if level is None:
        
        return 'WARNING'

    else:
        
        return level


