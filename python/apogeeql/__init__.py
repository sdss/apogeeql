from sdsstools import get_logger

log = get_logger('apogeeql')

configFile = os.path.join(os.path.dirname(__file__), 'etc/apogeeql.cfg')

try:
    config = yaml.load(open(configFile), Loader=yaml.FullLoader)
except AttributeError:
    # using pyyaml < 5, enforce old behavior
    config = yaml.load(open(configFile))

__version__ = '2.0.0a0'
