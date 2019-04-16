import logging
import argparse
import sys
from extension.base import KVKeyError
#import utils.kv_utils as kv

logging.basicConfig(level = logging.INFO,format = '[%(asctime)s][%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

KVTypes = ('redis', 'other')

def get_cli_class(kv_type):
    mod_name = "extension.%s_ext" % kv_type
    class_name = "%s_ext" % kv_type
    try:
        mod = __import__(mod_name)
        components = mod_name.split('.')
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return getattr(mod, class_name)
    except ImportError:
        logger.error("no such plugin in extension path:%s", class_name)
        sys.exit(1)


class CompareKV(object):
    def __init__(self, cliA, cliB):
        self.cliA = cliA
        self.cliB = cliB

    def compare_key(self, key):
        try:
            v1 = self.cliA.get(key)
        except KVKeyError, e:
            logger.debug("cliA: %s, %s", key, e)
            v1 = False
        try:
            v2 = self.cliB.get(key)
        except KVKeyError, e:
            logger.debug("cliB: %s, %s", key, e)
            v2 = False
        return compare_value(v1, v2)

def ReadKeyList(listFile):
    with open(listFile) as f:
        while True:
            line = f.readline()
            if line:
                yield line.strip()
            else:
                break


def compare_value(v1, v2):
    return v1 == v2


def parse_args(args):
    parser = argparse.ArgumentParser()
    #parser.add_argument("echo")
    parser.add_argument("-v", "--verbosity", help="increase output verbosity", action='store_true')
    parser.add_argument('-t', '--type', help="redis or kv", choices=KVTypes, required=True)
    parser.add_argument('-f', '--keyListFile', help='key list file', action='store', required=True)
    parser.add_argument('cluster_bootstrap', nargs=2, type=str, help="exp: cluster_bootstrap1 cluster_bootstrap2")
    args = parser.parse_args()
    return args


def main():
    options = parse_args(list(sys.argv[1:]))
    keyListFile = options.keyListFile
    if options.verbosity:
        print "set debug"
        logger.setLevel(logging.DEBUG)
    kv_class = get_cli_class(options.type)
    kv_address = options.cluster_bootstrap[0]
    kv_address2 = options.cluster_bootstrap[1]
    cliA = kv_class(kv_address)
    cliB = kv_class(kv_address2)
    compare_task = CompareKV(cliA, cliB)
    err_count = 0
    for key in ReadKeyList(keyListFile):
        try:
            if compare_task.compare_key(key):
                logger.debug("key=%s ret=0 msg=null" % (key))
            else:
                logger.error("key=%s ret=1 msg=null" % (key))
                err_count += 1
        except Exception, e:
            logger.error("key=%s ret=2 msg=%s" % (key, e))
            err_count += 1
    if err_count > 0:
        logger.error("total error count: %s", err_count)
        return 1
    logger.info("very good!")
    return 0

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
