#!/usr/bin/python

"""pyxdump

mysql dump import toolkit

Usage:
  pyxdump <module> <func> [<args>...]

Options:
  -h --help            Show this screen.
"""

from docopt import docopt
import sys
from . import xdump_module
from .xdump_module import *

def main():
    """
    run module

    example:
    pyxdump task track --trackid xxx
    """
    args = docopt(__doc__, options_first=True)
    module = args['<module>']
    func = args['<func>']
    argv = [module, func]+args['<args>']
    module_script = getattr(xdump_module, module)
    module_args = docopt(module_script.__doc__, argv=argv)
    return getattr(module_script, func)(module_args)

if __name__ == '__main__':
    main()
