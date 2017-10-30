import ConfigParser
import getopt
import sys
import os
import json


def get_sections_settingnames(args):
    '''
    given a string section1:name1,name2...;section2:name1,name2...
    return a dict with lists of names by section
    '''
    fields = args.split(';')
    sections = {}
    for field in fields:
        section, items = field.split(':', 1)
        sections[section] = items.split(',')
    return sections


def get_setting_from_overrides(conf, overrides, setting):
    '''
    look for a setting in the overrides section if
    the section and the setting in that section exist;
    return it if so, return None otherwise
    '''
    if not overrides:
        return None
    for secname in overrides:
        if not secname:
            continue
        if not conf.has_section(overrides):
            continue
        if not conf.has_option(overrides, setting):
            continue
        return conf.get(overrides, setting)
    return None


def display(confs, outformat):
    '''
    given a dict of conf settings and values,
    display them in the requested format
    '''
    if outformat == "json":
        print json.dumps(confs)
    elif outformat == "txt":
        for section in confs:
            print "section:%s" % section
            for item in confs[section]:
                print "item:%s:%s" % (item, confs[section][item])
    elif outformat == "values":
        for section in confs:
            for item in sorted(confs[section]):
                print "%s" % confs[section][item]
    else:
        for section in confs:
            for item in confs[section]:
                print "%s %s" % (item, confs[section][item])


def getconfs(configfile, overrides, args, outformat):
    '''
    given a configfile path and a string
    section1:name1,name2...;section2:name1,name2...
    print a json representation of a dict with
    the setting names and values per section
    if the overrides argument is supplied, arguments
    in this list of sections will override the values
    in the specific section requested.
    '''
    conf = ConfigParser.SafeConfigParser()
    conf.read(configfile)
    confs = {}
    sections = get_sections_settingnames(args)
    for section in sections:
        confs[section] = {}
        for setting in sections[section]:
            result = get_setting_from_overrides(conf, overrides, setting)
            if result:
                confs[section][setting] = result
            elif conf.has_option(section, setting):
                confs[section][setting] = conf.get(section, setting)
    display(confs, outformat)


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''

    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: getconfigvals.py --configfile path[:override_sec1[,override_sec2...]]
           --args section:name[,name...][;section:name[,name...]]
           [--help]

Get and display settings and values from a config file
in ConfigParser format

Note that this script does not load any defaults for config values.
It also cannot deal with per-wiki config value settings, unless you
explicitly set up the config file with a section for the wiki and
pass that in as an override section (see --configfile below).

Options:

  --configfile (-c):  path to config file
                      you may tack on colon ':' and a comma-separated list of
                      sections in which to look first for values, for example
                      the wiki project name, or a section 'bigwikis' that might
                      have values that override the regular ones.
  --args       (-a):  names of args for which to check the config file;
                      config file section names must be specified
                      along with the arg names
  --format     (-f):  output format (json, txt, pairs, values), default: json
                      json does what you expect, with a dict of section names, item names
                      and values. txt produces a text representation with each section and
                      item on a separate line. pairs produces item names and values
                      only, each on a separate line, space separated. values produces
                      a list of values only, one on each line, sorted by item name
                      within each section.
                      If an item is missing it is silently ignored.
  --help       (-h):  display this usage message

Examples: getconfig.py --configfile confs/wikidump.conf \
                 --args 'tools:php,mysqldump,gzip'
          getconfig.py --configfile confs/wikidump.conf:enwiki,hugewikis \
                 --args 'tools:php,mysqldump,gzip'
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_args(options):
    '''
    get and return the args passed on command line
    '''
    configfile = None
    args = None
    outformat = "json"

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configfile = val
        elif opt in ["-a", "--args"]:
            args = val
        elif opt in ["-f", "--format"]:
            outformat = val
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
            usage("Unknown option specified: <%s>" % opt)
    return (configfile, args, outformat)


def main():
    'main entry point, does all the work'

    overrides = None

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "c:a:f:h", ["configfile=", "args=", "format=", "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    (configfile, args, outformat) = get_args(options)

    if len(remainder) > 0:
        usage("Unknown option(s) specified: <%s>" % remainder[0])

    if configfile is None:
        usage("Mandatory argument --configfile not specified")
    if args is None:
        usage("Mandatory argument --args not specified")
    if outformat not in ["txt", "json", "pairs", "values"]:
        usage("Unknown format type %s" % outformat)

    if ':' in configfile:
        configfile, overrides = configfile.split(':', 1)
    if not overrides:
        overrides = []
    elif ',' in overrides:
        overrides = overrides.split(',')

    if not os.path.exists(configfile):
        usage("no such file found: " + configfile)

    getconfs(configfile, overrides, args, outformat)


if __name__ == '__main__':
    main()
