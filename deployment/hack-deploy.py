import os
import sys
import salt.client
import salt.cli.cp
import salt.utils
import time
import hashlib
import re
from salt.exceptions import SaltInvocationError
import runpy

# todo: test salt cmd_expandminions


class LocalClientPlus(salt.client.LocalClient):
    '''
    extend the salt LocalClient module with methods for showing
    list of known minions that match the specified expression,
    and for copying file content to a newly created remote file
    '''

    def condition_kwarg(self, arg, kwarg):
        '''
        Return a single arg structure for caller to use
        '''
        if isinstance(kwarg, dict):
            kw_ = []
            for key, val in kwarg.items():
                kw_.append('{0}={1}'.format(key, val))
            return list(arg) + kw_
        return arg

    def cmd_expandminions(self, tgt, fun, arg=(), timeout=None,
                          expr_form='glob', ret='',
                          kwarg=None, **kwargs):
        '''
        return an expanded list of minions, assuming that the expr form
        is glob or list or some other such thing that can be expanded
        and not e.g. grain based

        this is wasteful because we actually run the job but it's less
        wasteful than
          salt "$deployhosts" -v --out raw test.ping |
          grep '{' | mawk -F"'" '{ print $2 }'
        '''
        arg = self.condition_kwarg(arg, kwarg)
        pub_data = self.run_job(tgt, fun, arg, expr_form, ret,
                                timeout, **kwargs)

        if not pub_data:
            return []
        elif expr_form in ('glob', 'pcre', 'list'):
            return pub_data['minions']
        else:
            raise SaltInvocationError(
                'expanded minion list unavailable for expr_form {0}'.format(
                    expr_form
                )
            )

    def mycp(self, tgt, filename, dest, timeout=None):
        '''
        read contents of file, use salt cprecv to grab those and
        create the destination file with the same contents

        obviously this does not preserve date/timestamps
        '''
        if not os.path.isfile(filename):
            sys.stderr.write("copy of %s to %s on %s failed\n"
                             % (filename, dest, tgt))
            return None
        with salt.utils.fopen(filename, 'r') as fp_:
            data = fp_.read()
        file_dict = {filename: data}
        arg = [file_dict, dest]
        return self.cmd(tgt, 'cp.recv', arg, timeout,
                        expr_form='glob')


class Dirdate(object):
    '''
    handle date strings in directory names, used for
    prep/staging area primarily
    '''
    def __init__(self, date=None):
        self.date = self.check_date(date)

    def check_date(self, date):
        if date is None:
            print "No date specified, using today's date"
            date = time.strftime("%b-%d-%Y", time.gmtime(time.time()))
            return date[0].lower() + date[1:]
        else:
            # check the user's date for sanity
            date_regexp = ('^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)'
                           '-[0-9][0-9]-20[0-9][0-9]$')
            if not re.match(date, date_regexp):
                usage(None, "Bad format for datestring; expecting mon-dd-yyyy,"
                      " example: mar-12-2012")
            return date


class Conf(object):
    '''
    manage configuration dict from python file
    '''
    def __init__(self, filename):
        self.filename = filename
        self.services = []
        self.all_services = []
        self.cf = None
        if filename:
            try:
                self.cf = runpy.run_path(conf_file)['conf']
            except IOError:
                sys.stderr.write("Failed to read config file %s\n" % conf_file)
                return
            self.all_services = self.cf['services'].keys()
            self.check_conf()

    def check_conf(self):
        if 'prepdirbase' not in self.cf:
            usage(None, "missing prepdirbase in config setup")
        if 'targetbase' not in self.cf:
            usage(None, "missing targetbase in config setup")
        if 'repo' not in self.cf:
            usage(None, "missing repo in config setup")

    def check_conf_services(self, services_requested):
        if services_requested == ['all']:
            self.services = self.all_services
        else:
            services_found = [s for s in services_requested
                              if s in self.cf['services']]
            if len(services_found) != len(services_requested):
                usage(None, "service named not listed in config setup")
            self.services = list(set(services_found))
        for s in self.services:
            if ('files' in self.cf['services'][s]
                    and 'destdir' not in self.cf['services'][s]):
                usage(None, "files specified for service %s but no destdir"
                      % s)


class Prep(object):
    '''
    set up/manage files in prep/staging area
    '''
    def __init__(self, conf, date=None):
        self.c = conf
        self.date = Dirdate(date)
        self.prepdirbase = self.c.cf['prepdirbase']
        self.prepdir = None
        self.targetbase = self.c.cf['targetbase']
        self.repo = self.c.cf['repo']

    def get_repo_fileinfo(self, service):
        return self.c.cf['services'][service]['files']

    def get_repo_filenames(self, service):
        result = []
        files = self.c.cf['services'][service]['files']
        for finfo in files:
            if 'path' in files[finfo]:
                result.append({'name': finfo, 'path': files[finfo]['path']})
            else:
                result.append({'name': finfo, 'path': None})
        return result

    def make_prepdir(self, service):
        errs = 0
        self.prepdir = os.path.join(self.prepdirbase, service, self.date.date)
        if os.path.isdir(self.prepdir):
            result = raw_input("directory %s" % self.prepdir +
                               " already exists, are you sure? y/n: ")
            if not result.startswith('y'):
                print "exiting at user request"
            sys.exit(1)
        else:
            try:
                os.makedirs(self.prepdir)
            except:
                sys.stderr.write("failed to make prep dir %s\n" % self.prepdir)
                errs += 1

        return errs

    def copy_files(self, service):
        errs = 0
        files = self.get_repo_filenames(service)
        for f in files:
            if f['path'] is not None:
                copyme = os.path.join(self.repo, f['path'], f['name'])
            else:
                copyme = os.path.join(self.repo, f['name'])

            try:
                open(os.path.join(self.prepdir, f['name']), "w").write(
                    open(copyme).read())
            except:
                sys.stderr.write("failed to copy file %s to prepdir\n"
                                 % f['name'])
                errs += 1
        return errs

    def check_repo_files(self, service):
        # make sure they exist and are regular files
        errs = 0
        files = self.get_repo_filenames(service)
        for f in files:
            if f['path'] is not None:
                full_path = os.path.join(self.repo, f['path'], f['name'])
            else:
                full_path = os.path.join(self.repo, f['name'])
            if not os.path.isfile(full_path):
                sys.stderr.write("%s is not a file\n" % full_path)
                errs += 1
            elif os.path.islink(full_path):
                sys.stderr.write("Symlink %s cannot be processed\n"
                                 % full_path)
                errs += 1
        return errs

    def prepare(self):
        errs = 0
        for s in self.c.services:
            errs += self.check_repo_files(s)
        if errs:
            sys.exit(1)

        for s in self.c.services:
            print "prepping for", s
            if self.make_prepdir(s):
                sys.exit(1)
            if self.copy_files(s):
                sys.exit(1)
            print "prepped in", self.prepdir, "done"


class Deploy(object):
    '''
    deploy files from local prep dir to remote host staging area
    and from there to final location
    '''
    def __init__(self, conf, hostexpr, date):
        self.c = conf
        self.deploy_hosts = hostexpr
        self.date = Dirdate(date)
        self.prepdirbase = self.c.cf['prepdirbase']
        self.prepdir = None
        self.repo = self.c.cf['repo']
        self.targetbase = self.c.cf['targetbase']
        self.salt = LocalClientPlus()
        self.expanded_deploy_hosts = self.salt.cmd_expandminions(
            self.deploy_hosts, "test.ping", expr_form='glob')

    def check_local_prepdir(self, service):
        self.prepdir = os.path.join(self.prepdirbase, service, self.date.date)
        if not os.path.isdir(self.prepdir):
            sys.stderr.write("prepdir %s does not exist or is not"
                             " a directory, giving up\n" % self.prepdir)
            sys.exit(1)

    def check_missing_hosts(self, hosts_responding):
        errs = 0
        for h in self.expanded_deploy_hosts:
            if h not in hosts_responding:
                sys.stderr.write("Host %s failed to respond\n" % h)
                errs += 1
        return errs

    def get_repo_fileinfo(self, service):
        return self.c.cf['services'][service]['files']

    def get_repo_filenames(self, service):
        result = []
        files = self.c.cf['services'][service]['files']
        for finfo in files:
            if 'path' in files[finfo]:
                result.append({'name': finfo,
                               'path': files[finfo]['path']})
            else:
                result.append({'name': finfo, 'path': None})
        return result

    def salt_make_remote_dir(self, dirname):
        errs = 0
        result = self.salt.cmd(self.deploy_hosts, "cmd.run_all",
                               ["mkdir -p " + dirname], expr_form='glob')
        hosts_responding = []
        for h in result:
            hosts_responding.append(h)
            if result[h]['retcode']:
                sys.stderr.write("couldn't create directory %s on %s\n"
                                 % (dirname, h))
                errs += 1
        if self.check_missing_hosts(hosts_responding):
            sys.stderr.write("couldn't create directory on hosts" +
                             " failing to respond\n")
            errs += 1

        return errs

    def get_file_md5s(self, dirname, files, callback=None):
        output = []
        for f in files:
            md5out = None
            try:
                md5out = hashlib.md5(open(os.path.join(
                    dirname, f)).read()).hexdigest().strip()
            except:
                md5out = None
            if not md5out:
                sys.stderr.write("failed to get md5 of %s\n" % f)
                return None
            if callback is not None:
                output.append([md5out, callback(f)])
            else:
                output.append([md5out, f])
        return output

    def get_md5s_ok_count(self, text):
        return len([line for line in text.split('\n')
                    if line.endswith(': OK')])

    def check_file_md5s(self, service, local_dir):
        errs = 0
        files = self.get_repo_fileinfo(service)
        md5s = self.get_file_md5s(local_dir, files, self.get_tmp_filename)
        destdir = self.c.cf['services'][service]['destdir']
        # note that md5 needs two spaces between fields. no exceptions.
        md5s_to_check = ('\n'.join([
            m[0] + "  " + os.path.join(self.targetbase, destdir, m[1])
            for m in md5s]) + '\n')
        result = self.salt.cmd(self.deploy_hosts, "cmd.run_all",
                               ["/bin/echo -e -n '%s' |  md5sum -c -w"
                                % md5s_to_check],
                               expr_form='glob')

        hosts_responding = []
        for h in result:
            hosts_responding.append(h)
            if (result[h]['retcode'] or
                'stdout' not in result[h] or
                ('stdout' in result[h] and
                 ('did NOT match' in result[h]['stdout'] or
                  self.get_md5s_ok_count(result[h]['stdout']) !=
                  len(md5s)))):
                sys.stderr.write("%s: bad file copy\n" % h)
                if 'stdout' in result[h]:
                    sys.stderr.write(result[h]['stdout'] + "\n")
                if 'stderr' in result[h]:
                    sys.stderr.write(result[h]['stderr'] + "\n")
                errs += 1
        errs += self.check_missing_hosts(hosts_responding)
        return errs

    def check_deploy(self, service):
        errs = self.check_file_md5s(service, self.prepdir)
        return errs

    def salt_copy_file(self, filename, destpath):
        errs = 0
        result = self.salt.mycp(self.deploy_hosts, filename,
                                destpath)
        if result is None:
            sys.stderr.write("couldn't copy file %s to any hosts, giving up\n"
                             % filename)
            sys.exit(1)

        hosts_responding = []
        for h in result:
            hosts_responding.append(h)
            if destpath not in result[h] or result[h][destpath] is not True:
                if 'stderr' in result[h]:
                    sys.stderr.write(result[h]['stderr'] + "\n")
                sys.stderr.write("couldn't copy file %s to %s on %s\n"
                                 % (filename, destpath, h))
                errs += 1
        if self.check_missing_hosts(hosts_responding):
            sys.stderr.write("couldn't copy file %s to hosts" +
                             " failing to respond\n"
                             % filename)
            errs += 1

        return errs

    def salt_move_file(self, source_filename, dest_filename, destdir):
        errs = 0
        result = self.salt.cmd(self.deploy_hosts, "cmd.run_all",
                               ["mv %s %s"
                                % (os.path.join(destdir, source_filename),
                                   os.path.join(destdir, dest_filename))],
                               expr_form='glob')
        if result is None:
            sys.stderr.write("couldn't move file %s on any hosts, giving up\n"
                             % source_filename)
            sys.exit(1)

        hosts_responding = []
        for h in result:
            hosts_responding.append(h)
            if result[h]['retcode']:
                if 'stderr' in result[h]:
                    sys.stderr.write(result[h]['stderr'] + "\n")
                sys.stderr.write("couldn't move file %s to %s on %s\n"
                                 % (os.path.join(destdir, source_filename),
                                    os.path.join(destdir, dest_filename),
                                    h))
                errs += 1
        if self.check_missing_hosts(hosts_responding):
            sys.stderr.write("couldn't move file %s on hosts" +
                             " failing to respond\n",
                             source_filename)
            errs += 1

        return errs

    def set_file_mode(self, filename, destdir, mode):
        errs = 0
        result = self.salt.cmd(self.deploy_hosts, "cmd.run_all",
                               ["chmod %s %s"
                                % (mode, os.path.join(self.targetbase,
                                                      destdir, filename))],
                               expr_form='glob')
        hosts_responding = []
        for h in result:
            hosts_responding.append(h)
            if result[h]['retcode']:
                if 'stderr' in result[h]:
                    sys.stderr.write(result[h]['stderr'] + '\n')
                    sys.stderr.write("couldn't chmod file %s on %s\n"
                                     % (filename, h))
                    errs += 1
        if self.check_missing_hosts(hosts_responding):
            sys.stderr.write("couldn't chmod file %s on hosts" +
                             "failing to respond\n" % filename)
            errs += 1
        return errs

    def get_tmp_filename(self, filename):
        return filename + "_tmp"

    def deploy_file_for_service_docopy(self, service, filename):
        errs = 0
        destdir = self.c.cf['services'][service]['destdir']
        errs += self.salt_copy_file(
            os.path.join(self.prepdir, filename),
            os.path.join(self.targetbase, destdir,
                         self.get_tmp_filename(filename)))
        if 'mode' in self.c.cf['services'][service]['files'][filename]:
            mode = self.c.cf['services'][service]['files'][filename]['mode']
            errs += self.set_file_mode(self.get_tmp_filename(filename),
                                       destdir, mode)
        return errs

    def deploy_file_for_service_domove(self, service, filename):
        errs = 0
        destdir = self.c.cf['services'][service]['destdir']
        errs += self.salt_move_file(self.get_tmp_filename(filename),
                                    filename, os.path.join(self.targetbase,
                                                           destdir))
        return errs

    def check_local_prepdir_contents(self, files, service):
        for f in files:
            if not os.path.exists(os.path.join(self.prepdir, f['name'])):
                sys.stderr.write("missing file %s in %s for deploy of %s ,"
                                 % (f['name'], self.prepdir, service) +
                                 " giving up\n")
                sys.exit(1)

    def salt_update_release(self, service):
        release_file = os.path.join(self.targetbase, service,
                                    "hackdeploy_RELEASE.txt")
        errs = 0
        result = self.salt.cmd(self.deploy_hosts, "cmd.run_all",
                               ["/bin/echo " + self.date.date +
                                " > " + release_file], expr_form='glob')
        hosts_responding = []
        for h in result:
            hosts_responding.append(h)
            if result[h]['retcode']:
                sys.stderr.write("couldn't update release info on %s\n", h)
                errs += 1
        if self.check_missing_hosts(hosts_responding):
            sys.stderr.write("couldn't update release info on" +
                             " hosts failing to respond\n")
            errs += 1

        return errs

    def deploy_service(self, service):
        errs = 0
        self.check_local_prepdir(service)
        files = self.get_repo_filenames(service)
        self.check_local_prepdir_contents(files, service)

        # destdir on remote
        if self.salt_make_remote_dir(os.path.join(
                self.targetbase, self.c.cf['services'][service]['destdir'])):
            sys.exit(1)

        print "deploying %s" % service + " to hosts (doing copies): ",
        print ", ".join(self.expanded_deploy_hosts)
        for f in files:
            errs += self.deploy_file_for_service_docopy(service, f['name'])

        if errs:
            sys.stderr.write("giving up\n")
            sys.exit(1)

        errs += self.check_deploy(service)
        if errs:
            sys.stderr.write("giving up\n")
            sys.exit(1)

        print "deploying %s" % service + " to hosts (doing moves): ",
        print ", ".join(self.expanded_deploy_hosts)
        for f in files:
            errs += self.deploy_file_for_service_domove(service, f['name'])

        if errs:
            sys.stderr.write("giving up\n")
            sys.exit(1)

        print "updating RELEASE"
        self.salt_update_release(service)

        print "done!"
        return 0

    def deploy(self):
        errs = 0
        for s in self.c.services:
            errs += self.deploy_service(s)
        return errs


def usage(conf_file, message=None):
    services_known = """<service> should be one of the services specified
in the configuration file.
"""
    if conf_file:
        c = Conf(conf_file)
        if c is not None and c.cf is not None:
            services_known = ("Reading config file: %s\n\n" % conf_file +
                              "<service> may be one of the following:\n")
            for s in c.all_services:
                if 'description' in c.cf['services'][s]:
                    descr = c.cf['services'][s]['description']
                else:
                    descr = 'No description available'
                services_known += "    %s %s\n" % (
                    s.ljust(30), descr)
            services_known += "    %s all of the above\n" % "all".ljust(30)

    if message:
        sys.stderr.write(message + "\n")
    usage_message = ("""Usage: hack-deploy.py <service> prep [mon-dd-yyyy]
    or  hack-deploy.py <service> deploy hosts-expr [mon-dd-yyyy]
    or hack-deploy.py --help
""" + services_known + """
prep        -- prepare a directory with the specified or today's
               date for use for deployment, prepopulating it with
               the appropriate files from the git repo

deploy      -- copy the files in the prep dir of the specified or
               today's date to the appropriate destination dir on
               the snapshot hosts

hosts-expr  -- expression recognized by salt which will be
               expanded to a list of deployment hosts; e.g.
               'snapshot100*' (quote it if needed)

date format: monthabbrev-dd-yyyy
             where month abbrevs are jan feb mar apr may jun jul aug
             sep oct nov dec (and this way no one has to worry about
             whether the month or the day comes first, it's obvious)

hack-deploy.py --help prints this usage message

""")
    sys.stderr.write(usage_message)
    sys.exit(1)


if __name__ == '__main__':
    conf_file = os.path.join(os.path.dirname(sys.argv[0]),
                             'hackdeploy.conf')
    prepdir_date = None

    if len(sys.argv) < 2:
        usage(conf_file)

    if (sys.argv[1] == '--help' or
            sys.argv[1] == '-h' or
            sys.argv[1] == 'help'):
        usage(conf_file, "Help for hack deploy script\n")

    if len(sys.argv) < 3:
        usage(conf_file)

    if sys.argv[2] == 'deploy':
        if len(sys.argv) < 4:
            usage(conf_file, "Missing arguments for 'deploy' option")

        deploy_hosts = sys.argv[3]
        if len(sys.argv) > 4:
            prepdir_date = sys.argv[4]
        if len(sys.argv) > 5:
            conf_file = sys.argv[5]

    elif sys.argv[2] == 'prep':
        if len(sys.argv) < 3:
            usage(conf_file, "Missing arguments for 'prep' option")

        if len(sys.argv) > 3:
            prepdir_date = sys.argv[3]
        if len(sys.argv) > 4:
            conf_file = sys.argv[4]

    else:
        usage(conf_file, "One of deploy or prep must be specified.")

    cf = Conf(conf_file)
    services = sys.argv[1].split(',')
    cf.check_conf_services(services)

    if sys.argv[2] == 'prep':
        pr = Prep(cf, prepdir_date)
        errors = pr.prepare()
    elif sys.argv[2] == 'deploy':
        dp = Deploy(cf, deploy_hosts, prepdir_date)
        errors = dp.deploy()

    if errors:
        sys.stderr.write("Errors encountered\n")
        sys.exit(1)
