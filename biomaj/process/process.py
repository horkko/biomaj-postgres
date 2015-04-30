import logging
import os
import subprocess
import tempfile

class Process(object):
  '''
  Define a process to execute
  '''

  def __init__(self, name, exe, args, desc=None, proc_type=None, expand=True, bank_env=None, log_dir=None):
    '''
    Define one process

    :param name: name of the process (descriptive)
    :type name: str
    :param exe: path to the executable (relative to process.dir or full path)
    :type exe: str
    :param args: arguments
    :type args: str
    :param desc: process description
    :type desc: str
    :param proc_type: types of data generated by process
    :type proc_type: str
    :param expand: allow shell expansion on command line
    :type expand: bool
    :param bank_env: environnement variables to set
    :type bank_env: list
    :param log_dir: directroy to place process stdout and stderr
    :type log_dir: str
    '''
    # Replace env vars in args
    if args:
      for key,value in bank_env.iteritems():
        if value is not None:
          args = args.replace('${'+key+'}', value)

    self.name = name
    self.exe = exe
    self.desc= desc
    if args is not None:
      self.args = args.split()
    else:
      self.args = []
    self.bank_env = bank_env
    self.type = proc_type
    self.expand = expand
    if log_dir is not None:
      self.output_file = os.path.join(log_dir,name+'.out')
      self.error_file = os.path.join(log_dir,name+'.err')
    else:
      self.output_file = name+'.out'
      self.error_file = name+'.err'

    self.types = ''
    self.format = ''
    self.tags = ''
    self.files = ''

  def run(self, simulate=False):
    '''
    Execute process

    :param simulate: does not execute process
    :type simulate: bool
    :return: exit code of process
    '''
    args = [ self.exe ] + self.args
    logging.debug('PROCESS:EXEC:'+str(self.args))
    err= False
    if not simulate:
      logging.info('PROCESS:RUN:'+self.name)
      with open(self.output_file,'w') as fout:
        with open(self.error_file,'w') as ferr:
          if self.expand:
            args = " ".join(args)
            proc = subprocess.Popen(args, stdout=fout, stderr=ferr, env=self.bank_env, shell=True)
          else:
            proc = subprocess.Popen(args, stdout=fout, stderr=ferr, env=self.bank_env, shell=False)
          proc.wait()
          if proc.returncode == 0:
            err = True
          else:
            logging.error('PROCESS:ERROR:'+self.name)
          fout.flush()
          ferr.flush()
    else:
      err = True
    logging.info('PROCESS:EXEC:' + self.name + ':' + str(err))

    return err

class DockerProcess(Process):
  def __init__(self, name, exe, args, desc=None, proc_type=None, docker=None, expand=True, bank_env=None, log_dir=None, use_sudo=True):
    Process.__init__(self, name, exe, args, desc, proc_type, expand, bank_env, log_dir)
    self.docker = docker
    self.use_sudo = use_sudo

  def run(self, simulate=False):
    '''
    Execute process in docker container

    :param simulate: does not execute process
    :type simulate: bool
    :return: exit code of process
    '''
    use_sudo = ''
    if self.use_sudo:
      use_sudo = 'sudo'
    release_dir = self.bank_env['datadir']+'/'+self.bank_env['dirversion']+'/'+self.bank_env['localrelease']
    env = ''
    if self.bank_env:
      for key, value in self.bank_env.iteritems():
        env += ' -e "{0}={1}"'.format(key, value)
    #         docker run with data.dir env as shared volume
    #         forwarded env variables
    cmd = '''uid={uid}
gid={gid}
{sudo} docker pull {container_id}
{sudo} docker run --rm -w {bank_dir}  -v {data_dir}:{data_dir} {env} {container_id} \
bash -c "groupadd --gid {gid} {group_biomaj} && useradd --uid {uid} --gid {gid} {user_biomaj}; \
{exe} {args}; \
chown -R {uid}:{gid} {bank_dir}"'''.format(uid = os.getuid(),
                                          gid = os.getgid(),
                                          data_dir = self.bank_env['datadir'],
                                          env = env,
                                          container_id = self.docker,
                                          group_biomaj = 'biomaj',
                                          user_biomaj = 'biomaj',
                                          exe = self.exe,
                                          args = ' '.join(self.args),
                                          bank_dir=release_dir,
                                          sudo=use_sudo
                                          )

    (handler, tmpfile) = tempfile.mkstemp('biomaj')
    os.write(handler,cmd)
    os.close(handler)
    os.chmod(tmpfile, 0755)
    args = [ tmpfile ]
    logging.debug('PROCESS:EXEC:Docker:'+str(self.args))
    logging.debug('PROCESS:EXEC:Docker:Tmpfile:'+tmpfile)
    err= False
    if not simulate:
      logging.info('PROCESS:RUN:Docker:'+self.docker+':'+self.name)
      with open(self.output_file,'w') as fout:
        with open(self.error_file,'w') as ferr:
          if self.expand:
            args = " ".join(args)
            proc = subprocess.Popen(args, stdout=fout, stderr=ferr, env=self.bank_env, shell=True)
          else:
            proc = subprocess.Popen(args, stdout=fout, stderr=ferr, env=self.bank_env, shell=False)
          proc.wait()
          if proc.returncode == 0:
            err = True
          else:
            logging.error('PROCESS:ERROR:'+self.name)
          fout.flush()
          ferr.flush()
    else:
      err = True
    logging.info('PROCESS:EXEC:' + self.name + ':' + str(err))
    os.remove(tmpfile)
    return err


class DrmaaProcess(Process):
  def __init__(self, name, exe, args, desc=None, proc_type=None, native=None, expand=True, bank_env=None, log_dir=None):
    Process.__init__(self, name, exe, args, desc, proc_type, expand, bank_env, log_dir)
    self.native = native


  def run(self, simulate=False):
    '''
    Execute process

    :param simulate: does not execute process
    :type simulate: bool
    :return: exit code of process
    '''
    args = [ self.exe ] + self.args
    logging.debug('PROCESS:EXEC:'+str(self.args))
    err= False
    if not simulate:
      logging.info('Run process '+self.name)
      # Execute on DRMAA
      try:
        import drmaa
        with drmaa.Session() as s:
          jt = s.createJobTemplate()
          jt.remoteCommand = self.exe
          jt.args = self.args
          jt.joinFiles=False
          jt.workingDirectory = os.path.dirname(os.path.realpath(self.output_file))
          jt.jobEnvironment = self.bank_env
          if self.native:
            jt.nativeSpecification = " "+self.native+" "
          jt.outputPath = self.output_file
          jt.errorPath = self.error_file
          jobid = s.runJob(jt)
          retval = s.wait(jobid, drmaa.Session.TIMEOUT_WAIT_FOREVER)
          if  retval.hasExited > 0:
            err = True
          else:
            logging.error('PROCESS:ERROR:'+self.name)
          s.deleteJobTemplate(jt)

      except Exception as e:
        logging.error('Drmaa process error: '+str(e))
        return False
    else:
      err = True
    logging.info('PROCESS:EXEC:' + self.name + ':' + str(err))

    return err
