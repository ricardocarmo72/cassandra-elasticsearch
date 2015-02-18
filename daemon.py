#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys, os, time, atexit
from signal import SIGTERM 

class Daemon:
    """
    Uma classe daemon genérica.
    
    Créditos: Sander Marechal
    Página: http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
    """
    
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
    
    def daemonize(self):
        """
        Faz o UNIX double-fork. Para detalhes, veja 
        "Advanced Programming in the UNIX Environment", de Stevens, (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit first parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #1 falhou: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
    
        #os.chdir("/") 
        os.setsid() 
        os.umask(0) 
    
        # faz o segundo fork
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #2 falhou: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1) 
    
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
    
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)
    
    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        """
        Inicia o daemon
        """
        # Checa o pidfile para ver se o daemon já está rodando
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if pid:
            message = "pidfile %s já existe. Daemon já está rodando?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)
        
        self.daemonize()
        self.run()

    def stop(self):
        """
        Interrompe o daemon
        """
        # Obtém o pid do pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if not pid:
            message = "pidfile %s não existe. Daemon está rodando?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Tenta matar o processo daemon    
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
        Reinicia o daemon
        """
        self.stop()
        self.start()

    def run(self):
        """
        Método sobreescrito no programa synchronize.py 
        """
