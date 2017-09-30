#!/usr/bin/python3

# Copyright (c) 2016, University of Freiburg,
# Chair of Algorithms and Data Structures.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of the FreeBSD Project.

import copy
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import yaml

verbose = True

class SubprocessHelper(object):
    """ A helper class to provide various advanced functions which the
        standard subprocess module of Python doesn't provde, like easy
        PTY-wrapping or a check_output variant which still outputs to stdout
        while the command runs, while still collecting the output to a string
        in the end.

        This code is based on https://github.com/JonasT/drudder
    """
    @staticmethod
    def check_output_with_stdout_copy(cmd, shell=False, stdout=None,
            stderr=None, cwd=None, env=None, pty=False, write_function=None):
        """ Like subprocess.check_output, but also copies the stdout/stderr to
            sys.stdout/sys.stderr in realtime while the process runs while
            still returning the full output as a return value after
            termination and while still raising subprocess.CalledProcessError
            when encountering a process exit code other than 0.

            Optional parameters for extended behavior:

            Use pty=True for optional pseudo-TTY wrapping on Unix to make the
            launched command think it runs in an interactive terminal (this
            will also remove any sort of stdout/stdin buffering or delay). All
            other behavior remains the same.

            Use write_function to specify a custom write function in addition
            to sys.stdout/sys.stderr copying which can process the output in
            realtime in some way. If you want to abort the process from inside
            the write function, just raise an exception. If that happens,
            check_output_with_stdout_copy will raise a RuntimeError instead of
            the regular subprocess.CalledProcessError to indicate an abort
            caused by a write_function error.
        """
        return SubprocessHelper._check_output_extended(cmd, shell=shell,
            stdout=stdout, stderr=stderr, cwd=cwd, env=env, pty=pty,
            write_function=write_function, copy_to_regular_stdout_stderr=True)

    @staticmethod
    def _check_output_extended(cmd, shell=False, stdout=None, stdin=None,
            stderr=None, cwd=None, env=None, pty=False, write_function=None,
            copy_to_regular_stdout_stderr=True,
            process_handle_callback=None):
        # Get actual stdout/stderr as to be used internally:
        actual_stdout = stdout
        if stdout == None or stdout == sys.stderr:
            actual_stdout = subprocess.PIPE
        actual_stderr = stderr
        if stderr == None or stderr == subprocess.STDOUT or \
                stderr == sys.stderr:
            actual_stderr = subprocess.PIPE

        # Handle various other parameters:
        if cwd == None:
            cwd = os.getcwd()
        if env == None:
            env = os.environ.copy()

        # Launch command:
        if not pty:
            process = subprocess.Popen(cmd, shell=shell,
                stdout=actual_stdout, stderr=actual_stderr,
                stdin=stdin, cwd=cwd, env=env)
        else:
            process = subprocess.Popen([sys.executable,
                "-c", "import pty\nimport sys\n" +\
                "exit_code = pty.spawn(sys.argv[1:])\n" +\
                "sys.exit((int(exit_code) & (255 << 8)) >> 8)"] + cmd,
                shell=shell,
                stdout=actual_stdout, stderr=actual_stderr, stdin=stdin,
                cwd=cwd, env=env)
        if process_handle_callback != None:
            process_handle_callback(process)

        # Output reader thread definition:
        class OutputReader(threading.Thread):
            def __init__(self, process, fileobj, writer, copy_to=None):
                super().__init__()
                self.process = process
                self.fileobj = fileobj
                self.writer_func = writer
                self.copy_to = copy_to

            def run(self):
                if self.fileobj == None:
                    return
                while True:
                    try:
                        try:
                            c = self.fileobj.read(1)
                        except ValueError:
                            raise OSError("file object invalid")

                        # Make sure it is bytes:
                        try:
                            c = c.encode("utf-8", "replace")
                        except AttributeError:
                            pass

                        # Write to copy file handle if present:
                        if self.copy_to != None:
                            try:
                                self.copy_to.write(c)
                            except TypeError:
                                self.copy_to.write(c.decode(
                                    "utf-8", "replace"))
                            self.copy_to.flush()

                        # Process writer func:
                        if self.writer_func != None:
                            self.writer_func(c)
                    except OSError:
                        break

        # Launch output readers as required and collect data:
        stdout_reader = None
        stderr_reader = None
        presented_output_store = {
            "data" : b"",
            "user_write_error" : None,
        }
        def writer(data):
            """ This function gets called by the thread to store data from
                stdin to be returned or used later on.
            """
            if len(data) == 0:
                return
            try:
                # Call user write function:
                if write_function != None:
                    write_function(data)
            except Exception as e:
                # Propagate error if the user function caused an error:
                presented_output_store["user_write_error"] = str(e)
                try:
                    process.kill()
                except Exception:
                    pass
            presented_output_store["data"] += data
        if actual_stdout == subprocess.PIPE:  # we need to collect stdout.
            if copy_to_regular_stdout_stderr:
                copy_to = sys.stdout
            else:
                copy_to = None
            stdout_reader = OutputReader(process, process.stdout, writer,
                copy_to=copy_to)
            stdout_reader.start()
        if actual_stderr == subprocess.PIPE:  # we need to collect stderr.
            if copy_to_regular_stdout_stderr:
                copy_to = sys.stderr
            else:
                copy_to = None
            if stderr == subprocess.STDOUT:
                stderr_reader = OutputReader(process, process.stderr, writer,
                    copy_to=sys.stdout)
            else:
                stderr_reader = OutputReader(process, process.stderr, None,
                    copy_to=copy_to)
            stderr_reader.start()

        # Wait for process to end:
        process.wait()
        try:
            process.stdout.close()
        except Exception:
            pass
        try:
            process.stderr.close()
        except Exception:
            pass
        try:
            process.kill()
        except Exception:
            pass

        # Evaluate whether it ran successfully:
        exit_code = process.returncode
        got_data = None
        if presented_output_store["user_write_error"] != None:
            raise RuntimeError("specified write_function " +\
                "encountered an error: " +\
                str(presented_output_store["user_write_error"]))
        if stdout_reader != None or (stderr_reader != None and \
                stderr == subprocess.STDOUT):
            got_data = presented_output_store["data"]
        if exit_code != 0:
            new_error = subprocess.CalledProcessError(
                cmd=cmd, returncode=exit_code)
            new_error.output = got_data
            raise new_error

        return got_data

    @staticmethod
    def check_output_with_isolated_pty(cmd, shell=False, cwd=None,
            stdout=None, stderr=None, timeout=None, env=None):
        """ Like subprocess.check_output, but runs the process in a pseudo-TTY
            using python's pty module.
        """
        return SubprocessHelper._check_output_extended(
            cmd, shell=shell, cwd=cwd, env=env, stdout=stdout, stderr=stderr,
            pty=True, copy_to_regular_stdout_stderr=False)

class Remote(object):
    def __init__(self, url, auth_info, name="origin", verbose=False):
        self.show_debug = False
        self.verbose = verbose

        if not type(auth_info) == dict:
            raise RuntimeError("invalid auth info: " +
                "dictionary expected")
        self.url = url
        if self.url.startswith("scp://") or self.url.startswith("ssh://"):
            self.url = self.url.partition("//")[2]
        self.auth = auth_info
        self.name = name

        # Insert username into https:// urls if required for login:
        if self.url.startswith("https://"):
            uname = self._relevant_auth_username()
            if uname != None and self.url.find("@") < 0:
                self.url = ("https://" + uname + "@" +
                    self.url[len("https://"):])
            pw = self._relevant_auth_password()
            if pw != None and self.url.find("@") > self.url.find(":"):
                self.url = ("https://" + self.url.partition("@")[0][
                    len("https://"):] +
                    ":" + pw + "@" + self.url.partition("@")[2])

    def _relevant_auth_username(self):
        def uname():
            if not "type" in self.auth:
                return None
            if not "info" in self.auth:
                return None
            if self.auth["type"] == "anonymous":
                return None
            if not "user" in self.auth["info"]:
                return None
            return self.auth["info"]["user"]
        return uname()

    def _relevant_auth_password(self):
        def pw():
            if not "type" in self.auth:
                return None
            if not "info" in self.auth:
                return None
            if self.auth["type"] == "anonymous":
                return None
            if self.auth["type"] != "password":
                return None
            if not "password" in self.auth["info"]:
                return None
            return self.auth["info"]["password"]
        return pw()

    def set_repo(self, repo):
        self.repo = repo
        if self.verbose:
            print("verbose: shell cmd: ['git', 'remote']",
                file=sys.stderr, flush=True)
        remote_list_output = subprocess.check_output(["git", "remote"],
            cwd=self.repo.repo_dir)
        lines = [line.strip().decode("utf-8", "replace")\
            for line in remote_list_output.replace(
            b"\r\n", b"\n").split(b"\n") if len(line.strip()) > 0]
        remotes = [line.partition(" ")[0] for line in lines]
        if self.name in remotes:
            # Nothing to do, remote was already added.
            return
        if self.verbose:
            print("verbose: shell cmd: ['git', 'remote', 'add', '" +
                self.name + "', '" + self.url + "']",
                file=sys.stderr, flush=True)
        subprocess.check_output(["git", "remote", "add", self.name,
            self.url], cwd=self.repo.repo_dir)
        if self.verbose:
            print("verbose: shell cmd: ['git', 'remote', 'update']",
                file=sys.stderr, flush=True)
        subprocess.check_output(["git", "remote", "update"],
            cwd=self.repo.repo_dir)

    def _run_authed_git_command(self, command, binary="git",
            pw_arg_no=None):
        try:
            if verbose:
                l = command
                if pw_arg_no != None and len(pw_arg_no) >= 0 and \
                        len(pw_arg_no) < len(command):
                    l = copy.copy(command)
                    l[pw_arg_no] = "<retracted>"
                print("verbose: shell cmd: " + str([binary] + l),
                    file=sys.stderr, flush=True)
            if not "type" in self.auth:
                raise RuntimeError("invalid auth info: no \"type\" specified")
            if self.auth["type"] == "anonymous":
                output = SubprocessHelper._check_output_extended(
                    [binary] + command, pty=True,
                    copy_to_regular_stdout_stderr=self.show_debug,
                    cwd=self.repo.repo_dir, stderr=subprocess.STDOUT)
                return output
            elif self.auth["type"] == "password":
                if not "info" in self.auth:
                    raise RuntimeError("invalid auth info: password auth " +
                        "requires \"info\" entry with " +
                        "\"password\" specified " +
                        "inside")
                if not type(self.auth["info"]) == dict:
                    raise RuntimeError("invalid auth info: \"info\" " +
                        "entry not a dictionary as expected")
                if not "password" in self.auth["info"]:
                    raise RuntimeError("invalid auth info: password auth " +
                        "requires \"password\" specified " +
                        "inside the \"info\" dictionary")
                if not type(self.auth["info"]["password"]) == str:
                    raise RuntimeError("invalid auth info: " +
                        "specified password must be a string")

                output = SubprocessHelper._check_output_extended(
                    ["git"] + command, cwd=self.repo.repo_dir,
                    stderr=subprocess.STDOUT, pty=True,
                    copy_to_regular_stdout_stderr=self.show_debug)
                return output
            elif self.auth["type"] == "public_key":
                raise RuntimeError("not implemented yet")
            else:
                raise RuntimeError("unknown auth type")
        except subprocess.CalledProcessError as e:
            print("subprocess.CalledProcessError: an error just occured " +
                "in _run_authed_git_command.", file=sys.stderr, flush=True)
            output = e.output
            try:
                output = output.decode("utf-8", "replace")
            except AttributeError:
                pass
            print("subprocess.CalledProcessError: failed command " +
                "had this output:\n\n" + output, file=sys.stderr,
                flush=True)
            e.output = output
            raise e
                

    def branch(self):
        # Fetch newest remote branches:
        try:
            if self.verbose:
                print("verbose: shell cmd: ['git', 'fetch', '" +
                    self.name + "']",
            file=sys.stderr, flush=True)
            SubprocessHelper._check_output_extended(
                ["git", "fetch", self.name], cwd=self.repo.repo_dir,
                stderr=subprocess.STDOUT, pty=True,
                copy_to_regular_stdout_stderr=self.show_debug)
        except subprocess.CalledProcessError as e:
            raise RuntimeError("unexpected failure of: " +
                "git fetch " + self.name + "  (repo: " +
                self.repo.repo_dir + ") OUTPUT: " + str(e.output))

        # List all branches:
        branches = set()
        branch_output = subprocess.check_output(
            ["git", "branch", "-a"],
            cwd=self.repo.repo_dir)
        try:
            branch_output = branch_output.decode("utf-8", "replace")
        except AttributeError:
            pass
        for line in branch_output.split("\n"):
            l = line.strip()
            if l.startswith("*"):
                l = l[1:]
            l = l.strip()
            if not l.startswith("remotes/" + self.name + "/"):
                continue
            branches.add(l.rpartition("/")[2])
        return branches

    def is_clean(self):
        s = subprocess.check_output([
            "git", "status", "-s"],
            cwd=self.repo.repo_dir)
        try:
            s = s.decode('utf-8', 'replace')
        except AttributeError:
            pass
        if len(s.strip()) > 0:
            return False
        return True

    def pull(self, lfs=False, branch="master", require_clean_status=False):
        # Fetch newest remote branches:
        if self.verbose:
            print("verbose: shell cmd: ['git', 'fetch', '" +
                self.name + "']",
                file=sys.stderr, flush=True)
        SubprocessHelper._check_output_extended(
            ["git", "fetch", self.name], cwd=self.repo.repo_dir,
            stderr=subprocess.STDOUT, pty=True,
            copy_to_regular_stdout_stderr=self.show_debug)

        # Make sure git status is clean:
        if require_clean_status and not self.is_clean():
            raise RuntimeError("required a clean 'git status' for " +
                "pull, but " +
                "'git status' is not clean! (check 1)")

        # Change to branch:
        try:
            if self.verbose:
                print("verbose: shell cmd: ['git', 'checkout', '" +
                    branch + "']   (branch change to: " +
                    branch + ")",
                    file=sys.stderr, flush=True)
            SubprocessHelper._check_output_extended(
                ["git", "checkout", branch], cwd=self.repo.repo_dir,
                stderr=subprocess.STDOUT, pty=True,
                copy_to_regular_stdout_stderr=self.show_debug)
        except subprocess.CalledProcessError as e:
            # Check for LFS error:
            output = e.output
            try:
                output = output.decode("utf-8", "replace")
            except AttributeError:
                pass
            if output.lower().find("error downloading object") >= 0:
                if not self.repo.lfs:
                    print("****************************************",
                        file=sys.stderr, flush=True)
                    print("error: !!! MISCONFIGURED REPO !!! " +
                        "LFS error was encountered, but repo was " +
                        "configured with lfs:false. Correct your config " +
                        "and recreate the gitmirror container, and " +
                        "this error should go away.",
                        file=sys.stderr, flush=True)
                    print("****************************************",
                        file=sys.stderr, flush=True)
                    raise e
                print("error: LFS object download failed during " +
                    "branch checkout, aborting by propagating " +
                    "subprocess.CalledProcessError...",
                    file=sys.stderr, flush=True)
                raise e

            # Make sure 'git status' is still clean if required:
            if require_clean_status and not self.is_clean():
                raise RuntimeError("required a clean 'git status' for " +
                    "pull, but " +
                    "'git status' is not clean! (check 2) " +
                    "Checkout command output is: " + str(e.output))

            # Try checkout with creating new branch:
            if self.verbose:
                print("verbose: shell cmd: ['git', 'checkout', '-b', '" +
                    branch + "']   (branch change to: " +
                    branch + ")",
                    file=sys.stderr, flush=True)
            o = SubprocessHelper._check_output_extended(
                ["git", "checkout", "-b", branch], cwd=self.repo.repo_dir,
                stderr=subprocess.STDOUT, pty=True,
                copy_to_regular_stdout_stderr=self.show_debug)
            
            # Make sure 'git status' is still clean if required:
            if require_clean_status and not self.is_clean():
                raise RuntimeError("required a clean 'git status' for " +
                    "pull, but " +
                    "'git status' is not clean! (check 3) " +
                    "Checkout command output is: " + str(o)) 
        assert(self.repo.current_branch == branch), \
            ("requiring branch to be '" + branch + "', but it is " +
            "actually '" + str(self.repo.current_branch) + "'")
        if self.verbose:
            print("verbose: branch is now " + str(branch),
                flush=True)

        # Make sure git status is clean (again):
        if require_clean_status and not self.is_clean():
            raise RuntimeError("required a clean 'git status' for " +
                "pull, but " +
                "'git status' is not clean! (check 4)") 

        # Pull branch:
        self._run_authed_git_command(["pull", self.name, branch])
        if self.repo.lfs:
            retry = True
            while retry:
                retry = False
                try:
                    self._run_authed_git_command(["fetch", "--all",
                        self.name],
                        binary="git-lfs")
                except subprocess.CalledProcessError as e:
                    if e.output.find("connection reset by peer") >= 0:
                        print("CONNECTION RESET BY PEER DETECTED, " +
                            "RETRYING...")
                        time.sleep(10)
                        retry = True 
            self._run_authed_git_command(["checkout", self.name, branch],
                binary="git-lfs")

    def push(self, branch="master"):
        # Change to branch:
        try:
            SubprocessHelper._check_output_extended(
                ["git", "checkout", branch], cwd=self.repo.repo_dir,
                stderr=subprocess.STDOUT, pty=True,
                copy_to_regular_stdout_stderr=self.show_debug)
        except subprocess.CalledProcessError:
            SubprocessHelper._check_output_extended(
                ["git", "checkout", "-b", branch], cwd=self.repo.repo_dir,
                stderr=subprocess.STDOUT, pty=True,
                copy_to_regular_stdout_stderr=self.show_debug)
        assert(self.repo.current_branch == branch)

        # Push branch:
        self._run_authed_git_command(["push",
            self.name, branch])

class LocalRepo(object):
    def __init__(self, repo_dir=None, lfs=True, verbose=False):
        self.verbose = verbose
        if repo_dir == None:
            repo_dir = tempfile.mkdtemp(prefix="git-mirror-temp-repo-")
        self.repo_dir = repo_dir
        self.remotes = dict()
        self.lfs = lfs
        if not os.path.exists(os.path.join(self.repo_dir, ".git")):
            if self.verbose:
                print("verbose: shell cmd: git init .",
                    file=sys.stderr, flush=True)
            subprocess.check_output(["git", "init", "."],
                cwd=self.repo_dir)
            if self.lfs:
                if self.verbose:
                    print("verbose: shell cmd: git lfs install --skip-smudge",
                        file=sys.stderr, flush=True)
                subprocess.check_output(["git", "lfs", "install",
                    "--skip-smudge"],
                    cwd=self.repo_dir)
        if self.verbose:
            git_status_s = subprocess.check_output(["git", "status", "-s"],
                cwd=self.repo_dir)
            try:
                git_status_s = git_status_s.decode("utf-8", "replace")
            except AttributeError:
                pass
            print("verbose: local repo init: LocalRepo(repo_dir=" +
                str(self.repo_dir) + ")  -- " +
                str(len(os.listdir(self.repo_dir))) + " files, " +
                "git status: " + (
                "UNCLEAN" if len(git_status_s.strip()) > 0 else \
                "CLEAN"))

    @property
    def current_branch(self):
        try:
            if self.verbose:
                print("verbose: shell cmd: git branch",
                    file=sys.stderr, flush=True)
            lines = subprocess.check_output(
                ["git", "branch"], cwd=self.repo_dir).decode(
                "utf-8", "replace").split("\n")
        except subprocess.CalledProcessError as e:
            print("Warning: obtaining branch failed with output: \n\n" + str(
                e.output) + "\n\n --> will return None as branch",
                file=sys.stderr, flush=True)
            return None

        # Empty master / no commits case:
        if len(lines) == 1 and len(lines[0]) == 0:
            # Verify this is truly an empty master:
            try:
                if self.verbose:
                    print("verbose: shell cmd: git log",
                        file=sys.stderr, flush=True)
                output = subprocess.check_output(
                    ["git", "log"], cwd=self.repo_dir,
                    stderr=subprocess.STDOUT).decode(
                    "utf-8", "replace")
            except subprocess.CalledProcessError as e:
                output = e.output.decode("utf-8", "replac")
            if output.startswith("fatal: your current branch 'master"):
                return "master"

        # Regular branch output check:
        for line in lines:
            if line.startswith("* "):  # current branch is marked with star!
                line = line[2:].strip()
                return line
        return None

    def head(self, branch="master"):
        try:
            if self.verbose:
                print("verbose: shell cmd: git checkout " + branch,
                    file=sys.stderr, flush=True)
            subprocess.check_output(
                ["git", "checkout", branch], cwd=self.repo_dir)
        except subprocess.CalledProcessError:
            if self.verbose:
                print("verbose: shell cmd: git checkout -b " + branch,
                    file=sys.stderr, flush=True)
            subprocess.check_output(
                ["git", "checkout", "-b", branch], cwd=self.repo_dir)
        assert(self.current_branch == branch)
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=self.repo_dir).strip()

    def add_remote(self, remote):
        if remote.name in self.remotes:
            raise RuntimeError("there is already a remote named '" +
                remote.name + "'")
        remote.set_repo(self)
        self.remotes[remote.name] = remote

class Mirror(object):
    def __init__(self, settings):
        self._last_known_hash = dict()
        self.settings = settings
        if not "source" in self.settings:
            raise RuntimeError("'source' key missing in mirror settings")
        if not "target" in self.settings:
            raise RuntimeError("'source' key missing in mirror settings")
        if verbose:
            print("verbose: " + str(self) + ": initializing local repo...",
                file=sys.stderr, flush=True)

        use_lfs = False
        if "lfs" in self.settings:
            if self.settings["lfs"]:
                use_lfs = True
        self.repo = LocalRepo(lfs=use_lfs, verbose=verbose)

        if verbose:
            print("verbose: " + str(self) + ": adding source remote...",
                file=sys.stderr, flush=True)
        try:
            self.repo.add_remote(Remote(self.settings["source"]["url"],
                self.settings["source"]["auth"], "source",
                verbose=verbose))
        except (RuntimeError, subprocess.CalledProcessError) as e:
            raise RuntimeError("add_remote failed: " + str(e))
        if verbose:
            print("verbose: " + str(self) + ": adding target remote...",
                file=sys.stderr, flush=True)
        self.repo.add_remote(Remote(self.settings["target"]["url"],
            self.settings["target"]["auth"], "target",
            verbose=verbose))

    def __repr__(self):
        if hasattr(self, "settings"):
            return "<Mirror " + str(self.settings["source"]["url"]
                ) + " -> " + str(
                self.settings["target"]["url"]) + ">"
        return super().__repr__()

    def __str__(self):
        return self.__repr__()

    def update(self):
        print ("-------- STARTING UPDATE OF " + str(self), flush=True)
        class UpdateThread(threading.Thread):
            def __init__(self, mirror):
                super().__init__()
                self.mirror = mirror
                self.error = None

            def run(self):
                try:
                    if verbose:
                        print("verbose: " + str(self.mirror) +
                            ": obtain branches...")
                    branches = self.mirror.repo.remotes["source"].branch()
                    if verbose:
                        print("verbose: " + str(self.mirror) +
                            ": detected branches are: " +
                            str(branches))
                    branches.union(set(["master"]))
                    ordered_branches = list(branches)
                    if "master" in ordered_branches:
                        ordered_branches.remove("master")
                        ordered_branches = [ "master" ] + ordered_branches

                    for branch in ordered_branches:
                        # Pull from the source to see if there are new changes:
                        try:
                            self.mirror.repo.remotes["source"].pull(
                                branch=branch,
                                require_clean_status=True)
                        except subprocess.CalledProcessError as e:
                            o = e.output
                            try:
                                o = o.decode("utf-8", "replace")
                            except AttributeError:
                                pass
                            print("error: failed to pull mirror: " +
                                "encountered error: " + str(e) + "\n" +
                                "  - command output was:\n" +
                                "     " + "     ".join(o.split("\n")),
                                file=sys.stderr, flush=True)
                            raise e
                        if (branch in self.mirror._last_known_hash and
                                self.mirror.repo.head(branch=branch) ==
                                    self.mirror._last_known_hash[branch]):
                            if verbose:
                                print("verbose: " + str(self.mirror) +
                                    ": nothing to update for branch " +
                                    str(branch) + ", no new commit. "
                                    "Current HEAD: " +
                                    str(self.mirror._last_known_hash[branch]),
                                    file=sys.stderr, flush=True)
                            # Nothing to forward.
                            continue
                        self.mirror._last_known_hash[branch] = \
                            self.mirror.repo.head(branch=branch)
                        print(str(self.mirror) + ": branch " + str(branch) +
                            ": has NEW HEAD [" + str(branch) + "]: " + str(
                            self.mirror._last_known_hash[branch]))
                        if verbose:
                            print("verbose: " + str(self.mirror) +
                                ": pushing branch " + str(branch) +
                                " to target...",
                                file=sys.stderr, flush=True)

                        # Push changes to the target:
                        print(str(self.mirror) + ": branch " + str(branch) +
                            ": pushing update...")
                        try:
                            self.mirror.repo.remotes["target"].push(
                                branch=branch)
                        except Exception as e:
                            self.mirror._last_known_hash[branch] = None
                            raise e
                        if verbose:
                            print("verbose: " + str(self.mirror) +
                                ": thread work done.",
                                file=sys.stderr, flush=True)
                except Exception as e:
                    self.error = e

        task = UpdateThread(self)
        task.start()
        start_time = time.monotonic()
        warnings_displayed = 0
        while True:
            if not task.isAlive():
                break
            if time.monotonic() > start_time + 600 and warnings_displayed < 1:
                print("Warning: mirror task already running for 10 " +
                    "minutes [mirror: " + str(self) + "]", file=sys.stderr,
                    flush=True)
                warnings_displayed += 1
            elif time.monotonic() > start_time + 1200 and warnings_displayed < 2:
                print("Warning: mirror task already running for 20 " +
                    "minutes [mirror: " + str(self) + "]", file=sys.stderr,
                    flush=True)
                warnings_displayed += 1
            elif time.monotonic() > start_time + 1800 and warnings_displayed < 3:
                print("Error: mirror task already running for 30 " +
                    "minutes [mirror: " + str(self) + "], assuming " +
                    "it is stuck.", file=sys.stderr, flush=True)
                sys.exit(1)
            time.sleep(10)
        if task.error != None:
            raise task.error
        print("Mirror " + str(self) + " was updated.", file=sys.stderr,
            flush=True)

class MirrorHandler(object):
    def __init__(self, settings):
        self.settings = settings
        self.mirrors = list()

    def run(self):
        if verbose:
            print("verbose: " + str(self) + ": initializing mirrors...",
                file=sys.stderr, flush=True)

        # Initialize mirrors:
        for mirror_settings in self.settings:
            if not type(mirror_settings) == dict:
                raise RuntimeError(
                    "mirror settings have to be a list of " +
                    "dictionaries which contain the settings " +
                    "for each " +
                    "respective git mirror, but found a list " +
                    "entry that " +
                    "is of this type instead: " +
                    str(type(mirror_settings)))
            try:
                self.mirrors.append(Mirror(mirror_settings))
            except Exception as e:
                print("FAILED to add mirror: " +
                    mirror_settings["source"]["url"] + " -> " +
                    mirror_settings["target"]["url"] +
                    "  - This mirror will NOT be synced until " +
                    "restart. " +
                    "Error details: " +
                    str(e),
                    file=sys.stderr, flush=True)

        if verbose:
            print("verbose: " + str(self) + ": starting update loop...",
                file=sys.stderr, flush=True)

        # Update mirrors continuously:
        while True:
            for mirror in self.mirrors:
                try:
                    mirror.update()
                except Exception as e:
                    print("There was an error updating mirror " + str(
                        mirror) + ": " + str(e), file=sys.stderr,
                        flush=True)
                    traceback.print_exc()
                    sys.stdout.flush()
                    sys.stderr.flush()   
            print ("-------- UPDATES COMPLETE", flush=True)
            time.sleep(120)

if __name__ == "__main__":
    if verbose:
        print("verbose: launching...", file=sys.stderr, flush=True)
    try:
        settings_str = os.environ["MIRRORS"]
    except KeyError:
        raise RuntimeError("No settings found. Did you specify the MIRRORS "+
            "environment variable in your docker-compose.yml?")
    settings = yaml.safe_load(settings_str)
    if not type(settings) == list:
        raise RuntimeError("MIRRORS settings expected to be a list, " +
            "but type is " + str(type(settings)))
    if verbose:
        print("verbose: settings parsed. length: " + str(len(settings_str)),
            file=sys.stderr, flush=True)
    handler = MirrorHandler(settings)
    handler.run()

