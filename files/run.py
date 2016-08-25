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
    def _check_output_extended(cmd, shell=False, stdout=None,
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
                cwd=cwd, env=env)
        else:
            process = subprocess.Popen([sys.executable,
                "-c", "import pty\nimport sys\n" +\
                "exit_code = pty.spawn(sys.argv[1:])\n" +\
                "sys.exit((int(exit_code) & (255 << 8)) >> 8)"] + cmd,
                shell=shell,
                stdout=actual_stdout, stderr=actual_stderr,
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
    def __init__(self, url, auth_info, name="origin"):
        if not type(auth_info) == dict:
            raise RuntimeError("invalid auth info: " +
                "dictionary expected")
        self.url = url
        if self.url.startswith("scp://") or self.url.startswith("ssh://"):
            self.url = self.url.partition("//")[2]
        self.auth = auth_info
        self.name = name

    def set_repo(self, repo):
        self.repo = repo
        remote_list_output = subprocess.check_output(["git", "remote"],
            cwd=self.repo.repo_dir)
        lines = [line.strip().decode("utf-8", "replace")\
            for line in remote_list_output.replace(
            b"\r\n", b"\n").split(b"\n") if len(line.strip()) > 0]
        remotes = [line.partition(" ")[0] for line in lines]
        if self.name in remotes:
            # Nothing to do, remote was already added.
            return
        subprocess.check_output(["git", "remote", "add", self.name,
            self.url], cwd=self.repo.repo_dir)
        subprocess.check_output(["git", "remote", "update"],
            cwd=self.repo.repo_dir)

    def _run_authed_git_command(self, command, binary="git"):
        if verbose:
            print("verbose: shell cmd: " + str([binary] + command),
                file=sys.stderr, flush=True)
        if not "type" in self.auth:
            raise RuntimeError("invalid auth info: no \"type\" specified")
        if self.auth["type"] == "anonymous":
            output = subprocess.check_output([binary] + command,
                cwd=self.repo.repo_dir)
            return output
        elif self.auth["type"] == "password":
            if not "info" in self.auth:
                raise RuntimeError("invalid auth info: password auth " +
                    "requires \"info\" entry with \"password\" specified " +
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
            class PasswordResponder(object):
                def __init__(self, password):
                    self.password = password
                    self.process = None
                    self.output = ""
                    self.was_prompted = False

                def process_obj_callback(self, obj):
                    self.process = obj

                def write_callback(self, c):
                    self.output += c
                    if self.was_prompted:
                        return
                    if self.output.find("AELRKEARLEARLK") >= 0:
                        self.was_prompted = True
                        self.process.stdin.write(self.password + "\n")

            helper_obj = PasswordResponder(
                self.auth["info"]["password"])
            output = SubprocessHelper._check_output_extended(
                ["git"] + command, cwd=self.repo.repo_dir,
                write_function=helper_obj.write_callback,
                process_handle_callback=helper_obj.process_obj_callback)
            return output
        elif self.auth["type"] == "public_key":
            raise RuntimeError("not implemented yet")

    def pull(self, lfs=False, branch="master"):
        self._run_authed_git_command(["pull", self.name, branch])
        self._run_authed_git_command(["pull", self.name],
            binary="git-lfs")

    def push(self, directory, branch="master"):
        self._run_authed_git_command(["pull", self.name, branch])

class LocalRepo(object):
    def __init__(self, repo_dir=None, lfs=True):
        if repo_dir == None:
            repo_dir = tempfile.mkdtemp(prefix="git-mirror-temp-repo-")
        self.repo_dir = repo_dir
        self.remotes = dict()
        self.lfs = lfs
        if not os.path.exists(os.path.join(self.repo_dir, ".git")):
            subprocess.check_output(["git", "init", "."],
                cwd=self.repo_dir)
            if self.lfs:
                subprocess.check_output(["git", "lfs", "install",
                    "--skip-smudge"],
                    cwd=self.repo_dir)

    def head(self):
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
        self._last_known_hash = None
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
        self.repo = LocalRepo(lfs=use_lfs)

        if verbose:
            print("verbose: " + str(self) + ": adding source remote...",
                file=sys.stderr, flush=True)
        self.repo.add_remote(Remote(self.settings["source"]["url"],
            self.settings["source"]["auth"], "source"))
        if verbose:
            print("verbose: " + str(self) + ": adding target remote...",
                file=sys.stderr, flush=True)
        self.repo.add_remote(Remote(self.settings["target"]["url"],
            self.settings["target"]["auth"], "target"))

    def __repr__(self):
        if hasattr(self, "settings"):
            return "<Mirror " + str(self.settings["source"]["url"]
                ) + " -> " + str(
                self.settings["target"]["url"]) + ">"
        return super().__repr__()

    def __str__(self):
        return self.__repr__()

    def update(self):
        class UpdateThread(threading.Thread):
            def __init__(self, mirror):
                super().__init__()
                self.mirror = mirror
                self.error = None

            def run(self):
                try:
                    if verbose:
                        print("verbose: " + str(self.mirror) +
                            ": pulling from source...")
                    self.mirror.repo.remotes["source"].pull()
                    if (self.mirror._last_known_hash == None or
                            self.mirror.repo.head() ==
                                self.mirror._last_known_hash):
                        if verbose:
                            print("verbose: " + str(self.mirror) +
                                ": nothing to update, no new commit.")
                        # Nothing to forward.
                        return
                    self.mirror._last_known_hash = self.mirror.repo.head()
                    if verbose:
                        print("verbose: " + str(self.mirror) +
                            ": pushing to target...")

                    self.mirror.repo.remotes["target"].push()
                    if verbose:
                        print("verbose: " + str(self.mirror) +
                            ": thread work done.")
                except Exception as e:
                    self.error = e

        task = UpdateThread(self)
        task.start()
        start_time = time.monotonic()
        warnings_displayed = 0
        while True:
            if not task.isAlive():
                break
            if time.monotonic() > start_time + 600 and warning_displayed < 1:
                print("Warning: mirror task already running for 10 " +
                    "minutes [mirror: " + str(self) + "]", file=sys.stderr,
                    flush=True)
                warnings_displayed += 1
            elif time.monotonic() > start_time + 1200 and warning_displayed < 2:
                print("Warning: mirror task already running for 20 " +
                    "minutes [mirror: " + str(self) + "]", file=sys.stderr,
                    flush=True)
                warnings_displayed += 1
            elif time.monotonic() > start_time + 1800 and warning_displayed < 3:
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
                raise RuntimeError("mirror settings have to be a list of " +
                    "dictionaries which contain the settings for each " +
                    "respective git mirror, but found a list entry that " +
                    "is of this type instead: " + str(type(mirror_settings)))
            self.mirrors.append(Mirror(mirror_settings))

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
            time.sleep(5)

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

