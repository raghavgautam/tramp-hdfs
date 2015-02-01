# Tramp-HDFS
[![MELPA](http://melpa.org/packages/tramp-hdfs-badge.svg)](http://melpa.org/#/tramp-hdfs)

Browse HDFS(Hadoop Distributed File System) in Emacs using Tramp.

## Usage
Put this somewhere in your init file. Eg. ~/.emacs or ~/.emacs.d/init.el

    (require 'tramp-hdfs)

It uses ssh to login to another client machine that has hdfs client to access hdfs.
The syntax to open file in hdfs is just like opening file over tramp. For example:

    /hdfs:root@node-1:/tmp

where, root is the user that you want to use for ssh & node-1 is the name of the
 machine that has hdfs client.

It will use ssh for login and ask for password as needed.

## Supported features:

* Directory browsing
* Opening files
* Deleting files/directories

It doesn't support writing to files to hdfs or acls.

## Requirements
* The client machine should be accessible by ssh.
* The client machine should have bash.

## Installation

* Manually: Download tramp-hdfs.el and add the location to load path.
* Through melpa:
Ensure you have melpa in your package-archives
(see [Melpa Installation](http://melpa.org/#/getting-started)).
Then, M-x package-install [RET] tramp-hdfs. 

## Known issues:
Some modes don't play nice with tramp for eg: projectile mode

## Filing bugs & Feature requests:
Please open bugs at:
https://github.com/raghavgautam/tramp-hdfs

To get tramp-debug logs:

    (setq tramp-verbose 10)
