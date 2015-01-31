# tramp-hdfs

Browser HDFS in Emacs using Tramp. The syntax to open file in hdfs is like
opening file over tramp. For example:

    /hdfs:root@node-1:/tmp
    
## Supported features:

* Directory browsing
* Opening files
* Deleting files/directories


## Known issues:

* Some modes don't play nice with tramp for eg: projectile mode