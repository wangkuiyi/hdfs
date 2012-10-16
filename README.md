# HDFS

This Go package provides type `hdfs.File`, whose interface is similar
to that of `os.File`.  By default, `hdfs.File` represent files local
filesystem.  If you want it to represent files on HDFS, please call
`hdfs.Init` providing HDFS host, port and username.
