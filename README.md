# HDFS

This Go package provides type `hdfs.File`, which implements interface
`Reader` and `Writer` like `os.File`.  By default, `hdfs.File`
represents files on the local filesystem.  If you want it to represent
files on HDFS, please call `hdfs.Init` and provide HDFS host, port and
username.
