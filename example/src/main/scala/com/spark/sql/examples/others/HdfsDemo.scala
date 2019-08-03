package com.spark.sql.examples.others

import java.net.URI
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


/**
  * Created by yilong on 2019/7/8.
  */
object HdfsDemo {
  def main(args: Array[String]): Unit = {
    val uri = "hdfs://localhost:9000/";
    val newHadoopConf = new Configuration()
    val fs = FileSystem.get(URI.create(uri), newHadoopConf);
    System.out.println(fs.getUri.toString)
    System.out.println(fs.getWorkingDirectory.toUri.getPath)
    System.exit(1)
    val newdir = "/user/__tmp__/"+UUID.randomUUID().toString
    val nowdir = "/user/fileop/test/"
    val nowpath = new Path(nowdir)


    System.out.println(newdir+":"+fs.mkdirs(new Path(newdir)))

    val statuss = fs.listStatus(nowpath)
    statuss.foreach(fstastus => {
      System.out.println(fstastus)
      fs.rename(fstastus.getPath, new Path(newdir))
    })
    //fs.rename(nowpath, new Path(newdir))

    System.out.println(nowpath.toString+" create again : "+fs.mkdirs(nowpath))
  }

  /*
  *
  *     System.out.println("******************** acl status start ")
    try{
      System.out.println(fs.getAclStatus(nowpath))
    } catch {
      case e:Exception => None
    }
    System.out.println(fs.getStatus(nowpath).toString)
    System.out.println("******************** acl status end ")
  * */
}
