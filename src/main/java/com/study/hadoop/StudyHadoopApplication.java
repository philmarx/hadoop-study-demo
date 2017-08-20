package com.study.hadoop;

import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StudyHadoopApplication {

	static{
		System.setProperty("hadoop.home.dir", "F:\\programs\\winutils\\hadoop-2.8.0-RC3");
		System.setProperty("HADOOP_USER_NAME", "root"); //root为Master服务器上的用户
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) {
		SpringApplication.run(StudyHadoopApplication.class, args);
	}
}
