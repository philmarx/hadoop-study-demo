package com.study.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HDFSTests {

	@Test
	public void readFile() throws IOException {
		// 查看/user/text文件到
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path("hdfs://master:9000/file"));
		// 第四个参数为是否关闭输入流和输出流
		IOUtils.copyBytes(in, System.out, 4096, true);
	}

	@Test
	public void createFile() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 如果没有puglic目录，就自动创建该目录
		FSDataOutputStream out = fs.create(new Path("hdfs://master:9000/wordcount/input"));
		InputStream in = new ByteArrayInputStream("hello".getBytes("UTF-8"));
		IOUtils.copyBytes(in, out, 4096, true);
	}

	@Test
	public void copyFromLocalFile() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(new Path("D:/hadoop_dir/test.txt"), new Path("hdfs://master:9000/public/test1"));
	}

	@Test
	public void copyToLocalFile() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.copyToLocalFile(new Path("hdfs://master:9000/public/"), new Path("D:/hadoop_dir/public/"));
	}

	@Test
	public void delete() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 第二个参数为是否递归
		fs.delete(new Path("hdfs://master:9000/public2"), true);
	}

	@Test
	public void createDir() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 可以递归创建目录
		boolean success = fs.mkdirs(new Path("hdfs://master:9000/public3/user"));
		System.out.println(success);
	}

	@Test
	public void move() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// 目标目录必须存在
		fs.rename(new Path("hdfs://master:9000/public/user"), new Path("hdfs://master:9000/public/myDir"));
	}

	@Test
	public void listStates() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStates = fs.listStatus(new Path("hdfs://master:9000/public"));
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (FileStatus f : fileStates) {
			String lastModifyTime = df.format(new Date(f.getModificationTime()));
			System.out.println((f.isDirectory() ? "d" : "f") + "," + f.getPath() + "," + lastModifyTime);
		}
	}

	@Test
	public void exists() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		boolean exists = fs.exists(new Path("hdfs://master:9000/public/myDir"));
		System.out.println(exists);
	}
}
